using CommandLine;
using ScoreboardLiveApi;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace ScoreboardLiveTPConnector
{
    //Shamelessly copied from the ScoreboardLiveApiExample
    //TP protocols inspired by https://github.com/phihag/bts.git (which however uses TP Network for a dual direction communication)
    //TournamentTV protocol reverse engineered by Daniel Sahlberg using Wireshark

    public class Options
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; } = false;
        [Option('x', "export", Required = false, HelpText = "Export XML messages to path")]
        public string Export { get; set; } = "";
        [Option('d', "database", Required = false, HelpText = "Full path to TP/CP database file. If unspecified, the Tournament TV protocol is used.")]
        public string Database { get; set; } = "";
        [Option('p', "port", Required = false, HelpText = "Listening port for the Tournament TV protocol (default 13333).")]
        public int Port { get; set; } = 13333;
        [Option('i', "ip", Required = false, HelpText = "IP address/host name to listen at (default \"ANY\", ie let Windows decide).")]
        public string IP { get; set; } = "";
        [Option('u', "url", Required = false, HelpText = "URL to ScoreboardLive.")]
        public string URL { get; set; } = "https://www.scoreboardlive.se";
        //Utvecklingsmiljö: https://tokig.ddns.net/sbdev/
    }

    class lMatch
    {
        public Match match = new Match();
        public string court { get; set; } = "";
        public string location { get; set; } = "";
        public bool pinged { get; set; } = false;
        public new string ToString
        {
            get
            {
                return match.ToString();
            }
        }
    }
    class Program
    {
        private static ApiHelper api;
        private static readonly string keyStoreFile = string.Format("{0}scoreboardTPConnectorKeys.bin", AppDomain.CurrentDomain.BaseDirectory);

        static async Task<Unit> SelectUnit()
        {
            List<Unit> units = new List<Unit>();
            // First try and fetch all units to select from at server
            Console.Clear();
            Console.WriteLine("Fetching all available units from server");
            try
            {
                units.AddRange(await api.GetUnits());
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
            // Print them out
            int i = 1;
            units.ForEach(unit => Console.WriteLine("{0}. {1}", i++, unit.Name));
            // Have the user select one
            Console.Write("Select a unit to use: ");
            int.TryParse(Console.ReadLine(), out int selection);
            // Check so that the number is valid
            if (selection < 1 || selection > units.Count)
            {
                return await SelectUnit();
            }
            return units[selection - 1];
        }

        static async Task<Device> RegisterWithUnit(Unit unit, LocalKeyStore keyStore)
        {
            // Get activation code from user
            Console.Clear();
            Console.WriteLine("This device is not registered with {0}.", unit.Name);
            Console.Write("Enter activation code for {0}: ", unit.Name);
            string activationCode = Console.ReadLine().Trim();
            // Register this code with the server
            Device deviceCredentials = null;
            try
            {
                deviceCredentials = await api.RegisterDevice(activationCode);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.ReadKey();
            }
            // If registration was a success, save the credentials to the key store
            if (deviceCredentials != null)
            {
                keyStore.Set(deviceCredentials);
                keyStore.Save(keyStoreFile);
            }
            return deviceCredentials;
        }

        static async Task<bool> CheckCredentials(Unit unit, Device device, LocalKeyStore keyStore)
        {
            Console.WriteLine("Checking so that the credentials on file for {0} are still valid...", unit.Name);
            bool valid = false;
            try
            {
                valid = await api.CheckCredentials(device);
            }
            catch (Exception e)
            {
                // We end up here if it cant be determined if the credentials are valid or not,
                // so don't discard the keys here, just return.
                Console.WriteLine(e.Message);
                return false;
            }
            if (valid)
            {
                Console.WriteLine("Credentials checked out OK.");
            }
            else
            {
                Console.WriteLine("Credentials no longer valid. Removing from key store.");
                keyStore.Remove(device);
                keyStore.Save(keyStoreFile);
            }
            return valid;
        }

        static async Task<Tournament> SelectTournament(Unit unit, Device device)
        {
            Console.Clear();
            Console.WriteLine("Downloading latest tournaments for {0}...", unit.Name);
            List<Tournament> tournaments = new List<Tournament>();
            try
            {
                // Get the 10 newest tournaments for this unit
                tournaments.AddRange(await api.GetTournaments(device, 10));
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
            // Select a list for the user to choose from
            Console.WriteLine();
            Console.WriteLine("#   Starts     Tournament");
            Console.WriteLine("---------------------------------------------------------------------------");
            int i = 1;
            foreach (Tournament tournament in tournaments)
            {
                Console.Write("{0,2}. ", i++);
                Console.Write(tournament.StartDate.ToShortDateString());
                if (tournament.TournamentType == "series")
                {
                    Console.Write(" {0} - {1}", tournament.Team1, tournament.Team2);
                }
                else
                {
                    Console.Write(" {0}", tournament.Name);
                }
                Console.WriteLine(" ({0})", tournament.TournamentType);
            }
            Console.Write("Select a tournament (leave empty to let server decide): ");
            int.TryParse(Console.ReadLine(), out int selection);
            // Return the selected tournament
            if (selection == 0)
            {
                return null;
            }
            else if ((selection > 0) && (selection <= tournaments.Count))
            {
                return tournaments[selection - 1];
            }
            return await SelectTournament(unit, device);
        }

        static async Task<Match> CreateMatch(Device device, Tournament tournament, Match match)
        {
            Console.WriteLine("Uploading a match to server...");
            // Send request
            Match serverMatch = null;
            try
            {
                serverMatch = await api.CreateOnTheFlyMatch(device, tournament, match);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
            Console.WriteLine("The following match was created:");
            Console.WriteLine(serverMatch);
            Console.WriteLine();
            return serverMatch;
        }

        static async Task<List<Court>> GetCourts(Device device)
        {
            // Get courts from server
            Console.Clear();
            Console.WriteLine("Fetching all available courts from server...");
            List<Court> courts = new List<Court>();
            try
            {
                courts.AddRange(await api.GetCourts(device));
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
            return courts;
        }

        static async Task AssignMatchToCourt(Device device, Match match, Court court)
        {
            if (match.MatchID == 0)
            {
                Console.WriteLine("Illegal match ID {0}, cannot assign to court!", match.MatchID);
                return;
            }
            Console.WriteLine();
            Console.WriteLine("Assigning match {0} to court {1}", match.MatchID, court.Name);
            try
            {
                await api.AssignMatchToCourt(device, match, court);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        static async Task<List<Match>> GetMatches(Device device, Tournament tournament, int tournamentMatchNumber)
        {
            try
            {
                return await api.FindMatchBySequenceNumber(device, tournament, tournamentMatchNumber);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        static bool IsCategoriesTranslated(ref bool TranslateCategories)
        {
            Console.Write("Is the categories names in Swedish (1) or English (2)? ");
            int.TryParse(Console.ReadLine(), out int answer);
            switch (answer)
            {
                case 1:
                    TranslateCategories = true;
                    return true;
                     
                case 2:
                    TranslateCategories = false;
                    return true;
            }
            return false;
        }

        static string TranslateCategories(string Category, bool Translate)
        {
            if (!Translate)
                return Category;

            switch (Category)
            {
                case "dd":
                case "damdubbel":
                    return "wd";
                case "ds":
                case "damsingel":
                    return "ws";
                case "hd":
                case "herrdubbel":
                    return "md";
                case "hs":
                case "herrsingel":
                    return "ms";
                case "md":
                case "mixed":
                case "mixeddubbel":
                    return "xd";
                default:
                    return "";
            }
        }

        static void Main(string[] args)
        {
            try
            {
                Parser.Default.ParseArguments<Options>(args)
                       .WithParsed<Options>(o => Run(o));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unhandled exception: " + ex.ToString());
                Console.WriteLine(ex.StackTrace);
                return;
            }
            Console.WriteLine("Done, press any key to close!");
            Console.ReadKey();
        }

        static void Run(Options o)
        {
            api = new ApiHelper(o.URL);
            
            // Select a unit
            Unit selectedUnit = SelectUnit().Result;
            if (selectedUnit == null) return;
            Console.WriteLine("Unit {0} was selected.", selectedUnit.Name);

            // Load the keystore, and select the appropriate key to use for this unit.
            // If this device is not registered with that unit, do registration
            LocalKeyStore keyStore = LocalKeyStore.Load(keyStoreFile);
            while (keyStore.Get(selectedUnit.UnitID) == null)
            {
                RegisterWithUnit(selectedUnit, keyStore).Wait();
            }

            // Check the credentials to make sure they are still valid
            Device deviceCredentials = keyStore.Get(selectedUnit.UnitID);
            if (!CheckCredentials(selectedUnit, deviceCredentials, keyStore).Result)
            {
                Console.ReadKey();
                return;
            }

            // Select a tournament to add matches to
            Tournament selectedTournament = SelectTournament(selectedUnit, deviceCredentials).Result;
            Console.WriteLine("Selected tournament: {0}", selectedTournament);

            //Get courts
            List<Court> courts = GetCourts(deviceCredentials).Result;

            if (o.Database != "")
            {
                RunDatabase(o, deviceCredentials, selectedTournament, courts);
            }
            else
            {
                RunTTV(o, deviceCredentials, selectedTournament, courts);
            }
        }

        static void RunDatabase(Options o, Device deviceCredentials, Tournament selectedTournament, List<Court> courts)
        {
            bool Quit = false;

            if (o.Verbose)
            {
                Console.WriteLine($"Verbose output enabled.");
            }
            Console.WriteLine($"Database is {o.Database}");

            OleDbConnection oConn;
            string s = "";
            try
            {
                s = @"Provider=Microsoft.ACE.OLEDB.12.0;User ID=Admin;Data Source=" + o.Database + @";Mode=Share Deny None;Extended Properties="""";Jet OLEDB:Database Password = d4R2GY76w2qzZ;";
                /*                Provider = Microsoft.ACE.OLEDB.12.0; Password = """"; User ID = Admin; Data Source = " & oFolderDialog.SelectedItems(i%) & "\" & sFile & "; Mode = Share Deny Write; Extended Properties = """"; Jet OLEDB:System database = """"; Jet OLEDB:Registry Path = """"; Jet OLEDB:Database Password = d4R2GY76w2qzZ; Jet OLEDB:Engine Type = 5; Jet OLEDB:Database Locking Mode = 0; Jet OLEDB:Global Partial Bulk Ops = 2; Jet OLEDB:Global Bulk Transactions = 1; Jet OLEDB:New Database Password = """"; Jet OLEDB:Create System Database = False; Jet OLEDB:Encrypt Database = False; Jet OLEDB:Don't Copy Locale on Compact=False;Jet OLEDB:Compact Without Replica Repair=False;Jet OLEDB:SFP=False;Jet OLEDB:Support Complex Data=False;Jet OLEDB:Bypass UserInfo Validation=False*/
                oConn = new OleDbConnection(s);
                oConn.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error connecting to database: " + e.Message);
                Console.WriteLine(s.Substring(170));
                return;
            }

            Dictionary<string, lMatch> matches = new Dictionary<string, lMatch>();

            while (!Quit)
            {
                /*                OleDbCommand oCmd = new OleDbCommand(@"SELECT Court.name, Location.name, PlayerMatch.sp1, PlayerMatch.sp2, PlayerMatch.sp3, PlayerMatch.sp4 " +
                                                                     @"FROM (((Court " +
                                                                     @"INNER JOIN PlayerMatch ON PlayerMatch.id=Court.PlayerMatch) " +
                                                                     @"INNER JOIN Location ON Location.id=Court.Location) " +
                                                                     @"LEFT JOIN Player AS p ON Player.id=PlayerMatch.sp1) " +
                                                                     @"WHERE Court.PlayerMatch<>0", oConn);
                                                                     */
                /*                OleDbCommand oCmd = new OleDbCommand(@"SELECT Court.name, Location.name, PlayerMatch.sp1, PlayerMatch.sp2, PlayerMatch.sp3, PlayerMatch.sp4 " +
                                                                     @"FROM (((Court " +
                                                                     @"INNER JOIN PlayerMatch ON PlayerMatch.id=Court.PlayerMatch) " +
                                                                     @"INNER JOIN Location ON Location.id=Court.Location) " +
                                                                     @"LEFT JOIN Player AS p ON Player.id=PlayerMatch.sp1) " +
                                                                     @"WHERE Court.PlayerMatch<>0", oConn);*/

                const int COL_ID = 0;
                const int COL_COURT_NAME = 1;
                const int COL_LOCATION_NAME = 2;
                const int COL_PLAYER_1_NAME = 3;
                const int COL_CLUB_1_NAME = 4;
                const int COL_PLAYER_2_NAME = 5;
                const int COL_CLUB_2_NAME = 6;
                const int COL_PLAYER_3_NAME = 7;
                const int COL_CLUB_3_NAME = 8;
                const int COL_PLAYER_4_NAME = 9;
                const int COL_CLUB_4_NAME = 10;
                const int COL_EVENT = 11;
                const int COL_COUNT = 12;

                foreach (lMatch match in matches.Values)
                {
                    match.pinged = false;
                }

                OleDbCommand oCmd = new OleDbCommand(/* When changing columns - remember to change the COL_ integer constants above!! */
                                                     @"SELECT PlayerMatch.id AS id, " +
                                                            @"Court.name AS Court_name, " +
                                                            @"Location.name AS Location_name, " +
                                                            @"IIF(Player_1.asianname, Player_1.name + ' ' + Player_1.firstname, Player_1.name + ', ' + Player_1.firstname) AS Player_1_name, " +
                                                            @"Club_1.name AS Club_1_name, " +
                                                            @"IIF(Player_2.asianname, Player_2.name + ' ' + Player_2.firstname, Player_2.name + ', ' + Player_2.firstname) AS Player_2_name, " +
                                                            @"Club_2.name AS Club_2_name, " +
                                                            @"IIF(Player_3.asianname, Player_3.name + ' ' + Player_3.firstname, Player_3.name + ', ' + Player_3.firstname) AS Player_3_name, " +
                                                            @"Club_3.name AS Club_3_name, " +
                                                            @"IIF(Player_4.asianname, Player_4.name + ' ' + Player_4.firstname, Player_4.name + ', ' + Player_4.firstname) AS Player_4_name, " +
                                                            @"Club_4.name AS Club_4_name " +
                                                            @"IIF(Event.gender = 1, 'm', IIF(Event.gender = 2, 'w', IIF(Event.gender = 3, 'x', IIF(Event.gender = 4, 'm', IIF(Event.gender = 5, 'w', IIF(Event.gender = 6, 'x', _)))))) + IIF(Event.eventtype = '1', 's', 'd') AS Event " +
                                                     @"FROM (((((((((Location " +
                                                         @"INNER JOIN (PlayerMatch " +
                                                             @"INNER JOIN (Event ON PlayerMatch.event = Event.id " +
                                                                @"INNER JOIN Court ON PlayerMatch.id = Court.playermatch)) " +
                                                         @"ON Location.id = Court.location) " +
                                                     @"LEFT JOIN Player AS Player_1 ON Player_1.id = PlayerMatch.sp1) " +
                                                     @"LEFT JOIN Club AS Club_1 ON Club_1.id = Player_1.club) " +
                                                     @"LEFT JOIN Player AS Player_2 ON Player_2.id = PlayerMatch.sp2) " +
                                                     @"LEFT JOIN Club AS Club_2 ON Club_2.id = Player_2.club) " +
                                                     @"LEFT JOIN Player AS Player_3 ON Player_3.id = PlayerMatch.sp3) " +
                                                     @"LEFT JOIN Club AS Club_3 ON Club_3.id = Player_3.club) " +
                                                     @"LEFT JOIN Player AS Player_4 ON Player_4.id = PlayerMatch.sp4) " +
                                                     @"LEFT JOIN Club AS Club_4 ON Club_4.id = Player_4.club) " +
                                                     @"WHERE Court.PlayerMatch <> 0;", oConn);

                OleDbDataReader oRead = oCmd.ExecuteReader();
                Dictionary<string, lMatch> newMatches = new Dictionary<string, lMatch>();
                while (oRead.HasRows && oRead.Read())
                {
                    object[] values = new object[COL_COUNT];
                    int c = oRead.GetValues(values);
                    if (c < COL_COUNT)
                    {
                        throw new Exception($"Not enough columns returned from query. Got {c}, expected {COL_COUNT}");
                    }
                    int id;
                    if (int.TryParse(values[COL_ID].ToString(), out id))
                    {
                        // If the court exists and the court is the same ID
                        if (matches.ContainsKey(values[COL_COURT_NAME].ToString()) && matches[values[COL_COURT_NAME].ToString()].match.TournamentMatchNumber == id)
                        {
                            // Same, don't touch
                            matches[values[COL_COURT_NAME].ToString()].pinged = true;
                        }
                        else
                        {
                            // New match
                            lMatch match = new lMatch();
                            match.match.Category = values[COL_EVENT].ToString();
                            match.match.Team1Player1Team = values[COL_CLUB_1_NAME].ToString();
                            match.match.Team1Player2Team = values[COL_CLUB_2_NAME].ToString();
                            match.match.Team2Player1Team = values[COL_CLUB_3_NAME].ToString();
                            match.match.Team2Player2Team = values[COL_CLUB_4_NAME].ToString();
                            match.match.Team1Player1Name = values[COL_PLAYER_1_NAME].ToString();
                            match.match.Team1Player2Name = values[COL_PLAYER_2_NAME].ToString();
                            match.match.Team2Player1Name = values[COL_PLAYER_3_NAME].ToString();
                            match.match.Team2Player2Name = values[COL_PLAYER_4_NAME].ToString();
                            match.location = values[COL_LOCATION_NAME].ToString();
                            match.court = values[COL_COURT_NAME].ToString();
                            match.match.TournamentMatchNumber = id;
                            match.pinged = true;
                            matches[match.court] = match;
                            Console.WriteLine("New: " + match.ToString);

                            Match serverMatch = CreateMatch(deviceCredentials, selectedTournament, match.match).Result;
                            if (serverMatch == null)
                            {
                                Console.ReadKey();
                                return;
                            }
                            else
                            {
                                match.match = serverMatch;
                            }

                            Console.WriteLine("Servermatch: " + serverMatch.ToString());
                            bool foundCourt = false;
                            foreach (Court court in courts)
                            {
                                if (court.Name == match.court)
                                {
                                    Console.WriteLine("Match: " + match.match.ToString());
                                    // Assign the new match to the selected court
                                    AssignMatchToCourt(deviceCredentials, match.match, court).Wait();
                                    foundCourt = true;
                                }
                            }
                            if (!foundCourt)
                            {
                                Console.WriteLine("Could not find court {match.court} in ScoreboardLive!");
                            }
                            Console.WriteLine("Done.");
                        }
                    }
                }

                HashSet<string> ToDelete = new HashSet<string>();
                foreach (string key in matches.Keys)
                {
                    if (!matches[key].pinged)
                    {
                        ToDelete.Add(key);
                    }
                }
                foreach (string key in ToDelete)
                {
                    Console.WriteLine($"Match {matches[key].match.TournamentMatchNumber} removed from court {matches[key].court}!");
                    matches.Remove(matches[key].court);
                }
                oRead.Close();
                oCmd.Dispose();

                System.Threading.Thread.Sleep(1000);
                if (Console.KeyAvailable)
                {
                    ConsoleKeyInfo k = Console.ReadKey(true);
                    Console.WriteLine("Detected key: " + k.KeyChar);
                    switch (k.KeyChar.ToString().ToUpper())
                    {
                        case "Q":
                            Quit = true;
                            break;
                    }
                }
            }
            oConn.Close();
        }

        static void RunTTV(Options o, Device deviceCredentials, Tournament selectedTournament, List<Court> courts)
        {
            bool Quit = false;
            bool DoTranslateCategories = false;
            Dictionary<string, lMatch> matches = new Dictionary<string, lMatch>();

            while (true)
            {
                if (IsCategoriesTranslated(ref DoTranslateCategories))
                {
                    break;
                }
            }

            if (o.Verbose)
            {
                Console.WriteLine($"Verbose output enabled.");
            }
            Console.WriteLine($"Port is {o.Port}");
            TcpListener server = null;
            try
            {
                IPAddress address;
                if (o.IP != "")
                {
                    try
                    {
                        address = IPAddress.Parse(o.IP);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Can't parse IP {0}", o.IP);
                        throw e;
                    }
                }
                else
                {
                    address = IPAddress.Any;
                }

                // TcpListener server = new TcpListener(port);
                server = new TcpListener(address, o.Port);

                // Start listening for client requests.
                server.Start();

                // Buffer for reading data
                Byte[] bytes = new Byte[1024];
                List<Byte> data = new List<Byte>(1024);

                // Enter the listening loop.
                while (!Quit)
                {
                    try
                    {
                        Console.Write("Waiting for a connection... ");
                        if (o.Verbose)
                        {
                            Console.Clear();
                        }

                        // Perform a blocking call to accept requests.
                        // You could also user server.AcceptSocket() here.
                        TcpClient client = server.AcceptTcpClient();
                        Console.WriteLine("Connected!");

                        // Get a stream object for reading and writing
                        NetworkStream stream = client.GetStream();

                        int i;
                        bool first = true;

                        // Loop to receive all the data sent by the client.
                        while ((i = stream.Read(bytes, 0, bytes.Length)) != 0)
                        {
                            if (first)
                            {
                                data.AddRange(bytes.Skip(4).Take(i - 4));
                                first = false;
                            }
                            else
                            {
                                data.AddRange(bytes.Take(i));
                            }
                        }

                        Console.WriteLine("Received update from TP: {0} bytes", data.Count());

                        string output;

                        // “compressed” now contains the compressed string.
                        // Also, all the streams are closed and the above is a self-contained operation.

                        using (var inStream = new MemoryStream(data.ToArray()))
                        using (var bigStream = new GZipStream(inStream, CompressionMode.Decompress))
                        using (var bigStreamOut = new MemoryStream())
                        {
                            bigStream.CopyTo(bigStreamOut);
                            //Kommer tre byte slaskdata före XML. Hoppa över dessa
                            output = Encoding.UTF8.GetString(bigStreamOut.ToArray().Skip(3).ToArray());
                        }

                        if (o.Export != "")
                        {
                            string fileName = o.Export + "\\" + DateTime.Now.ToString("yyyyMMddHHmmssfff") + ".xml";
                            try
                            {
                                File.WriteAllText(fileName, output);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("Kan inte exportera XML-meddelande till " + fileName + " Exception: " + e.Message);
                            }
                        }

                        //& i texten Court & Time TBA skickas inte korrekt
                        output = output.Replace("& ", "&amp; ");
                        Console.WriteLine("Uncompressed to {0} bytes", output.Count());

                        // <OFFICIALS> är positionerad konstigt i XMLen i några versioner av 201904. Vi tar bort den.
                        //                        while ((i = output.IndexOf("<OFFICIALS>")) >= 0)
                        //                        {
                        //                            int j = output.IndexOf("</OFFICIALS>", i);
                        //                            output = output.Substring(0, i - 1) + output.Substring(j + 12);
                        //                        }
                        //Fast det är senare fixat i versionen 2019-11-26

                        if (o.Verbose)
                        {
                            Console.WriteLine(output);
                        }

                        foreach (lMatch match in matches.Values)
                        {
                            match.pinged = false;
                        }

                        XmlDocument doc = new XmlDocument();
                        doc.PreserveWhitespace = true;
                        doc.LoadXml(output);
                        XmlNodeList onCourtMatches = doc.SelectNodes("/" + doc.DocumentElement.LocalName + "/MATCHES/ONCOURT/MATCH");
                        foreach (XmlNode courtMatch in onCourtMatches)
                        {
                            try
                            {
                                string eventKey = "/" + doc.DocumentElement.LocalName + "/EVENTS/EVENT[@ID=" + courtMatch.Attributes.GetNamedItem("EID").InnerText + "]";

                                int TournamentMatchNumber = int.Parse(courtMatch.Attributes.GetNamedItem("ID").InnerText);
                                if (o.Verbose)
                                {
                                    Console.WriteLine("ID: " + TournamentMatchNumber + " EID: " + courtMatch.Attributes.GetNamedItem("EID").InnerText);
                                }

                                // Get court
                                string CourtName = courtMatch.Attributes.GetNamedItem("CT").InnerText;

                                // If the court exists and the court is the same ID
                                if (matches.ContainsKey(CourtName) && matches[CourtName].match.TournamentMatchNumber == TournamentMatchNumber)
                                {
                                    // Same, don't touch
                                    matches[CourtName].pinged = true;
                                }
                                else
                                {
                                    // New match
                                    lMatch match = new lMatch();
                                    match.court = CourtName;
                                    match.pinged = true;

                                    List<Match> existingMatches = GetMatches(deviceCredentials, selectedTournament, TournamentMatchNumber).Result;
                                    if (existingMatches.Count > 0)
                                    {
                                        match.match = existingMatches[0];
                                    }
                                    else
                                    {
                                        Match sbMatch = new Match();

                                        // Get event
                                        XmlNode eventNode = doc.SelectSingleNode(eventKey);
                                        string category = eventNode.Attributes.GetNamedItem("NAME").InnerText.Trim().ToLower();
                                        int space = category.IndexOf(" ");
                                        // Handle if categories have space in them, for examle "HS U11": We only care about the first part.
                                        // We also need to consider the length of the category: it could also be "Herrsingel U11"
                                        if (space > 0)
                                        {
                                            sbMatch.Category = TranslateCategories(category.Substring(0, space), DoTranslateCategories);
                                        }
                                        else
                                        {
                                            sbMatch.Category = TranslateCategories(category, DoTranslateCategories);
                                        }

                                        // Get match
                                        if (o.Verbose)
                                        {
                                            Console.WriteLine(eventKey + "/DRAWS/DRAW/MATCHES/MATCH[@ID=" + TournamentMatchNumber + "]");
                                        }
                                        XmlNode drawMatch = doc.SelectSingleNode(eventKey + "/DRAWS/DRAW/MATCHES/MATCH[@ID=" + TournamentMatchNumber + "]");

                                        // Used for indexof etc.
                                        int pos;

                                        // Get the scheduled time
                                        // #543: R1 lör 2020-02-29 09:00 Hallen
                                        string playTime;
                                        try { playTime = drawMatch.Attributes.GetNamedItem("PLAYTIME").InnerText; } catch (Exception e) { playTime = ""; }
                                        // Sometimes match number is prefixed in the time. Then there should be multiple : (one after match number and one in the time). Remove everything up to the first :
                                        pos = playTime.IndexOf(":");
                                        if (pos >= 0 && playTime.Substring(pos + 1).IndexOf(":") >= 0)
                                        {
                                            playTime = playTime.Substring(pos + 1).Trim();
                                        }

                                        // Get planning, ie placement within the draw
                                        string planning = drawMatch.Attributes.GetNamedItem("PLANNING").InnerText;
                                        if (o.Verbose)
                                        {
                                            Console.WriteLine("Match planning is " + planning);
                                        }
                                        int level = int.Parse(planning.Substring(0, 1));
                                        pos = int.Parse(planning.Substring(1, 3));
                                        string planningP1 = "";
                                        string planningP2 = "";

                                        // Get draw
                                        XmlNode draw = drawMatch.ParentNode.ParentNode;
                                        int drawType = int.Parse(draw.Attributes.GetNamedItem("DRAWTYPE").InnerText);
                                        bool doubles = draw.Attributes.GetNamedItem("DOUBLES").InnerText == "-1";

                                        // Depending on drawtype, the entries are placed in different plannings
                                        switch (drawType)
                                        {
                                            case 2:
                                                //Pool
                                                //A match is planning 2003. Entries are stored in planning 2000 and 3000.
                                                planningP1 = string.Format("{0:D4}", level * 1000);
                                                planningP2 = string.Format("{0:D4}", pos * 1000);
                                                //Remove round number
                                                try { playTime = playTime.Substring(playTime.IndexOf(" ") + 1); } catch { playTime = ""; }
                                                break;

                                            case 1:
                                            //Elimination
                                            //Same as 6.
                                            case 6:
                                                //Elimination qualifications
                                                //A match is planning 4003. Entries are stored in planning with higer level and pos*2-1 respetive pos*2, ie 5005 and 5006.
                                                planningP1 = string.Format("{0}{1:D3}", level + 1, pos * 2 - 1);
                                                planningP2 = string.Format("{0}{1:D3}", level + 1, pos * 2);
                                                break;

                                            default:
                                                Console.WriteLine("Illegal draw type {0} in {1}", drawType, eventNode.Attributes.GetNamedItem("NAME").InnerText);
                                                break;
                                        }

                                        // Parse playtime
                                        if (playTime != "")
                                        {
                                            //Remove hall
                                            pos = playTime.IndexOf(" "); // After day
                                            if (pos >= 0)
                                            {
                                             //   playTime = playTime.Substring(pos+1);
                                                pos = playTime.IndexOf(" ", pos + 1); // After date
                                                if (pos >= 0)
                                                {
                                                    pos = playTime.IndexOf(" ", pos + 1); // After time
                                                    if (pos >= 0)
                                                    {
                                                        playTime = playTime.Substring(0, pos);
                                                    }
                                                }
                                            }

                                            try
                                            {
                                                sbMatch.StartTime = DateTime.ParseExact(playTime, "ddd yyyy'-'MM'-'dd hh:mm", CultureInfo.CurrentCulture);
                                            }
                                            catch (Exception e)
                                            {
                                                Console.WriteLine("Failed to parse the scheduled time {0}, error message {1}", playTime, e.Message);
                                                sbMatch.StartTime = DateTime.Now;
                                            }
                                        }
                                        else
                                        {
                                            sbMatch.StartTime = DateTime.Now;
                                        }

                                        // Get entries and player names
                                        if (o.Verbose)
                                        {
                                            Console.WriteLine("Player 1 at " + eventKey + "/DRAWS/DRAW[@ID=" + draw.Attributes.GetNamedItem("ID").InnerText + "]/MATCHES/MATCH[@PLANNING=" + planningP1 + "]");
                                        }
                                        XmlNode drawMatchP1 = doc.SelectSingleNode(eventKey + "/DRAWS/DRAW[@ID=" + draw.Attributes.GetNamedItem("ID").InnerText + "]/MATCHES/MATCH[@PLANNING=" + planningP1 + "]");
                                        if (o.Verbose)
                                        {
                                            Console.WriteLine(eventKey + "/ENTRIES/ENTRY[@ID=" + drawMatchP1.Attributes.GetNamedItem("ENTRY").InnerText + "]");
                                        }
                                        XmlNode entryP1 = doc.SelectSingleNode(eventKey + "/ENTRIES/ENTRY[@ID=" + drawMatchP1.Attributes.GetNamedItem("ENTRY").InnerText + "]");
                                        sbMatch.Team1Player1Name = entryP1.Attributes.GetNamedItem("NAME1").InnerText;
                                        sbMatch.Team1Player1Team = entryP1.Attributes.GetNamedItem("CLUB1").InnerText;
                                        //T
                                        if (doubles)
                                        {
                                            sbMatch.Team1Player2Name = entryP1.Attributes.GetNamedItem("NAME2").InnerText;
                                            // Try to get Player2Team from XML. If not, default to same as player 1
                                            try { sbMatch.Team1Player2Team = entryP1.Attributes.GetNamedItem("CLUB2").InnerText; } catch { sbMatch.Team1Player2Team = sbMatch.Team1Player1Team; }
                                        }

                                        if (o.Verbose)
                                        {
                                            Console.WriteLine("Player 2 at " + eventKey + "/DRAWS/DRAW[@ID=" + draw.Attributes.GetNamedItem("ID").InnerText + "]/MATCHES/MATCH[@PLANNING=" + planningP2 + "]");
                                        }
                                        XmlNode drawMatchP2 = doc.SelectSingleNode(eventKey + "/DRAWS/DRAW[@ID=" + draw.Attributes.GetNamedItem("ID").InnerText + "]/MATCHES/MATCH[@PLANNING=" + planningP2 + "]");
                                        if (o.Verbose)
                                        {
                                            Console.WriteLine(eventKey + "/ENTRIES/ENTRY[@ID=" + drawMatchP2.Attributes.GetNamedItem("ENTRY").InnerText + "]");
                                        }
                                        XmlNode entryP2 = doc.SelectSingleNode(eventKey + "/ENTRIES/ENTRY[@ID=" + drawMatchP2.Attributes.GetNamedItem("ENTRY").InnerText + "]");
                                        sbMatch.Team2Player1Name = entryP2.Attributes.GetNamedItem("NAME1").InnerText;
                                        sbMatch.Team2Player1Team = entryP2.Attributes.GetNamedItem("CLUB1").InnerText;
                                        //T
                                        if (doubles)
                                        {
                                            sbMatch.Team2Player2Name = entryP2.Attributes.GetNamedItem("NAME2").InnerText;
                                            // Try to get Player2Team from XML. If not, default to same as player 1
                                            try { sbMatch.Team2Player2Team = entryP2.Attributes.GetNamedItem("CLUB2").InnerText; } catch { sbMatch.Team2Player2Team = sbMatch.Team2Player1Team; }
                                        }
                                        //                                match.location = values[COL_LOCATION_NAME].ToString();
                                        sbMatch.TournamentMatchNumber = TournamentMatchNumber;

                                        //Umpire & SJ
                                        XmlNode umpireNode = courtMatch.SelectSingleNode("OFFICIALS/OFFICIAL[@ID=1]");
                                        try { sbMatch.Umpire = umpireNode.Attributes.GetNamedItem("F").InnerText + " " + umpireNode.Attributes.GetNamedItem("N").InnerText; } catch { sbMatch.Umpire = ""; }
                                        XmlNode servicejudgeNode = courtMatch.SelectSingleNode("OFFICIALS/OFFICIAL[@ID=2]");
                                        try { sbMatch.Servicejudge = servicejudgeNode.Attributes.GetNamedItem("F").InnerText + " " + servicejudgeNode.Attributes.GetNamedItem("N").InnerText; } catch { sbMatch.Servicejudge = ""; }

                                        Match serverMatch = CreateMatch(deviceCredentials, selectedTournament, sbMatch).Result;
                                        if (serverMatch == null)
                                        {
                                            match.match = sbMatch;
                                        }
                                        else
                                        {
                                            match.match = serverMatch;
                                        }

                                        Console.WriteLine("Servermatch: " + serverMatch.ToString());
                                    }
                                    matches[match.court] = match;

                                    Console.WriteLine("Match: " + match.match.ToString());
                                    bool foundCourt = false;
                                    foreach (Court court in courts)
                                    {
                                        if (court.Name == match.court)
                                        {
                                            // Assign the new match to the selected court
                                            AssignMatchToCourt(deviceCredentials, match.match, court).Wait();
                                            foundCourt = true;
                                            break;
                                        }
                                    }
                                    if (!foundCourt)
                                    {
                                        Console.WriteLine("Could not find court {0} in ScoreboardLive!", match.court);
                                    }

                                    //string Status??
                                }

                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e.Message);
                            }
                        }
                        if (o.Verbose)
                        {
                            Console.WriteLine(matches.Count());
                        }
                        HashSet<string> ToDelete = new HashSet<string>();
                        foreach (string key in matches.Keys)
                        {
                            if (!matches[key].pinged)
                            {
                                ToDelete.Add(key);
                            }
                        }
                        foreach (string key in ToDelete)
                        {
                            Console.WriteLine($"Match {matches[key].match.TournamentMatchNumber} removed from court {matches[key].court}!");
                            matches.Remove(matches[key].court);
                        }
                        //
                        // Shutdown and end connection
                        client.Close();
                        data.Clear();

                        if (Console.KeyAvailable)
                        {
                            ConsoleKeyInfo k = Console.ReadKey(true);
                            Console.WriteLine("Detected key: " + k.KeyChar);
                            switch (k.KeyChar.ToString().ToUpper())
                            {
                                case "Q":
                                    Quit = true;
                                    break;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
            }
            catch (SocketException e)
            {
                Console.WriteLine("SocketException: {0}", e);
            }
            finally
            {
                // Stop listening for new clients.
                server.Stop();
            }
        }
    }
}

