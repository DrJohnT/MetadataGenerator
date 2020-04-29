using System;
using System.Linq;
using System.Data.SqlClient;

namespace Generator
{
    static class Program
    {
        public enum Action {
            ImportMetadataFromSourceDatabases,
            UpdateMetadataWithSpecificLookupsAgainstSourceDatabase,
            ImportMetadataFromSourceDatabasesANDUpdateMetadataWithSpecificLookupsAgainstSourceDatabase,
            UpdateAndRegen,
            UpdateMetadataFromDeployedDatabases,
            CreateTablesViewsAndStoredProcsFromMetadata,
            All,
            InvalidSelection,
            Ignore
        }

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            //try
            //{
                Action action;
                if (args.Count() == 1)
                {
                    // we have a command line argument, so do not display the menu
                    switch (args[0].ToString())
                    {
                        case "ImportMetadataFromSourceDatabases":
                            action = Action.ImportMetadataFromSourceDatabases;
                            break;

                        case "UpdateMetadataWithSpecificLookupsAgainstSourceDatabase":
                            action = Action.UpdateMetadataWithSpecificLookupsAgainstSourceDatabase;
                            break;

                        case "ImportMetadataFromSourceDatabasesANDUpdateMetadataWithSpecificLookupsAgainstSourceDatabase":
                            action = Action.ImportMetadataFromSourceDatabasesANDUpdateMetadataWithSpecificLookupsAgainstSourceDatabase;
                            break;

                        case "UpdateMetadataFromDeployedDatabases":
                            action = Action.UpdateMetadataFromDeployedDatabases;
                            break;

                        case "CreateTablesViewsAndStoredProcsFromMetadata":
                            action = Action.CreateTablesViewsAndStoredProcsFromMetadata;
                            break;

                        case "UpdateAndRegen":
                            action = Action.UpdateAndRegen;
                            break;

                        case "All":
                            action = Action.All;
                            break;


                        default:
                            action = Action.InvalidSelection;
                            break;
                    }
                }
                else
                {
                    Console.WriteLine("All commands are now available from PowerShell");
                    Console.WriteLine("\nIn PowerShell please run ");
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine(@".\psake.ps1 Get-Help");
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.Write("\nYou will find all the commands under the ");
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.Write("Generator");
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.Write(" heading");
                    Console.WriteLine("\n\nPress any key to exit");
                    Console.ReadKey();
                    action = Action.Ignore;
                }

                switch (action)
                {
                    case Action.ImportMetadataFromSourceDatabases:
                        Console.WriteLine("Importing Source Database Metadata");
                        ImportDatabaseMetadata.ImportMetadataFromSourceDatabases();
                        Console.WriteLine("Source Database Metadata Imported!");
                        break;

                    case Action.UpdateMetadataWithSpecificLookupsAgainstSourceDatabase:
                        Console.WriteLine("Updating Metadata with specific column lookups");
                        ImportDatabaseMetadata.UpdateMetadataWithSpecificColumnLookups();
                        Console.WriteLine("Source Database Metadata Imported!");
                        break;

                    case Action.ImportMetadataFromSourceDatabasesANDUpdateMetadataWithSpecificLookupsAgainstSourceDatabase:
                        Console.WriteLine("Importing Source Database Metadata");
                        ImportDatabaseMetadata.ImportMetadataFromSourceDatabases();
                        Console.WriteLine("Source Database Metadata Imported!");
                        Console.WriteLine("Updating Metadata with specific column lookups");
                        ImportDatabaseMetadata.UpdateMetadataWithSpecificColumnLookups();
                        Console.WriteLine("Source Database Metadata Imported!");
                        break;

                    case Action.UpdateAndRegen:
                        Console.WriteLine("Importing Target Database Metadata");
                        ImportDatabaseMetadata.ImportMetadataFromTargetDatabases();
                        Console.WriteLine("Target Database Metadata Imported!");
                        Console.WriteLine("Creating Tables, Views and Stored Procs from metadata");
                        using (SqlConnection conn = new SqlConnection(Generator.Properties.Settings.Default.MetadataDatabase))
                        {
                            conn.Open();
                            EssentialsAccountsDB.CreateObjectsFromMetadata(conn);
                            Console.WriteLine("\nIgnoring Staging tables!");
                         //   EssentialsAccountsSTG.CreateObjectsFromMetadata(conn, true);
                        }
                        Console.WriteLine("\nTables, Views and Stored Procs Recreated!");
                        break;

                    case Action.All:
                        Console.WriteLine("Importing Source Database Metadata");
                        ImportDatabaseMetadata.ImportMetadataFromSourceDatabases();
                        Console.WriteLine("Source Database Metadata Imported!");
                        Console.WriteLine("Updating Metadata with specific column lookups");
                        ImportDatabaseMetadata.UpdateMetadataWithSpecificColumnLookups();
                        Console.WriteLine("Source Database Metadata Imported!");
                        Console.WriteLine("Importing Target Database Metadata");
                        ImportDatabaseMetadata.ImportMetadataFromTargetDatabases();
                        Console.WriteLine("Target Database Metadata Imported!");
                        Console.WriteLine("Creating Tables, Views and Stored Procs from metadata");
                        using (SqlConnection conn = new SqlConnection(Generator.Properties.Settings.Default.MetadataDatabase))
                        {
                            conn.Open();
                            EssentialsAccountsDB.CreateObjectsFromMetadata(conn);
                            EssentialsAccountsSTG.CreateObjectsFromMetadata(conn, true);
                        }
                        Console.WriteLine("\nTables, Views and Stored Procs Recreated!");
                        break;

                    case Action.UpdateMetadataFromDeployedDatabases:
                        Console.WriteLine("Importing Target Database Metadata");
                        ImportDatabaseMetadata.ImportMetadataFromTargetDatabases();
                        Console.WriteLine("Target Database Metadata Imported!");
                        break;

                    case Action.CreateTablesViewsAndStoredProcsFromMetadata:
                        Console.WriteLine("Creating Tables, Views and Stored Procs from metadata");
                        Console.WriteLine(Generator.Properties.Settings.Default.MetadataDatabase);
                        using (SqlConnection conn = new SqlConnection(Generator.Properties.Settings.Default.MetadataDatabase))
                        {
                            conn.Open();
                            EssentialsAccountsDB.CreateObjectsFromMetadata(conn);
                            Console.WriteLine("\nIgnoring Staging tables!");
                            EssentialsAccountsSTG.CreateObjectsFromMetadata(conn, false);                           
                        }
                        Console.WriteLine("\nTables, Views and Stored Procs Created!");
                        break;

                    
                    case Action.Ignore:
                        break;

                    case Action.InvalidSelection:
                    default:
                        Console.WriteLine("Invalid key selection!");
                        break;
                }
            //}
            //catch (SqlException sqlEx)
            //{
            //    Console.WriteLine("sqlEx Error: {0} {1}", sqlEx.Message, sqlEx.InnerException.ToString());
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Exit Error: {0} {1}", ex.Message, ex.InnerException.ToString());
            //}
            Console.WriteLine("\n\nDone");
            Console.WriteLine("\n\nPress any key to exit");
            Console.ReadKey();

        }
    }
}
