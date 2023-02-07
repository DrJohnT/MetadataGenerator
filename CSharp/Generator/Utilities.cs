using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;

namespace Generator
{
    public static class Utilities
    {
        public static string GetProjectDir()
        {
            return Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
        }

        public static DirectoryInfo GetAndCleanOutputDir(string databaseName, string schemaName)
        {
            DirectoryInfo outputFolder = GetOutputDir(databaseName, schemaName, "");
            Utilities.CleanDirectory(outputFolder);
            return outputFolder;
        }

        public static DirectoryInfo GetAndCleanOutputDir(string databaseName, string schemaName, string subFolderName)
        {
            DirectoryInfo outputFolder = GetOutputDir(databaseName, schemaName, subFolderName);
            Utilities.CleanDirectory(outputFolder);
            return outputFolder;
        }

        public static DirectoryInfo GetOutputDir(string databaseName, string schemaName, string subFolderName)
        {
            DirectoryInfo outputFolder = Utilities.GetDirectory(Path.Combine(Generator.Properties.Settings.Default.OutputDirectory, databaseName, schemaName, subFolderName));
            return outputFolder;
        }

        public static DirectoryInfo GetDirectory(string path)
        {
            DirectoryInfo dir = new DirectoryInfo(path);
            if (!dir.Exists) dir.Create();
            return dir;
        }

        public static void CleanDirectory(DirectoryInfo directory)
        {
            // clean the output folder so we don't have old sql objects hanging around
            foreach (FileInfo file in directory.GetFiles())
            {
                file.Delete();
            }
        }

        public static string GetConnectionString(DatabaseInfo database)
        {
            SqlConnectionStringBuilder sb = new SqlConnectionStringBuilder();
            sb.InitialCatalog = database.DatabaseName;
            sb.DataSource = database.ServerName;
            sb.IntegratedSecurity = true;
            sb.ConnectTimeout = 30;
            return sb.ConnectionString;
        }

        /// <summary>
        /// Returns a single integer value from a sql query
        /// </summary>
        /// <param name="connection">Connection to the database</param>
        /// <param name="sql">Query to run which should return a single row with a single column</param>
        /// <returns></returns>
        public static int? ExecuteIntQuery(SqlConnection connection, string sql)
        {
            int? returnValue = null;
            try
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    //cmd.CommandTimeout = 120;  // default is 30 seconds
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            if (!reader.IsDBNull(0))
                                returnValue = int.Parse(reader[0].ToString());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ExecuteIntQuery Error: {0}", ex.Message);
            }
            return returnValue;
        }

        /// <summary>
        /// Returns a single string value from a sql query
        /// </summary>
        /// <param name="connection">Connection to the database</param>
        /// <param name="sql">Query to run which should return a single row with a single column</param>
        /// <returns></returns>
        public static string ExecuteStringQuery(SqlConnection connection, string sql)
        {
            string returnValue = null;
            try
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    //cmd.CommandTimeout = 120;  // default is 30 seconds
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            if (!reader.IsDBNull(0))
                                returnValue = reader[0].ToString();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ExecuteIntQuery Error: {0}", ex.Message);
            }
            return returnValue;
        }


        public static String ToCamelCase(this String s)
        {
            if (s == null) return s;

            String[] words = s.Split(' ');
            for (int i = 0; i < words.Length; i++)
            {
                if (words[i].Length == 0) continue;

                Char firstChar = Char.ToUpper(words[i][0]);
                String rest = "";
                if (words[i].Length > 1)
                {
                    rest = words[i].Substring(1).ToLower();
                }
                words[i] = firstChar + rest;
            }
            return String.Join("", words);
        }

        public static string ToCamelCaseRemovingPrefix(this string objectName, string prefix)
        {
            string returnStr = string.Empty;
            if (objectName.StartsWith(prefix))
                returnStr = objectName.Substring(2).ToLower();
            else
                returnStr = objectName.ToLower();

            return returnStr.ToCamelCaseReplacingUnderscores();
        }

        public static string ToCamelCaseReplacingUnderscores(this string objectName)
        {
            return objectName.Replace("_", " ").ToLower().ToCamelCase(); ;
        }
    }
}
