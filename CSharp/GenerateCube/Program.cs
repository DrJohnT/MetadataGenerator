using System;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.AnalysisServices.Tabular;

namespace GenerateCube
{
    class Program
    {
        static void Main(string[] args)
        {
            using (Server svr = new Server())
            {
                svr.Connect(Properties.Settings.Default.AsServerName);

                Database db = svr.Databases.FindByName(Properties.Settings.Default.AsDatabaseId);
                if (db != null)
                { 
                    Console.WriteLine(db.CompatibilityLevel);
                    Model m = db.Model;

                    foreach (Table table in m.Tables)
                    {
                        foreach (Column col in table.Columns)
                        {
                            if (col.Name.ToLower().EndsWith("key"))
                            {
                                // hide key columns
                                col.IsHidden = true;
                            }
                            else if (col.Name.ToLower() == "sortorder" || col.Name.ToLower() == "sort order")
                            {
                                col.IsHidden = true;
                                //col.SortByColumn = col;
                            }
                            else if (col.Name.StartsWith("m_") || col.Name.StartsWith("a_"))
                            {
                                // hide the original column
                                col.IsHidden = true;

                                string measureName = SplitCamelCase(col.Name.Substring(2));
                                string func = "sum"; // default
                                if (col.Name.StartsWith("a_"))
                                    func = "avg";

                                // check if the measure already exists 
                                Measure measure = table.Measures.Find(measureName);
                                if (measure == null)
                                {
                                    // the measure does not exist, so add it
                                    Console.WriteLine("Creating measure {0}", measureName);
                                    measure = new Measure();
                                    measure.Name = measureName;
                                    measure.Expression = string.Format(" {0}({1}[{2}])", func, table.Name, col.Name);
                                    measure.FormatString = GetMeasureFormatString(col.Name);
                                    
                                    // add the new measure to the cube
                                    table.Measures.Add(measure);
                                }
                                else
                                {
                                    // set the format string (in case we missed it last time)
                                    measure.FormatString = GetMeasureFormatString(col.Name);
                                }
                            }
                            else if (col.Type.ToString() == "Data")
                            {
                                // camel case other Data columns
                                col.Name = SplitCamelCase(col.Name);
                            }
                        }
                    }
                    
                    // save the new measures and renamed columns to the server
                    db.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);

                    // export the cube definition to the file system so we can diff using Beyond Compare
                    ExportCubeDefinitionToDisk.ExportCubeDefinition(db);

                }
                svr.Disconnect();
            }
         
            Console.WriteLine("Done!");
            //Console.ReadKey();
        }

        private static string GetMeasureFormatString(string columnName)
        {
            const string CurrencyFormatString = "\"\"#,0;-\"\"#,0;\"\"#,0";
            const string WholeNumberFormatString = "\"\"#,0;-\"\"#,0;\"\"#,0";
            if (columnName.ToLower().IndexOf("count") > 0)
            {
                return WholeNumberFormatString;
            }
            else
            {
                return CurrencyFormatString;
            }
        }
       
        #region General Helper functions
        private static string GetProjectDir()
        {
            return Path.GetDirectoryName(Path.GetDirectoryName(System.IO.Directory.GetCurrentDirectory()));
        }

        public static string SplitCamelCase(string str)
        {
            if (!str.Contains(" "))
            {
                string returnValue = Regex.Replace(
                    Regex.Replace(
                        str,
                        @"(\P{Ll})(\P{Ll}\p{Ll})",
                        "$1 $2"
                    ),
                    @"(\p{Ll})(\P{Ll})",
                    "$1 $2"
                );
                return returnValue.Replace("_", "").Replace("  ", " ");
            }
            else
                return str;
        }
        #endregion
    }
}
