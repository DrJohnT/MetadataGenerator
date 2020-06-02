using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MarkdownWikiGenerator;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.AnalysisServices.Tabular;

namespace DocumentCube
{
    class DocumentCube
    {
        static void Main(string[] args)
        {
            using (Server svr = new Server())
            {
                svr.Connect(Properties.Settings.Default.AsServerName);

                Database db = svr.Databases.FindByName(Properties.Settings.Default.AsDatabaseId);
                if (db != null)
                {
                    MarkdownBuilder mdb = new MarkdownBuilder();

                    mdb.Header(1, db.Name + " Documentation");


                    mdb.ListLink("Tables", "#Tables");
                    mdb.ListLink("Measures", "#Measures");
                    mdb.ListLink("DAX Expressions", "#DAX-Expressions");

                    Model m = db.Model;

                    mdb.AppendLine();
                    mdb.Header(1, "Tables");

                    foreach (Table table in m.Tables.OrderBy(x => x.Name))
                    {
                        mdb.AppendLine();
                        mdb.Header(2, table.Name);
                        mdb.AppendLine();

                        string[] headers = new[] { "Column Name", "Description", "DisplayFolder", "DataType", "IsHidden", "FormatString", "SortByColumn", "Type", "IsKey", "IsUnique" };

                        List<string[]> rows = new List<string[]>();

                        foreach (Column col in table.Columns.OrderBy(x => x.Name))
                        {
                            string[] row = new string[10];
                            row[0] = col.Name;
                            row[1] = col.Description;
                            row[2] = col.DisplayFolder;
                            row[3] = col.DataType.ToString();
                            row[4] = col.IsHidden.ToString();
                            row[5] = col.FormatString;
                            if (col.SortByColumn == null)
                                row[6] = "";
                            else
                                row[6] = col.SortByColumn.ToString();
                            row[7] = col.Type.ToString();
                            row[8] = col.IsKey.ToString();
                            row[9] = col.IsUnique.ToString();

                            rows.Add(row);
                        }
                        mdb.Table(headers, rows);
                    }
                    mdb.AppendLine();
                    mdb.Header(1, "Measures");
                    {
                        string[] headers = new[] { "Table Name", "Measure Name", "Description", "DisplayFolder", "DataType", "IsHidden", "FormatString" };
                        
                        List<string[]> rows = new List<string[]>();

                        foreach (Table table in m.Tables.OrderBy(x => x.Name))
                        {

                            foreach (Measure measure in table.Measures.OrderBy(x => x.Name))
                            {
                                string[] row = new string[7];
                                row[0] = table.Name;
                                row[1] = measure.Name;
                                row[2] = measure.Description;
                                row[3] = measure.DisplayFolder;
                                row[4] = measure.DataType.ToString();
                                row[5] = measure.IsHidden.ToString();
                                row[6] = measure.FormatString;

                                rows.Add(row);
                            }

                        }

                        mdb.Table(headers, rows);
                    }
                    mdb.AppendLine();
                    mdb.Header(1, "DAX Expressions");
                    mdb.AppendLine();
                    mdb.AppendLine("<hr>");
                    foreach (Table table in m.Tables)
                    {

                        foreach (Measure measure in table.Measures.OrderBy(x => x.Name))
                        {
                            mdb.AppendLine();
                            mdb.Header(2, measure.Name);
                            mdb.AppendLine();
                            mdb.Code("DAX", measure.Expression);
                            mdb.AppendLine();
                            mdb.AppendLine("<hr>");
                        }

                    }
                
                
                    string filePath = Path.Combine(Properties.Settings.Default.OutputDir, db.Name + ".md");
                    File.WriteAllText(filePath, mdb.ToString());
                    
                }

                svr.Disconnect();


            }
            Console.WriteLine("Done!");
            //Console.ReadKey();
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
