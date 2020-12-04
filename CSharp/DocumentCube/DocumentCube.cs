using System;
using System.Collections.Generic;
using System.Linq;
using MarkdownWikiGenerator;
using System.IO;
using Microsoft.AnalysisServices.Tabular;

namespace DocumentCube
{
    class DocumentCube
    {
        static void Main(string[] args)
        {

            Console.WriteLine("\n\nDid you change the version number in app.config?");
            Console.WriteLine("\n\nPress any key to continue to generate - or click X in corner of this dialog to EXIT");
            Console.ReadKey();

            using (Server svr = new Server())
            {
                svr.Connect(Properties.Settings.Default.AsServerName);

                string docName = "Essentials-Accounts-Cube-";
                string releaseVersion = "v" + Properties.Settings.Default.VersionNumber;

                Database db = svr.Databases.FindByName(Properties.Settings.Default.AsDatabaseId);
                if (db != null)
                {
                    int iMeasureCount = 0;
                    int iMeasureCountTM = 0;
                    int iCalcGroupMeasureCount = 0;
                    int iVisibleAttributeCount = 0;
                    int iHiddenAttributeCount = 0;

                    string MainFile = docName + releaseVersion + ".md";
                    string MainDaxFile = docName + releaseVersion +  "_DAX.md";
                    string TimeCalcsFile = docName + releaseVersion + "_TimeCalcs.md";

                    MarkdownBuilder mdb = new MarkdownBuilder();

                    mdb.Header(1, "Documentation for " + db.Name + " " + releaseVersion);

                    mdb.Header(2, "Table of Contents");
                    mdb.ListLink("Tables", "#Tables");
                    mdb.ListLink("Measures", "#Measures");
                    //mdb.ListLink("Calculation Groups", "#Calculation-Groups");

                    mdb.ListLink("DAX Expressions", "./" + MainDaxFile);
                    mdb.ListLink("DAX for Time Variants", "./" + TimeCalcsFile);
                    mdb.ListLink("Roles", "#Roles");
                    mdb.ListLink("Summary", "#Summary");

                    MarkdownBuilder mdbDAX = new MarkdownBuilder();
                    mdbDAX.Header(1, "DAX Expressions in " + db.Name + " " + releaseVersion);
                    mdbDAX.AppendLine("This file lists the DAX for each hand-crafted measures. ");
                    mdbDAX.Link("The list of automatically generated Time Calculations can be found here", "./" + TimeCalcsFile);
                    mdbDAX.AppendLine();

                    MarkdownBuilder mdbTM = new MarkdownBuilder();
                    mdbTM.Header(1, "AutoGen Time Calcs in " + db.Name + " " + releaseVersion);
                    mdbTM.AppendLine("This file lists the automatically generated Time Calculations");


                    Model m = db.Model;

                    mdb.AppendLine();
                    mdb.Header(1, "Tables");

                    foreach (Table table in m.Tables.OrderBy(x => x.Name))
                    {
                        mdb.AppendLine();
                        mdb.Header(2, table.Name);
                        mdb.AppendLine();

                        string[] headers = new[] { "Column Name", "Description", "Display Folder", "DataType", "IsHidden", "Format String", "Sort By Column", "Type", "IsKey", "IsUnique" };

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
                                row[6] = col.SortByColumn.Name;
                            row[7] = col.Type.ToString();
                            row[8] = col.IsKey.ToString();
                            row[9] = col.IsUnique.ToString();

                            rows.Add(row);

                            if (col.IsHidden)
                                iHiddenAttributeCount++;
                            else
                                iVisibleAttributeCount++;
                        }
                        mdb.Table(headers, rows);
                    }
                    mdb.AppendLine();
                    mdb.Header(1, "Measures");
                    {
                        string[] headers = new[] { "Display Folder", "Measure Name", "Description", "DataType", "IsHidden", "Format String" };
                        
                        List<string[]> rows = new List<string[]>();
                        List<string[]> rowsTM = new List<string[]>();

                        foreach (Table table in m.Tables.OrderBy(x => x.Name))
                        {

                            foreach (Measure measure in table.Measures.OrderBy(x => x.DisplayFolder))
                            {
                                string[] row = new string[6];
                                row[0] = measure.DisplayFolder;
                                row[1] = measure.Name;
                                row[2] = measure.Description;
                                row[3] = measure.DataType.ToString();
                                row[4] = measure.IsHidden.ToString();
                                row[5] = measure.FormatString;
                                //var annotation = measure.Annotations.FirstOrDefault(x => x.Name == "AutoGen");
                                if (measure.Description == "TimeCalc")
                                    rowsTM.Add(row);
                                else
                                    rows.Add(row);
                            }

                        }

                        mdb.Table(headers, rows);
                        mdbTM.Table(headers, rowsTM);
                    }

                    //mdb.AppendLine();
                    //mdb.Header(1, "Calculation Groups");
                    //mdb.AppendLine();
                    //foreach (Table table in m.Tables)
                    //{
                    //    if (table.CalculationGroup != null)
                    //    {
                    //        mdb.Header(2, "Calculation Group: " + table.Name);
                    //        mdb.AppendLine();
                    //        foreach (CalculationItem calc in table.CalculationGroup.CalculationItems.OrderBy(x => x.Name))
                    //        {
                    //            mdb.AppendLine();
                    //            mdb.Header(3, calc.Name);
                    //            mdb.SmallFont(calc.Description);
                    //            mdb.AppendLine();
                    //            mdb.Code("DAX", calc.Expression);
                    //            mdb.AppendLine();

                    //            iCalcGroupMeasureCount++;
                    //        }
                    //    }
                    //}
                    
                    mdb.AppendLine();
                    mdb.Header(1, "DAX Expressions");
                    
                    mdb.AppendLine();

                    string PrevDisplayFolder = string.Empty;

                    foreach (Table table in m.Tables)
                    {
                        foreach (Measure measure in table.Measures.OrderBy(x => x.DisplayFolder))
                        {
                            //var annotation = measure.Annotations.FirstOrDefault(x => x.Name == "AutoGen");
                            if (measure.Description == "TimeCalc")
                            {
                                //mdbTM.AppendLine();
                                //mdbTM.Header(2, measure.Name);
                                //mdbTM.SmallFont(measure.Description);
                                //mdbTM.AppendLine();
                                //mdbTM.Code("DAX", measure.Expression);
                                //mdbTM.AppendLine();
                                iMeasureCountTM++;
                            }
                            else
                            { 
                                if (PrevDisplayFolder != measure.DisplayFolder)
                                {
                                    mdbDAX.Header(2, "Folder: " + measure.DisplayFolder);
                                }
                                mdbDAX.AppendLine();
                                mdbDAX.Header(3, measure.Name);
                                mdbDAX.SmallFont(measure.Description);
                                mdbDAX.AppendLine();
                                mdbDAX.Code("DAX", measure.Expression);
                                mdbDAX.AppendLine();
                                iMeasureCount++;
                                PrevDisplayFolder = measure.DisplayFolder;
                            }
                        }
                    }

                    mdb.AppendLine();
                    mdb.Header(1, "Roles");
                    mdb.AppendLine();
                    foreach (ModelRole role in m.Roles.OrderBy(x => x.Name))
                    {
                        mdb.AppendLine();
                        mdb.Header(2, role.Name);
                        mdb.Header(3, "Table Permissions");
                        foreach (TablePermission permission in role.TablePermissions.OrderBy(x => x.Table.Name))
                        {
                            mdb.Header(4, permission.Table.Name);
                            mdb.Code("DAX", permission.FilterExpression);                            
                        }
                        mdb.Header(3, "Members");
                        foreach (ModelRoleMember member in role.Members)
                        {                            
                            mdb.List(member.Name);
                        }
                        mdb.AppendLine();
                    }

                    mdb.AppendLine();
                    mdb.Header(1, "Summary");
                    mdb.AppendLine();
                    string msg = string.Format("The cube {0} now contains {2} calculation group expressions, {1} measures and {5} time metric variants.  There are {3} visible attributes and {4} hidden attributes (keys etc.).", 
                        db.Name,
                        iMeasureCount,
                        iCalcGroupMeasureCount,
                        iVisibleAttributeCount,
                        iHiddenAttributeCount,
                        iMeasureCountTM
                    );
                    mdb.AppendLine(msg);


                    string filePath = Path.Combine(Properties.Settings.Default.OutputDir, MainFile);
                    File.WriteAllText(filePath, mdb.ToString());

                    filePath = Path.Combine(Properties.Settings.Default.OutputDir, MainDaxFile);
                    File.WriteAllText(filePath, mdbDAX.ToString());

                    filePath = Path.Combine(Properties.Settings.Default.OutputDir, TimeCalcsFile);
                    File.WriteAllText(filePath, mdbTM.ToString());
                }

                svr.Disconnect();
            }
            Console.WriteLine("Done!");
        }
    }
}
