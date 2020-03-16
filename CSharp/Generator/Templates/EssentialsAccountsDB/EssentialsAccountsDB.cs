using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Data.SqlClient;
using Dapper;

namespace Generator
{
    /// <summary>
    /// Creates tables and views used by the main database
    /// </summary>
    class EssentialsAccountsDB
    {
        public const string DatabaseName = "EssentialsAccountsDB";
        private const string LoadingSchema = "qLoad";
        private const string LookupSchema = "qLookup";
        private const string ExtractSchema = "qExtract";
        private const string TableSchema = "qDm";
        private const string DataMartSchema = "qDataMart";
        private const string CubeSchema = "qCube";

        public static void CreateObjectsFromMetadata(SqlConnection conn)
        {
            try
            {
                Console.WriteLine();
                Console.WriteLine("Creating objects for {0}", DatabaseName);
             
                // get a reference to the database so we can generate tables
                string sql = string.Format(@"
select 
	B.DatabaseInfoId,
	B.DatabaseName,
	B.DatabaseDescription,
	B.ServerName
from Metadata.DatabaseInfo B 
	join metadata.DatabaseUse A on
		A.DatabaseUseId = B.DatabaseUseId
where A.DatabaseUse = 'TARGET' and B.DatabaseName = '{0}'
", DatabaseName);

                List<DatabaseInfo> databases = conn.Query<DatabaseInfo>(sql).ToList();

                DatabaseInfo database = databases.First();  // we only have one database to generate for

                string[] staticDims = { "dimCalendar" };

                DirectoryInfo dirPostDeployFolder = Utilities.GetAndCleanOutputDir(DatabaseName, @"Scripts\Post-Deploy");

                DirectoryInfo dirLoadProcs = Utilities.GetAndCleanOutputDir(DatabaseName, LoadingSchema, "Stored Procedures");

                DirectoryInfo dirLookupProcs = Utilities.GetAndCleanOutputDir(DatabaseName, LookupSchema, "Stored Procedures");

                DirectoryInfo dirLoadViewStubs = Utilities.GetAndCleanOutputDir(DatabaseName, LoadingSchema, "Views");

                DirectoryInfo dirTables = Utilities.GetAndCleanOutputDir(DatabaseName, TableSchema, "Tables");

                DirectoryInfo dirDataMartViews = Utilities.GetAndCleanOutputDir(DatabaseName, DataMartSchema, "Views");

                DirectoryInfo dirCubeViews = Utilities.GetAndCleanOutputDir(DatabaseName, CubeSchema, "Views");

                DirectoryInfo dirExtractViews = Utilities.GetAndCleanOutputDir(DatabaseName, ExtractSchema, "Views");
                
                string checkInsertViewsSP = string.Empty;
                string insertUnknowns = string.Empty;

                IEnumerable<DatabaseColumn> policyInwardViewFilteredColumns = null;
                IEnumerable<DatabaseColumn> policyOutwardViewFilteredColumns = null;

                DataModel dm = PopulateFromMetadataDatabase.PopulateDataModelFromMetadataDatabase(database, conn);
                foreach (DatabaseObject table in dm.Objects.Where(x => x.SchemaName.ToUpper() == TableSchema.ToUpper() && x.DatabaseObjectType.Trim().ToUpper() == "U"))
                {
                    // create an artificial sort order where the dimXXXid columns are at the beginning
                    foreach (DatabaseColumn column in table.Columns.Where(x => x.DatabaseColumnName.StartsWith("dim")))
                    {
                        column.DatabaseColumnSortOrder = "0" + column.DatabaseColumnName;
                    }

                    string[] firstColumns = { "PropNonProp", "PolicyReference","Company", "PolicyDirection", "StatusDescription", "TreatyFac", "UnderwritingYear", "TreatyCode", "UnderwriterName" };
                    foreach (DatabaseColumn column in table.Columns.Where(x => firstColumns.Contains(x.DatabaseColumnName)))
                    {
                        column.DatabaseColumnSortOrder = "1" + column.DatabaseColumnName;
                    }

                    if (!staticDims.Contains(table.DatabaseObjectName))
                    {
                       
                       TemplateCommon.StandardMergeSP(table, dirLoadProcs, LoadingSchema, null);
                      
                    }
                    {
                        IEnumerable<DatabaseColumn> loadViewFilteredColumns = table.Columns.Where(column => (column.DatabaseColumnName.ToLower() != DatabaseObject.updatedloadlogid) &&
                           column.DatabaseColumnName.ToLower().EndsWith("id") && !column.IsIdentity);

                        // Create stub of the load view
                        CreateLoadViewStub(table, dirLoadViewStubs, LoadingSchema, table.GetColumnListSql(loadViewFilteredColumns));
                    }
                    {
                        // SELECT distinct DatabaseColumnName FROM Metadata.ColumnInfo where DatabaseInfoId = 15 and ObjectType = 'Table' and LEFT(DatabaseColumnName,3) = 'dim'
                        // following keys are allowed in the views:
                        //dimAccountId
                        //dimBrokerId
                        //dimCedantId
                        //dimClaimId
                        //dimDateId
                        string[] excludeColumns = { "policynumber", "policysequencenumber", "companycode", "departmentcode", "policyoutwardparentid", "dimoutwardbrokerid", "dimoutwardreinsurerid", "dimoutwardtermid", "dimpolicyinwardprorataid", "dimpolicyinwardregulatoryid", "dimpolicyinwardxlid", "dimpolicylimitid", "dimpolicyreinsuranceid", "dimpolicyinwardtermid", "dimpolicyinwardid", "dimpolicyoutwardid", "accountcode" };

                        IEnumerable<DatabaseColumn> dataMartViewFilteredColumns = table.Columns.Where(column => (!DatabaseObject.allStandardColumns.Contains(column.DatabaseColumnName.ToLower()) &&                           
                            !excludeColumns.Contains(column.DatabaseColumnName.ToLower()) &&
                            !column.DatabaseColumnName.ToLower().StartsWith("spk") &&
                            !column.DatabaseColumnName.ToLower().StartsWith("sfk") &&
                            !(column.DatabaseColumnName.ToLower().StartsWith("fact") && column.DatabaseColumnName.ToLower().EndsWith("id"))));

                        // for Datamart views (used below)
                        string[] policyInwardDims = { "dimpolicy", "dimpolicyinward", "dimpolicyinwardxl"};
                        if (policyInwardDims.Contains(table.DatabaseObjectName.ToLower()))
                        {
                            Console.WriteLine();
                            Console.WriteLine("Addding {0} to {1}", table.DatabaseObjectName, "qUDM_PolicyInward");
                            if (policyInwardViewFilteredColumns == null)
                                policyInwardViewFilteredColumns = dataMartViewFilteredColumns;
                            else
                            {
                                ColumnComparer columnComparer = new ColumnComparer();
                                foreach (DatabaseColumn column in dataMartViewFilteredColumns.Where(x => !policyInwardViewFilteredColumns.Contains<DatabaseColumn>(x, columnComparer)))
                                {
                                    // add column to common view
                                    policyInwardViewFilteredColumns = policyInwardViewFilteredColumns.Concat(new[] { column });
                                }
                            }
                        }

                        // for Datamart views (used below)
                        string[] policyOutwardDims = { "dimpolicy", "dimpolicyoutward" };
                        if (policyOutwardDims.Contains(table.DatabaseObjectName.ToLower()))
                        {
                            Console.WriteLine();
                            Console.WriteLine("Addding {0} to {1}", table.DatabaseObjectName, "qUDM_PolicyOutward");
                            if (policyOutwardViewFilteredColumns == null)
                                policyOutwardViewFilteredColumns = dataMartViewFilteredColumns;
                            else
                            {
                                ColumnComparer columnComparer = new ColumnComparer();
                                foreach (DatabaseColumn column in dataMartViewFilteredColumns.Where(x => !policyOutwardViewFilteredColumns.Contains<DatabaseColumn>(x, columnComparer)))
                                {
                                    // add column to common view
                                    policyOutwardViewFilteredColumns = policyOutwardViewFilteredColumns.Concat(new[] { column });
                                }
                            }
                        }

                        // DataMart views
                        CreateDataMartView(table, dirDataMartViews, DataMartSchema, table.GetViewColumnListSql(dataMartViewFilteredColumns));

                        // cube views
                        CreateDataMartView(table, dirCubeViews, CubeSchema, table.GetColumnListSql(dataMartViewFilteredColumns));
                    }

                    string[] excludeDimensions = { "dimcalendar" };
                    if (!excludeDimensions.Contains(table.DatabaseObjectName.ToLower()))
                    {
                        
                        IEnumerable<DatabaseColumn> checkInsertViewFilteredColumns = table.Columns.Where(column => (column.DatabaseColumnName.ToLower() != DatabaseObject.updatedloadlogid && !column.IsIdentity));
                        checkInsertViewsSP += TemplateComponents.CheckInsertView(LoadingSchema, table.DatabaseObjectName, table.GetColumnListSql(checkInsertViewFilteredColumns));

                        // include dimensions
                        if (table.DatabaseObjectName.ToLower().StartsWith("dim"))
                            insertUnknowns += InsertUnknowns(LoadingSchema, table);
                    }
                }

                // Special case - join three dimensions together for qDataMart.dimPolicyInward and qUDM_PolicyInward
                {
                    DatabaseObject table = dm.Objects.First<DatabaseObject>(x => x.DatabaseObjectName == "dimPolicyInward" && x.SchemaName.ToUpper() == TableSchema.ToUpper() && x.DatabaseObjectType.Trim().ToUpper() == "U");
                    foreach (DatabaseColumn column in policyInwardViewFilteredColumns.Where(x => x.DatabaseObjectName == "dimPolicyInwardXl"))
                    {
                        // change column prefix
                        column.DatabaseColumnPrefix = "PXL";
                    }
                    foreach (DatabaseColumn column in policyInwardViewFilteredColumns.Where(x => x.DatabaseObjectName == "dimPolicy"))
                    {
                        // change column prefix
                        column.DatabaseColumnPrefix = "POL";
                    }
                    // DataMart views
                    CreateDataMartView(table, dirDataMartViews, DataMartSchema, table.GetViewColumnListSql(policyInwardViewFilteredColumns), "CreatePolicyInwardView.sql");

                    // cube views
                    CreateDataMartView(table, dirCubeViews, CubeSchema, table.GetColumnListSql(policyInwardViewFilteredColumns), "CreatePolicyInwardView.sql");

                    // Extract view
                    CreateDataMartView(table, dirExtractViews, ExtractSchema, table.GetViewColumnListSql(policyInwardViewFilteredColumns), "CreateqUDM_PolicyInward.sql", "qUDM_PolicyInward");
                }

                // Special case - join two dimensions together for qDataMart.dimPolicyOutward and qUDM_PolicyOutward
                {
                    DatabaseObject table = dm.Objects.First<DatabaseObject>(x => x.DatabaseObjectName == "dimPolicyOutward" && x.SchemaName.ToUpper() == TableSchema.ToUpper() && x.DatabaseObjectType.Trim().ToUpper() == "U");

                    foreach (DatabaseColumn column in policyOutwardViewFilteredColumns.Where(x => x.DatabaseObjectName == "dimPolicy"))
                    {
                        // change column prefix
                        column.DatabaseColumnPrefix = "POL";
                    }
                    // DataMart views
                    CreateDataMartView(table, dirDataMartViews, DataMartSchema, table.GetViewColumnListSql(policyOutwardViewFilteredColumns), "CreatePolicyOutwardView.sql");

                    // cube views
                    CreateDataMartView(table, dirCubeViews, CubeSchema, table.GetColumnListSql(policyOutwardViewFilteredColumns), "CreatePolicyOutwardView.sql");

                    // Extract view
                    CreateDataMartView(table, dirExtractViews, ExtractSchema, table.GetViewColumnListSql(policyOutwardViewFilteredColumns), "CreateqUDM_PolicyOutward.sql", "qUDM_PolicyOutward");
                }


                CheckInsertViews(dirLoadProcs, checkInsertViewsSP);

                InsertUnknownsScript(dirPostDeployFolder, insertUnknowns);

                foreach (DatabaseObject table in dm.Objects.Where(x => x.SchemaName.ToUpper() == LookupSchema.ToUpper() && x.DatabaseObjectType.Trim().ToUpper() == "U"))
                {
                    TemplateCommon.StandardMergeSP(table, dirLookupProcs, LookupSchema, "lookup");
                }

                Console.WriteLine("Done all " + DatabaseName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in EssentialsAccountsDB.CreateObjectsFromMetadata {0}", ex.Message);
            }
        }

        private static void CreateLoadViewStub(DatabaseObject table, DirectoryInfo dirLoadViewSubs, string viewSchema, string columnList)
        {
            string sqlScript = Templates.GetTemplateContent(DatabaseName, "CreateLoadViewStub.sql");

            string sql = string.Format(sqlScript,
                viewSchema,                         // {0} = view schema name
                table.DatabaseObjectName,           // {1} = table name
                columnList ,                        // {2} = column list
                "dt_" + table.DatabaseObjectName    // {3} = data translation list
                );

            string sqlPath = Path.Combine(dirLoadViewSubs.FullName, string.Format("{0}Insert.sql", table.DatabaseObjectName));
            Console.Write("s");
            System.IO.File.WriteAllText(sqlPath, sql);
        }

        private static void CreateDataMartView(DatabaseObject table, DirectoryInfo dirDimViews, string viewSchema, string columnList)
        {
            CreateDataMartView(table, dirDimViews, viewSchema, columnList, null, null);
        }

        private static void CreateDataMartView(DatabaseObject table, DirectoryInfo dirDimViews, string viewSchema, string columnList, string templateName)
        {
            CreateDataMartView(table, dirDimViews, viewSchema, columnList, templateName, null);
        }


        private static void CreateDataMartView(DatabaseObject table, DirectoryInfo dirDimViews, string viewSchema, string columnList, string templateName, string outputFileName)
        {
            if (templateName == null)
                templateName = "CreateDataMartView.sql";

            if (outputFileName == null)
                outputFileName = table.DatabaseObjectName;

            string sqlScript = Templates.GetTemplateContent(DatabaseName, templateName);

            string sql = string.Format(sqlScript,
                viewSchema,                         // {0} = view schema name
                outputFileName,                     // {1} = table name
                table.SchemaName,                   // {2} = table schema
                columnList                          // {3} = column list
                );

            string sqlPath = Path.Combine(dirDimViews.FullName, string.Format("{0}.sql", outputFileName));
            Console.Write("v");
            System.IO.File.WriteAllText(sqlPath, sql);
        }


        private static string InsertUnknowns(string LoadingSchema, DatabaseObject table)
        {
            string sqlScript = Templates.GetTemplateContent(DatabaseName, "InsertUnknowns.sql");
            string sql = string.Empty;

            if (table.Columns.Count(column => column.IsPrimaryKey) > 0)
            {
                string setIdentity = "--";
                if (table.Columns.Where(column => column.IsIdentity == true).Count() > 0)
                    setIdentity = string.Empty;

                string[] excludeColumnsFromInsertView = { DatabaseObject.updatedloadlogid };
                IEnumerable<DatabaseColumn> filteredColumns = table.Columns.Where(column => (!excludeColumnsFromInsertView.Contains(column.DatabaseColumnName.ToLower()) && !column.IsIdentity && !column.IsNullable));

                sql = string.Format(sqlScript, 
                    table.SchemaName, 
                    table.DatabaseObjectName, 
                    table.DatabaseObjectName + "Id", 
                    table.GetColumnListSql(filteredColumns, string.Empty), 
                    table.GetUnknownList(filteredColumns, "N'UNKNOWN'", "N'U'", -1), 
                    -1, 
                    setIdentity);
            }
            return sql;
        }

        private static void CheckInsertViews(DirectoryInfo dirProcs, string AllChecks)
        {
            string sqlScript = Templates.GetTemplateContent(DatabaseName, "CheckInsertViews.sql");
            string sql = string.Format(sqlScript, LoadingSchema, AllChecks);
            string sqlPath = Path.Combine(dirProcs.FullName, "CheckInsertViews.sql");
            Console.WriteLine();
            Console.WriteLine("Generated CheckInsertViews.sql");
            System.IO.File.WriteAllText(sqlPath, sql);
        }

        private static void InsertUnknownsScript(DirectoryInfo dirPostDeployFolder, string insertUnknowns)
        {
            string sqlScript = Templates.GetTemplateContent(DatabaseName, "InsertUnknownsScript.sql");
            string sql = string.Format(sqlScript, insertUnknowns);
            string sqlPath = Path.Combine(dirPostDeployFolder.FullName, "AddNegativePKToDimTables.sql");
            Console.WriteLine("Generated AddNegativePKToDimTables.sql");
            System.IO.File.WriteAllText(sqlPath, sql);

        }

    }
}
