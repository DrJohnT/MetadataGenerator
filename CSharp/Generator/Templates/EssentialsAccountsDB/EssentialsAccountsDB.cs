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
        private const string LoadingSchema = "eaLoad";
        private const string LookupSchema = "eaLookup";
        private const string ExtractSchema = "eaExtract";
        private const string TableSchema = "eaDataMart";
        private const string DataMartSchema = "eaDataMart";
        private const string CubeSchema = "eaCubeView";
        private const string StagingAreaSchema = "stgMdfl";        

        public static void CreateObjectsFromMetadata(SqlConnection conn)
        {
            //try
            //{
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

                string[] staticDims = { "" };

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

                DataModel dm = PopulateFromMetadataDatabase.PopulateDataModelFromMetadataDatabase(database, conn);
                foreach (DatabaseObject table in dm.Objects.Where(x => x.SchemaName.ToUpper() == TableSchema.ToUpper() && x.DatabaseObjectType.Trim().ToUpper() == "U"))
                {
                    // create an artificial sort order where the dimXXXid columns are at the beginning
                    foreach (DatabaseColumn column in table.Columns.Where(x => x.DatabaseColumnName.ToLower().StartsWith("dim")))
                    {
                        column.DatabaseColumnSortOrder = "0" + column.DatabaseColumnName;
                    }

                    string[] firstColumns = { "XXX" };
                    foreach (DatabaseColumn column in table.Columns.Where(x => firstColumns.Contains(x.DatabaseColumnName)))
                    {
                        column.DatabaseColumnSortOrder = "1" + column.DatabaseColumnName;
                    }

                    if (!staticDims.Contains(table.DatabaseObjectName) && (
                        (table.DatabaseObjectName.StartsWith("Dim") || table.DatabaseObjectName.StartsWith("Security") 
                        || table.DatabaseObjectName.StartsWith("Active") || table.DatabaseObjectName.StartsWith("Masked"))
                    ))
                    {
                        // if we have a primary key in the table, then use standard merge template, otherwise use truncate and load
                        int iCount = table.Columns.Where(column => column.IsPrimaryKey == true).Count();
                        if (iCount > 0)
                            TemplateCommon.StandardMergeSP(table, dirLoadProcs, LoadingSchema, null);
                        else
                            TemplateCommon.StandardInsertSP(table, dirLoadProcs, LoadingSchema, null);
                    }

                    
                    
                    if (table.DatabaseObjectName.StartsWith("Fact"))
                    {
                        // if we have a primary key in the fact table, then use simple merge template, otherwise use truncate and load
                        int iCount = table.Columns.Where(column => column.IsPrimaryKey == true).Count();
                        if (iCount > 0)
                            TemplateCommon.StandardMergeSP(table, dirLoadProcs, LoadingSchema, null);
                        else
                            TemplateCommon.StandardInsertSP(table, dirLoadProcs, LoadingSchema, null);

                    }

                    if ( table.DatabaseObjectName.StartsWith("Ref") || table.DatabaseObjectName.StartsWith("Bridge"))
                    {
                        TemplateCommon.StandardInsertSP(table, dirLoadProcs, LoadingSchema, null);
                    }

                {
                    IEnumerable<DatabaseColumn> loadViewFilteredColumns = table.Columns;
                        //.Where(column => (column.DatabaseColumnName.ToLower() != DatabaseObject.updatedloadlogid) &&
                          // column.DatabaseColumnName.ToLower().EndsWith("key") && !column.IsIdentity);

                        // Create stub of the load view
                        CreateLoadViewStub(table, dirLoadViewStubs, LoadingSchema, table.GetColumnListSql(loadViewFilteredColumns), StagingAreaSchema);
                    }
                    {
                        string[] excludeColumns = { "XXX" };

                        IEnumerable<DatabaseColumn> dataMartViewFilteredColumns = table.Columns.Where(column => (!DatabaseObject.allStandardColumns.Contains(column.DatabaseColumnName.ToLower()) &&                           
                            !excludeColumns.Contains(column.DatabaseColumnName.ToLower()) &&
                            !(column.DatabaseColumnName.ToLower().StartsWith("fact") && column.DatabaseColumnName.ToLower().EndsWith("key"))));

                        // DataMart views
                        CreateDataMartView(table, dirDataMartViews, DataMartSchema, table.GetViewColumnListSql(dataMartViewFilteredColumns));

                        // cube views
                        CreateDataMartView(table, dirCubeViews, CubeSchema, table.GetColumnListSql(dataMartViewFilteredColumns));
                    }

                    string[] excludeDimensions = { "" };
                    if (!excludeDimensions.Contains(table.DatabaseObjectName.ToLower()))
                    {
                        
                        IEnumerable<DatabaseColumn> checkInsertViewFilteredColumns = table.Columns.Where(column => (!DatabaseObject.allStandardColumns.Contains(column.DatabaseColumnName.ToLower()) && 
                            !column.IsIdentity));
                        checkInsertViewsSP += TemplateComponents.CheckInsertView(LoadingSchema, table.DatabaseObjectName, table.GetColumnListSql(checkInsertViewFilteredColumns));

                        // include dimensions
                        if (table.DatabaseObjectName.ToLower().StartsWith("dim"))
                            insertUnknowns += InsertUnknowns(LoadingSchema, table);
                    }
                }

                CheckInsertViews(dirLoadProcs, checkInsertViewsSP);

                InsertUnknownsScript(dirPostDeployFolder, insertUnknowns);

                foreach (DatabaseObject table in dm.Objects.Where(x => x.SchemaName.ToUpper() == LookupSchema.ToUpper() && x.DatabaseObjectType.Trim().ToUpper() == "U"))
                {
                    if (table.DatabaseObjectName.StartsWith("Bridge"))                    
                        TemplateCommon.StandardInsertSP(table, dirLookupProcs, LookupSchema, "lookup");
                    else
                        TemplateCommon.StandardMergeSP(table, dirLookupProcs, LookupSchema, "lookup");
                }

                Console.WriteLine("Done all " + DatabaseName);
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Error in EssentialsAccountsDB.CreateObjectsFromMetadata {0}", ex.Message);
            //}
        }

        private static void CreateLoadViewStub(DatabaseObject table, DirectoryInfo dirLoadViewSubs, string viewSchema, string columnList, string StagingAreaSchema)
        {
            string sqlScript = Templates.GetTemplateContent(DatabaseName, "CreateLoadViewStub.sql");

            string sql = string.Format(sqlScript,
                viewSchema,                         // {0} = view schema name
                table.DatabaseObjectName,           // {1} = table name
                columnList ,                        // {2} = column list
                "dt_" + table.DatabaseObjectName,    // {3} = data translation list
                StagingAreaSchema                    // {4} = staging area schema
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
                    table.DatabaseObjectName + "Key", 
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
