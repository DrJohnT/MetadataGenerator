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
    /// Creates tables and views used by the staging database
    /// </summary>
    public class StubSourceTarget
    {
        public const string StubDatabaseName = "StubSourceTarget";
        public const string UamDatabaseName = "UAM_DB";
        public static void CreateStagingTablesFromMetadata(SqlConnection conn)
        {
            Console.WriteLine();
            Console.WriteLine("Creating objects for {0} and {1}", StubDatabaseName, UamDatabaseName);
            
            string DatabaseDescription = "FR";

            // get the list of databases to generate tables
            string sql = string.Format(@"
            SELECT 
	            B.DatabaseInfoId,
                B.DatabaseGroupId,
	            B.DatabaseName,
	            B.DatabaseDescription,
	            B.ServerName,
	            B.ImportMetadata,
                B.pkPrefix,
                B.DatabaseGroup
            FROM Metadata.DatabaseInfo B 
                JOIN metadata.DatabaseUse A ON A.DatabaseUseId = B.DatabaseUseId
            WHERE A.DatabaseUse = 'SOURCE'");
//and DatabaseDescription = '{0}'", DatabaseDescription);                

            List<DatabaseInfo> databases = conn.Query<DatabaseInfo>(sql).ToList();
            foreach (DatabaseInfo database in databases)
            {
                // Bring all database objects into memory
                DataModel dm = PopulateFromMetadataDatabase.PopulateDataModelFromMetadataDatabase(database, conn);

                DirectoryInfo stagingTables = Utilities.GetAndCleanOutputDir(UamDatabaseName, "Staging", "Tables");
                sql = string.Format(@"
                        select distinct DatabaseInfoId, SchemaName, SchemaName as StagingAreaSchema from [Metadata].[DatabaseObject] where DatabaseInfoId = {0}
                    ", database.DatabaseInfoId);
                database.Schemas = conn.Query<SchemaInfo>(sql).ToList();
                foreach (SchemaInfo schema in database.Schemas)
                {
                    DirectoryInfo stubTables = Utilities.GetAndCleanOutputDir(StubDatabaseName, schema.SchemaName, "Tables");


                foreach (DatabaseObject table in dm.Objects.Where(x => (x.DatabaseObjectType.Trim().ToUpper() == "U") &&
                            x.SchemaName == schema.SchemaName))
                {
                        CreateStubTableScript(database, table, stubTables);
                        switch (schema.SchemaName)
                        {
                            case "Dimension":
                            //case "DimensionMercury":
                            case "Portal":
                                CreateStagingTableScript(database, table, stagingTables, DatabaseDescription);
                                break;

                            default:
                                break; 
                        }
                    }
                }
            }
            
}
        private static void CreateStubTableScript(DatabaseInfo database, DatabaseObject table, DirectoryInfo dirTables)
        {
            StringBuilder sb = new StringBuilder("create table [");
            sb.Append(table.SchemaName);
            sb.Append("].[");
            sb.Append(table.DatabaseObjectName);
            sb.AppendLine("]");
            sb.AppendLine("(");

            foreach (DatabaseColumn column in table.Columns.Where(x => x.UseColumn))
            {
                sb.Append("\t[");
                sb.Append(column.DatabaseColumnName);
                sb.Append("] ");
                sb.Append(column.DataType);
                sb.Append(column.GetDataTypeModifiers(false, false));
                sb.AppendLine(",");
            }
            sb.AppendLine(") on [PRIMARY] with (data_compression = page);");
            string sqlPath = Path.Combine(dirTables.FullName, table.DatabaseObjectName + ".sql");
            //Console.WriteLine("Writing Table {0}", sqlPath);
            Console.Write("t");
            System.IO.File.WriteAllText(sqlPath, sb.ToString());
        }

        private static void CreateStagingTableScript(DatabaseInfo database, DatabaseObject table, DirectoryInfo dirTables, string prefix)
        {
            StringBuilder sb = new StringBuilder("create table [Staging].[");
            sb.Append(prefix);
            sb.Append("_");
            sb.Append(table.DatabaseObjectName);
            sb.AppendLine("]");
            sb.AppendLine("(");
            sb.AppendLine(TemplateComponents.AutogenWarning());

            // add LoadLogId and DatabaseInfoId columns
            sb.AppendLine("\t[LoadLogId] bigint not null,");
            
            foreach (DatabaseColumn column in table.Columns.Where(x => x.UseColumn))
            {
                sb.Append("\t[");
                sb.Append(column.DatabaseColumnName);
                sb.Append("] ");
                sb.Append(column.DataType);
                sb.Append(column.GetDataTypeModifiers(false, false));
                sb.AppendLine(",");
            }
            sb.AppendLine(") on [PRIMARY] with (data_compression = page);");
            string sqlPath = Path.Combine(dirTables.FullName, prefix + "_" + table.DatabaseObjectName + ".sql");
            //Console.WriteLine("Writing Table {0}", sqlPath);
            Console.Write("s");
            System.IO.File.WriteAllText(sqlPath, sb.ToString());
        }
    }
}
