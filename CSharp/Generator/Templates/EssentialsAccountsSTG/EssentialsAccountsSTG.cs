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
    public class EssentialsAccountsSTG
    {
        public const string DatabaseName = "EssentialsAccountsSTG";
        public static void CreateObjectsFromMetadata(SqlConnection conn, bool saveRenames)
        {
            //try
            //{
                Console.WriteLine();
                Console.WriteLine("Creating objects for {0}", DatabaseName);

                // ensure that all custom updates to the metadata have been done before creating the tables by calling [Metadata].[CustomUpdateToColumnMetadata]
                using (SqlCommand cmd = new SqlCommand("Metadata.CustomUpdateToColumnMetadata", conn))
                {
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.CommandTimeout = 600;
                    cmd.ExecuteNonQuery();
                }

                // get the list of databases to generate tables
                string sql = @"
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
                WHERE A.DatabaseUse = 'SOURCE' 
                ";

                DirectoryInfo dirDmTables = Utilities.GetAndCleanOutputDir("EssentialsAccountsDB", "eaDataMart", "Tables");

                List<DatabaseInfo> databases = conn.Query<DatabaseInfo>(sql).ToList();
                foreach (DatabaseInfo database in databases)
                {
                    // Bring all database objects into memory
                    DataModel dm = PopulateFromMetadataDatabase.PopulateDataModelFromMetadataDatabase(database, conn);

                    sql = string.Format(@"
                            SELECT DatabaseInfoId, SchemaName, StagingAreaSchema
                            FROM Metadata.SchemaInfo A 
                            WHERE A.DatabaseInfoId = {0}
                        ", database.DatabaseInfoId);
                    database.Schemas = conn.Query<SchemaInfo>(sql).ToList();
                    foreach (SchemaInfo schema in database.Schemas)
                    {
                        DirectoryInfo dirTables = Utilities.GetAndCleanOutputDir(DatabaseName, schema.StagingAreaSchema, "Tables");

                        DirectoryInfo dirViews = Utilities.GetAndCleanOutputDir(DatabaseName, schema.StagingAreaSchema, "Views");

                        foreach (DatabaseObject table in dm.Objects.Where(x => x.DatabaseObjectType.Trim().ToUpper() == "U" && 
                                    x.UseObject && 
                                    x.SchemaName == schema.SchemaName))
                        {
                            table.StagingAreaSchema = schema.StagingAreaSchema; // ensure this is populated
                            CreateStgTableScript(database, table, dirTables);

                            // CreateDataTranslationViews(database, table, dirViews);

                            //if (dm.Database.DatabaseGroup == "MDFL")
                            //    CreateDmTableScript(database, table, dirDmTables, "dm");
                        }
                    }
                }
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Error in EssentialsAccountsSTG.CreateObjectsFromMetadata {0}", ex.Message);
            //}
}
        private static void CreateStgTableScript(DatabaseInfo database, DatabaseObject table, DirectoryInfo dirTables)
        {
            StringBuilder sb = new StringBuilder("create table [");
            sb.Append(table.StagingAreaSchema);
            sb.Append("].[");
            sb.Append(table.DatabaseObjectName);
            sb.AppendLine("]");
            sb.AppendLine("(");
            sb.AppendLine(TemplateComponents.AutogenWarning());

            // add LoadLogId and DatabaseInfoId columns
            sb.AppendLine("\t[LoadLogId] bigint not null,");
            //sb.AppendLine("\t[DatabaseInfoId] int not null,");
            //sb.AppendLine("\t[DatabaseGroupId] int not null,");

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
            Console.Write(".");
            System.IO.File.WriteAllText(sqlPath, sb.ToString());
        }

        private static void CreateDmTableScript(DatabaseInfo database, DatabaseObject table, DirectoryInfo dirDmTables, string TableSchema)
        {
            StringBuilder sb = new StringBuilder("create table [");
            sb.Append(TableSchema);
            sb.Append("].[");
            sb.Append(table.TargetObjectName);
            sb.AppendLine("]");
            sb.AppendLine("(");

            // add LoadLogId and DatabaseInfoId columns
            sb.AppendLine("\t[LoadLogId] bigint not null,");
            sb.AppendLine("\t[DatabaseInfoId] int not null,");
            sb.AppendLine("\t[DatabaseGroupId] int not null,");

            foreach (DatabaseColumn column in table.Columns.Where(x => x.UseColumn).OrderBy(x => x.DatabaseColumnName))
            {
                sb.Append("\t[");
                sb.Append(column.TargetColumnName);
                sb.Append("] ");
                sb.Append(column.TargetDataType);
                sb.Append(column.GetDataTypeModifiers(false, true));
                sb.AppendLine(",");
            }
            sb.AppendLine(") on [PRIMARY] with (data_compression = page);");
            string sqlPath = Path.Combine(dirDmTables.FullName, table.StagingAreaSchema + "_" + table.TargetObjectName + ".sql");
            //Console.WriteLine("Writing Table {0}", sqlPath);
            Console.Write("t");
            System.IO.File.WriteAllText(sqlPath, sb.ToString());
        }

        private static void CreateDataTranslationViews(DatabaseInfo database, DatabaseObject table, DirectoryInfo dirViews)
        {
            // clean up table names here
            NamingCleanup.CleanDatabaseObjectName(table);

            StringBuilder sb = new StringBuilder("create view [");
            sb.Append(table.StagingAreaSchema);
            sb.Append("].[dt_");
            sb.Append(table.DatabaseObjectName);
            sb.AppendLine("]");
            sb.AppendLine("as");
            sb.AppendLine(TemplateComponents.AutogenWarning());
            sb.AppendLine("select");

            // add LoadLogId column
            sb.AppendLine("\t-- system fields");
            sb.AppendLine("\t[LoadLogId],");
            sb.AppendLine("\t[DatabaseInfoId],");
            sb.AppendLine("\t[DatabaseGroupId],");
            sb.AppendLine();
            sb.AppendLine("\t-- staging table fields");
            bool addComma = false;

            foreach (DatabaseColumn column in table.Columns)
            {
                if (table.StagingAreaSchema.StartsWith("stgQu"))
                {
                    // clean up Quantum column names here
                    NamingCleanup.CleanColumnNameQuantum(column);
                }
                
            }

            foreach (DatabaseColumn column in table.Columns.Where(x => x.UseColumn).OrderBy(x => x.DatabaseColumnName))
            {
                if (addComma)
                    sb.AppendLine(",");
                if (column.DataType == column.TargetDataType && column.Length == column.TargetLength && column.Precision == column.TargetPrecision && column.Scale == column.TargetScale)
                {
                    // no cast to apply
                    sb.AppendLine();
                    sb.Append("\t-- no cast from ");
                    sb.Append(column.DataType.ToLower());
                    sb.Append(column.GetDataTypeModifiers(false, false));
                    AddMaxLength(sb, column);
                    sb.AppendLine();

                    switch (column.TargetDataType.ToLower())
                    {
                        case "nvarchar":
                        case "nchar":
                        case "varchar":
                        case "char":
                            sb.Append("\t");
                            TrimStringAsRequired(sb, column);
                            break;

                        default:
                            sb.Append("\t[");
                            sb.Append(column.DatabaseColumnName);
                            sb.Append("]");
                            break;
                    }
                }
                else
                {
                    // we have a specific cast to apply
                    sb.AppendLine();
                    sb.Append("\t-- was ");
                    sb.Append(column.DataType.ToLower());
                    sb.Append(column.GetDataTypeModifiers(false, false));
                    AddMaxLength(sb, column);
                    sb.AppendLine();

                    sb.Append("\t");
                    switch (column.TargetDataType.ToLower())
                    {
                        case "nvarchar":
                        case "varchar":
                        case "nchar":
                        case "char":
                            if (column.DataType == "text")
                            {
                                // source data type is text, so first do a cast e.g.
                                // cast(rtrim(ltrim(left(cast([notes_text] as [varchar](500)) ,500)))   as [varchar](500)) as [NotesText]
                                sb.Append("cast(rtrim(ltrim(left(cast([");
                                sb.Append(column.DatabaseColumnName);
                                sb.Append("] as [");
                                sb.Append(column.TargetDataType);
                                sb.Append("](");
                                sb.Append(column.TargetLength);
                                sb.Append(")),");
                                sb.Append(column.TargetLength);
                                sb.Append("))) as [");
                                sb.Append(column.TargetDataType);
                                sb.Append("](");
                                sb.Append(column.TargetLength);
                                sb.Append("))");

                            }
                            else
                                TrimStringAsRequired(sb, column);
                            break;



                        case "bit":
                            if (column.DataType.ToLower() == "tinyint")
                            {
                                if (column.MaxValueInTable <= 1)
                                {
                                    ReplaceTinyIntWithBit(sb, column);
                                }
                                else
                                {
                                    // leave as tinyint (no cast)
                                    sb.Append("[");
                                    sb.Append(column.DatabaseColumnName);
                                    sb.Append("]");
                                }
                            }
                            else
                            {
                                ReplaceTinyIntWithBit(sb, column);
                            }
                            break;

                        case "decimal":
                        case "numeric":
                            sb.Append("cast([");
                            sb.Append(column.DatabaseColumnName);
                            sb.Append("] as ");
                            sb.Append(column.TargetDataType);
                            sb.Append("(");
                            sb.Append(column.Precision);
                            sb.Append(",");
                            sb.Append(column.Scale);
                            sb.Append("))");
                            break;

                        default:
                            sb.Append("cast([");
                            sb.Append(column.DatabaseColumnName);
                            sb.Append("] as ");
                            sb.Append(column.TargetDataType);
                            sb.Append(")");
                            break;
                    }
                }

                sb.Append(" as [");
                sb.Append(column.TargetColumnName);
                sb.Append("]");

                addComma = true;
            }

            sb.AppendLine("");
            sb.Append("from ");
            sb.Append(table.StagingAreaSchema);
            sb.Append(".[");
            sb.Append(table.DatabaseObjectName);
            sb.Append("]");
            sb.AppendLine(";");
            string sqlPath = Path.Combine(dirViews.FullName, table.DatabaseObjectName + ".sql");
            //Console.WriteLine("Writing View {0}", sqlPath);
            Console.Write("v");
            System.IO.File.WriteAllText(sqlPath, sb.ToString());
        }


        private static void TrimStringAsRequired(StringBuilder sb, DatabaseColumn column)
        {
            if (column.DataType != column.TargetDataType)
                sb.Append("cast(");

            sb.Append("case when ltrim(rtrim(");
            sb.Append(column.DatabaseColumnName);
            sb.Append(")) = '' then null else ");
            if (column.TrimWhitespace)
                sb.Append("rtrim(ltrim(");
           

            if (column.TargetLength > 0)
            {
                sb.Append("left([");
                sb.Append(column.DatabaseColumnName);
                sb.Append("], ");
                sb.Append(column.TargetLength);
                sb.Append(")");
            }
            else
            {
                sb.Append("[");
                sb.Append(column.DatabaseColumnName);
                sb.Append("]");
            }
           
            if (column.TrimWhitespace)
                sb.Append("))");
            sb.Append(" end ");
           
            if (column.DataType != column.TargetDataType)
            {
                sb.Append(" as [");
                sb.Append(column.TargetDataType);
                sb.Append("](");
                sb.Append(column.TargetLength);
                sb.Append("))");
            }

            
        }

        private static void AddMaxLength(StringBuilder sb, DatabaseColumn column)
        {
            switch (column.TargetDataType.ToLower())
            {
                case "nvarchar":
                case "nchar":
                case "varchar":
                case "char":
                    sb.Append(" MaxLengthInTable = ");
                    sb.Append(column.MaxLengthInTable);
                    break;
            }
        }

        

        private static void ReplaceTinyIntWithBit(StringBuilder sb, DatabaseColumn column)
        {
            sb.Append("cast([");
            sb.Append(column.DatabaseColumnName);
            sb.Append("] as ");
            sb.Append(column.TargetDataType);
            sb.Append(")");
        }

    }
}
