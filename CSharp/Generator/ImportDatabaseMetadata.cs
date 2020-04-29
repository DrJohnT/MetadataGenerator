using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using Dapper;

namespace Generator
{
    public class ImportDatabaseMetadata
    {
        public static void ImportMetadataFromSourceDatabases()
        {
            try
            {
                SqlConnection conn = new SqlConnection(Generator.Properties.Settings.Default.MetadataDatabase);
                string sql = @"
                SELECT DatabaseInfoId, DatabaseName, DatabaseDescription, ServerName 
                FROM Metadata.DatabaseInfo A 
                JOIN Metadata.DatabaseUse B on A.DatabaseUseId = B.DatabaseUseId
                WHERE B.DatabaseUse = 'SOURCE' AND ImportMetadata = 1
                ";

                List<DatabaseInfo> databases = conn.Query<DatabaseInfo>(sql).ToList();
                foreach (DatabaseInfo database in databases)
                {
                    sql = string.Format(@"
                        SELECT DatabaseInfoId, SchemaName, StagingAreaSchema
                        FROM Metadata.SchemaInfo A 
                        WHERE A.DatabaseInfoId = {0}
                    ", database.DatabaseInfoId);
                    database.Schemas = conn.Query<SchemaInfo>(sql).ToList();

                    Console.WriteLine("Importing metadata for SOURCE database {0}", database.DatabaseDescription);
                    DataModel dm = PopulateDataModelFromDeployedDatabase(database);
                    // now save to SQL tables
                    dm.SaveToMetadataDatabase(conn);
                    // save to XML file
                    //dm.SaveToXml(@"E:\Downloads\" + database.DatabaseName + ".xml");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in ImportMetadataFromTargetDatabases {0}", ex.Message);
            }
        }

        public static void ImportMetadataFromTargetDatabases()
        {
            try
            { 
                using (SqlConnection conn = new SqlConnection(Generator.Properties.Settings.Default.MetadataDatabase))
                {
                    string sql = @"
                    SELECT DatabaseInfoId, DatabaseName, DatabaseDescription, ServerName 
                    FROM Metadata.DatabaseInfo A 
                    JOIN Metadata.DatabaseUse B on A.DatabaseUseId = B.DatabaseUseId 
                    WHERE B.DatabaseUse = 'TARGET' 
                    ";
                    // note that and A.DatabaseName <> 'IrisReporting' was added because we don't care about IrisReporting - but needs to be in metadata

                    List<DatabaseInfo> databases = conn.Query<DatabaseInfo>(sql).ToList();
                    foreach (DatabaseInfo database in databases)
                    {
                        Console.WriteLine("Importing metadata for TARGET database {0} on server {1}", database.DatabaseDescription, database.ServerName);
                        DataModel dm = PopulateDataModelFromDeployedDatabase(database);
                        // now save to SQL tables
                        dm.SaveToMetadataDatabase(conn);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in ImportMetadataFromTargetDatabases {0}", ex.Message);
            }
        }

        public static DataModel PopulateDataModelFromDeployedDatabase(DatabaseInfo database)
        {
            DataModel dataModel = new DataModel();
            try
            { 
                dataModel.Database = database;

                using (SqlConnection connection = new SqlConnection(Utilities.GetConnectionString(database)))
                {
                    Console.WriteLine("Database {0} on server {1}", database.DatabaseName, database.ServerName);

                    if (dataModel.Database.DatabaseDescription != "MDS")
                    {
                        // read System Dynamic Management Views to obtain metadata about the source database tables
                        string sql = string.Format(@"
                        with rowCounts as (
	                        select 
		                        object_id, 
		                        sum(rows) as NumberOfRows 
	                        from sys.partitions group by object_id
                        )
                        SELECT DISTINCT
                            {0} as DatabaseInfoId,    
                            '{1}' as DatabaseName,
                            t.object_id AS DatabaseObjectId,
                            s.[name] AS SchemaName,
                            t.[name] AS DatabaseObjectName,
                            'U' as DatabaseObjectType,
                            C.ColumnCount,
	                        A.NumberOfRows
                        FROM sys.tables t
                            JOIN sys.schemas s ON s.schema_id = t.schema_id
                            JOIN (
                                    SELECT object_id, COUNT(*) AS ColumnCount
                                    FROM sys.columns
                                    GROUP BY object_id
                                    ) AS C ON t.object_id = C.object_id
	                        JOIN rowCounts A
		                        on t.object_id = A.object_id                        
                        WHERE (lower(rtrim(t.[name])) NOT LIKE '''%') -- remove tables which have a single quote at the begining 
                            and (lower(rtrim(t.[name])) NOT LIKE '%''') -- remove tables which have a single quote at the end
                        ORDER BY
	                        s.[name],
                            t.[name];
                    ", dataModel.Database.DatabaseInfoId, dataModel.Database.DatabaseName);

                        dataModel.Objects = connection.Query<DatabaseObject>(sql).ToList();
                    }

                    // add views
                    if (dataModel.Database.DatabaseDescription == "RA_COE")
                    {
                        string sql = string.Format(@"
                            SELECT 
                                {0} AS DatabaseInfoId,
                                '{1}' AS DatabaseName,
                                t.object_id AS DatabaseObjectId,
                                s.[name] AS SchemaName,
                                t.[name] AS DatabaseObjectName,
                                'V' AS DatabaseObjectType,
                                C.ColumnCount
                            FROM sys.views t
                                JOIN sys.schemas s ON s.schema_id = t.schema_id
                                JOIN (
                                        SELECT object_id, COUNT(*) AS ColumnCount
                                        FROM sys.columns
                                        GROUP BY object_id
                                     ) AS C ON t.object_id = C.object_id
                            ORDER BY
	                            s.[name],
                                t.[name]
                        ", dataModel.Database.DatabaseInfoId, dataModel.Database.DatabaseName);
                        
                        if (dataModel.Objects.Count == 0)
                        {
                            dataModel.Objects = connection.Query<DatabaseObject>(sql).ToList();
                        } else
                        {
                            List<DatabaseObject> views = connection.Query<DatabaseObject>(sql).ToList();
                            foreach (DatabaseObject view in views)
                            {
                                dataModel.Objects.Add(view);
                            }
                        }
                    }

                        if (dataModel.Database.DatabaseDescription == "MDS")
                        {
                            string sql = string.Format(@"
                                select
                                            {0} AS DatabaseInfoId,
                                            '{1}' AS DatabaseName,
			                                t.object_id as DatabaseObjectId,
			                                s.[name]		as SchemaName,
			                                t.[name]		as DatabaseObjectName,
			                                'V'			as DatabaseObjectType,
			                                C.ColumnCount
                                from		sys.views	t
	                                join	sys.schemas s
	                                  on	s.schema_id = t.schema_id
	                                join	(
				                                select
							                                object_id,
							                                COUNT(*) as ColumnCount
				                                from		sys.columns
				                                group by	object_id
			                                )			as C
	                                  on t.object_id = C.object_id
                                where		s.[name] = N'mdm' and t.[name] not like 'viw_SYSTEM%'
                                order by
			                                s.[name],
			                                t.[name];
                        ", dataModel.Database.DatabaseInfoId, dataModel.Database.DatabaseName);
                        if (dataModel.Objects.Count == 0)
                        {
                            dataModel.Objects = connection.Query<DatabaseObject>(sql).ToList();
                        }
                        else
                        {
                            List<DatabaseObject> views = connection.Query<DatabaseObject>(sql).ToList();
                            foreach (DatabaseObject view in views)
                            {
                                dataModel.Objects.Add(view);
                            }
                        }
                    }

                    int tableCount = 1;
                    foreach (DatabaseObject table in dataModel.Objects) //.Where(x => x.SchemaName != "BIML"))
                    {
                        Console.WriteLine("\nProcessing object {0} of {1} '{2}.{3}'", tableCount, dataModel.Objects.Count, table.SchemaName, table.DatabaseObjectName);

                        // add StagingAreaSchema
                        //try
                        //{
                        //    table.StagingAreaSchema = database.Schemas.Find(c => c.SchemaName == table.SchemaName).StagingAreaSchema;
                        //}
                        //catch { }

                        

                        // read System Dynamic Management Views to obtain metadata about the source columns
                        string sql = @"
                        WITH pks AS (
	                        SELECT
		                        c.object_id,
		                        c.column_id,
		                        ind.is_primary_key 
	                        FROM sys.columns c
		                        JOIN sys.indexes ind ON ind.object_id = c.object_id
		                        JOIN sys.index_columns indcol ON indcol.object_id = c.object_id
			                        AND indcol.index_id = ind.index_id
			                        AND c.column_id = indcol.column_id
	                        WHERE ind.is_primary_key = 1
                        )
                        SELECT
                             {0} AS DatabaseInfoId,    
                            '{1}' AS SchemaName,
                            c.object_id AS DatabaseObjectId,
                            '{2}' AS DatabaseObjectName,
	                        c.column_id AS DatabaseColumnId,
	                        c.[name] AS DatabaseColumnName,
	                        columnproperty(c.object_id, c.[name], 'charmaxlen') AS [Length],
                            t.[name] AS DataType,	
                            c.is_identity AS IsIdentity,
	                        c.is_nullable AS IsNullable,
                            c.precision AS [Precision],
                            c.scale AS Scale,
	                        CAST(CASE WHEN pks.is_primary_key = 1 THEN 1 ELSE 0 END AS BIT) AS IsPrimaryKey
                        FROM sys.columns c 
	                        JOIN sys.types t ON c.user_type_id = t.user_type_id
	                        LEFT JOIN pks ON c.object_id = pks.object_id
		                        AND c.column_id = pks.column_id
                        WHERE c.object_id = {3}
                        ORDER BY c.column_id;
                        ";

                        string columnSql = string.Format(sql, dataModel.Database.DatabaseInfoId, table.SchemaName, table.DatabaseObjectName, table.DatabaseObjectId);

                        table.Columns = connection.Query<DatabaseColumn>(columnSql).ToList();

                        tableCount++;
                    }
                    

                    /*
                    sql = string.Format(@"
                    SELECT 
                        {0} AS DatabaseInfoId,
                        '{1}' AS DatabaseName,
                        t.object_id AS DatabaseObjectId,
                        s.[name] AS SchemaName,
                        t.[name] AS DatabaseObjectName,
                        'P' AS DatabaseObjectType,
                        -1 as ColumnCount,
                        -1 as NumberOfRows
                    FROM sys.procedures t
                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE (lower(rtrim(t.[name])) NOT LIKE 'tmp%' and lower(rtrim(t.[name])) NOT LIKE 'temp%')
                    ORDER BY
	                    s.[name],
                        t.[name]
                    ", dataModel.Database.DatabaseInfoId, dataModel.Database.DatabaseName);

                    
                    List<DatabaseObject> sps = connection.Query<DatabaseObject>(sql).ToList();
                    foreach (DatabaseObject proc in sps)
                    {
                        Console.WriteLine("\nProcessing Stored Proc {0} of {1} '{2}'", tableCount, dataModel.Objects.Count, proc.DatabaseObjectName);

                        // read System Dynamic Management Views to obtain the parameters for each stored proc (columns)
                        sql = @"
                         SELECT
                            {0} AS DatabaseInfoId,    
                            '{1}' AS SchemaName,
                            c.object_id AS DatabaseObjectId,
                            '{2}' AS DatabaseObjectName,
	                        c.[name] AS DatabaseColumnName,
	                        case 
								when c.max_length = -1 then -1
								when left(t.[name],1) = N'n' then c.max_length / 2 
								else c.max_length 
							end AS [Length],
                            t.[name] AS DataType,	
                            c.precision AS [Precision],
                            c.scale AS Scale
                        FROM sys.parameters c 
	                        JOIN sys.types t ON c.user_type_id = t.user_type_id
                        WHERE c.object_id = {3}
                        ";

                        string columnSql = string.Format(sql, dataModel.Database.DatabaseInfoId, proc.SchemaName, proc.DatabaseObjectName, proc.DatabaseObjectId);

                        proc.Columns = connection.Query<DatabaseColumn>(columnSql).ToList();
                        proc.ColumnCount = proc.Columns.Count;
                        dataModel.Objects.Add(proc);
                    }
                    */
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in PopulateDataModelFromDeployedDatabase {0}", ex.Message);
            }
            return dataModel;
        }

        public static void UpdateMetadataWithSpecificColumnLookups()
        {
            try
            { 
                SqlConnection conn = new SqlConnection(Generator.Properties.Settings.Default.MetadataDatabase);
                string sql = @"SELECT DatabaseInfoId, DatabaseName, DatabaseDescription, ServerName
                FROM Metadata.DatabaseInfo A
                JOIN Metadata.DatabaseUse B on A.DatabaseUseId = B.DatabaseUseId
                WHERE B.DatabaseUse = 'SOURCE'";

                List<DatabaseInfo> databases = conn.Query<DatabaseInfo>(sql).ToList();
                foreach (DatabaseInfo database in databases)
                {
                    Console.WriteLine("Updating metadata for {0}", database.DatabaseDescription);
                    DataModel dm = PopulateFromMetadataDatabase.PopulateDataModelFromMetadataDatabase(database, conn);
                    using (SqlConnection connection = new SqlConnection(Utilities.GetConnectionString(database)))
                    {
                        foreach (DatabaseObject table in dm.Objects.Where(x => x.DatabaseObjectType.Trim().ToUpper() == "U"))
                        {
                            bool updateTable = false;
                            foreach (DatabaseColumn column in table.Columns.Where(c => c.SpecificLookupsDone == false))
                            {
                                updateTable = true;
                                break;
                            }
                            if (updateTable)
                            {
                                Console.WriteLine("\nTable '{0}'", table.DatabaseObjectName);
                                foreach (DatabaseColumn column in table.Columns.Where(c => c.SpecificLookupsDone == false))
                                {
                                    PerformSpecificLookups(connection, table, column);
                                }
                                dm.UpdateColumnMetadataForTable(conn, table);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in UpdateMetadataWithSpecificColumnLookups {0}", ex.Message);
            }
        }

        public static void PerformSpecificLookups(SqlConnection connection, DatabaseObject table, DatabaseColumn column)
        {
            Console.Write("\n\tProcessing '{0}' column {1}", column.DataType.ToUpper(), column.DatabaseColumnName);

            // set defaults
            column.MaxLengthInTable = null;
            column.MaxValueInTable = null;
            column.NumberOfBlanks = null;
            column.NumberOfNulls = null;

            if (table.NumberOfRows > 0 && table.NumberOfRows < 10000000)  // 10 million rows
            {
                switch (column.DataType.ToLower())
                {
                    case "nvarchar":
                    case "nchar":
                    case "varchar":
                    case "char":
                        if (column.Length > 3 && column.Length != -1) // -1 = varchar(max) or nvarchar(max) 
                        {
                            Console.Write("\tMaxLengthInTable");
                            column.MaxLengthInTable = Utilities.ExecuteIntQuery(connection, string.Format("select max(len([{0}])) as MaxLengthInTable from [{1}].[{2}] with(nolock)", column.DatabaseColumnName, table.SchemaName, table.DatabaseObjectName));
                        }
                        Console.Write("\tNumberOfBlanks");
                        column.NumberOfBlanks = Utilities.ExecuteIntQuery(connection, string.Format("select count(*) as NumberOfBlanks from [{1}].[{2}] with(nolock) where rtrim(ltrim([{0}])) = ''", column.DatabaseColumnName, table.SchemaName, table.DatabaseObjectName));
                        break;

                    case "bigint":
                    case "int":
                    case "tinyint":
                        Console.Write("\tMaxValueInTable");
                        column.MaxValueInTable = Utilities.ExecuteIntQuery(connection, string.Format("select max({0}) as MaxValueInTable from [{1}].[{2}] with(nolock)", column.DatabaseColumnName, table.SchemaName, table.DatabaseObjectName));
                        break;
                }

                if (column.IsNullable)
                {
                    Console.WriteLine("\tNumberOfNulls");
                    column.NumberOfNulls = Utilities.ExecuteIntQuery(connection, string.Format("select count(*) as NumberOfNulls from [{1}].[{2}] with(nolock) where [{0}] is null", column.DatabaseColumnName, table.SchemaName, table.DatabaseObjectName));
                }
                else
                {
                    column.NumberOfNulls = 0;
                }
            }

            column.SpecificLookupsDone = true;
        }
    }
}
