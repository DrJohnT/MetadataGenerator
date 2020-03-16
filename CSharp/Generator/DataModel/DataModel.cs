using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;
using System.Data.SqlClient;
using Dapper;
using System;

namespace Generator
{
    public class DataModel
    {
        public DatabaseInfo Database;
        public List<DatabaseObject> Objects;

        public DataModel()
        {
            Objects = new List<DatabaseObject>();
        }

        public void SaveToMetadataDatabase(SqlConnection conn)
        {
            Console.WriteLine("SaveToMetadataDatabase for database {0}", this.Database.DatabaseName);

            // useful guide to Dapper and CRUD
            // https://stackoverflow.com/questions/5957774/performing-inserts-and-updates-with-dapper

            // delete everything for this database from DatabaseObject and DatabaseColumn
            try
            {
                string sql = string.Format("exec Metadata.DeleteDatabaseObjects @DatabaseInfoId = {0}", this.Database.DatabaseInfoId);
                conn.Execute(sql);

                sql = @"
                    INSERT INTO Metadata.DatabaseObject 
                            ( DatabaseInfoId,  DatabaseObjectId,  DatabaseObjectType,  DatabaseObjectName,  SchemaName,  ColumnCount,  NumberOfRows) 
                    VALUES 
                            (@DatabaseInfoId, @DatabaseObjectId, @DatabaseObjectType, @DatabaseObjectName, @SchemaName, @ColumnCount, @NumberOfRows)
                    ";

                conn.Execute(sql, this.Objects);

                sql = @"
                    INSERT INTO Metadata.DatabaseColumn 
                            (DatabaseInfoId, SchemaName, DatabaseObjectId, DatabaseObjectName, DatabaseColumnId, DatabaseColumnName, IsIdentity, IsNullable, DataType, 
                            Length, [Precision], Scale, MaxLengthInTable, NumberOfNulls, NumberOfBlanks, MaxValueInTable, SpecificLookupsDone, IsPrimaryKey) 
                    VALUES 
                            (@DatabaseInfoId, @SchemaName, @DatabaseObjectId, @DatabaseObjectName, @DatabaseColumnId, @DatabaseColumnName, @IsIdentity, @IsNullable, @DataType, 
                            @Length, @Precision, @Scale, @MaxLengthInTable, @NumberOfNulls, @NumberOfBlanks, @MaxValueInTable, @SpecificLookupsDone, @IsPrimaryKey)
                    ";

                conn.Execute(sql, GetColumns());

                sql = @"
                    UPDATE 
                        Metadata.DatabaseInfo
                    SET 
                        ImportMetadata = 0
                    WHERE 
                        DatabaseInfoId = @DatabaseInfoId                
                    ";

                conn.Execute(sql, this.Database);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to store data. Error: {0} ", ex.Message);

            }
        }

        public void SaveColumnRenamesToMetadataDatabase(SqlConnection conn)
        {
            Console.WriteLine();
            Console.WriteLine("SaveColumnRenamesToMetadataDatabase for database {0}", this.Database.DatabaseName);

            // delete everything for this database from DatabaseObject and DatabaseColumn
            string sql = string.Format("DELETE FROM Metadata.LogColumnRename WHERE DatabaseInfoId = {0}", this.Database.DatabaseInfoId);
            conn.Execute(sql);

            sql = @"
                INSERT INTO Metadata.LogColumnRename 
                        ( DatabaseInfoId,  SchemaName,  DatabaseObjectName,  DatabaseColumnName,  TargetColumnName) 
                VALUES 
                        (@DatabaseInfoId, @SchemaName, @DatabaseObjectName, @DatabaseColumnName, @TargetColumnName)
                ";

            List<DatabaseColumn> columns = new List<DatabaseColumn>();
            foreach (DatabaseObject table in this.Objects.Where(x => x.DatabaseObjectType.Trim().ToUpper() == "U"))
            {
                foreach (DatabaseColumn col in table.Columns)
                {
                    columns.Add(col);
                }
            }

            conn.Execute(sql, columns);
        }

        public void SaveTableRenamesToMetadataDatabase(SqlConnection conn)
        {
            Console.WriteLine("SaveTableRenamesToMetadataDatabase for database {0}", this.Database.DatabaseName);

            // delete everything for this database from DatabaseObject and DatabaseColumn
            string sql = string.Format("DELETE FROM Metadata.LogTableRename WHERE DatabaseInfoId = {0}", this.Database.DatabaseInfoId);
            conn.Execute(sql);

            sql = @"
                INSERT INTO Metadata.LogTableRename 
                        ( DatabaseInfoId,  SchemaName,  DatabaseObjectName,  TargetObjectName) 
                VALUES 
                        (@DatabaseInfoId, @SchemaName, @DatabaseObjectName, @TargetObjectName)
                ";
            conn.Execute(sql, GetTables());
        }

        private List<DatabaseObject> GetTables()
        {
            List<DatabaseObject> tables = new List<DatabaseObject>();
            foreach (DatabaseObject table in this.Objects.Where(x => x.DatabaseObjectType.Trim().ToUpper() == "U"))
            {
                tables.Add(table);
            }

            return tables;
        }

        private List<DatabaseColumn> GetColumns()
        {
            List<DatabaseColumn> columns = new List<DatabaseColumn>();
            foreach (DatabaseObject dbObject in this.Objects)
            {
                foreach (DatabaseColumn column in dbObject.Columns)
                {
                    columns.Add(column);
                }
            }

            return columns;
        }

        /// <summary>
        /// Updates the additional metadata for an existing model
        /// </summary>
        /// <param name="metadataDatabaseConn"></param>
        /// <param name="table"></param>
        public void UpdateColumnMetadataForTable(SqlConnection metadataDatabaseConn, DatabaseObject table)
        {
            // useful guide to Dapper and CRUD
            // https://stackoverflow.com/questions/5957774/performing-inserts-and-updates-with-dapper

            Console.WriteLine("UpdateColumnMetadataForTable for table {0} in database {1}", table.DatabaseObjectName, table.DatabaseName);

            string updateSql = @"
            UPDATE 
                Metadata.DatabaseColumn 
            SET 
                MaxLengthInTable        = @MaxLengthInTable, 
                NumberOfNulls           = @NumberOfNulls, 
                NumberOfBlanks          = @NumberOfBlanks, 
                MaxValueInTable         = @MaxValueInTable, 
                SpecificLookupsDone     = @SpecificLookupsDone
            WHERE   
                    DatabaseInfoId      = @DatabaseInfoId 
                AND DatabaseObjectName  = @DatabaseObjectName 
                AND SchemaName          = @SchemaName 
                AND DatabaseColumnName  = @DatabaseColumnName 
                AND DatabaseColumnId    = @DatabaseColumnId
            ";

            List<DatabaseColumn> columns = new List<DatabaseColumn>();
            foreach (DatabaseColumn col in table.Columns)
            {
                columns.Add(col);
            }
            metadataDatabaseConn.Execute(updateSql, columns);
        }

        public void SaveDataModelToXml(string xmlFileName)
        {
            var serializer = new XmlSerializer(typeof(DataModel));

            using (FileStream stream = File.Create(xmlFileName))
            {
                serializer.Serialize(stream, this);
            }
        }
    }
}
