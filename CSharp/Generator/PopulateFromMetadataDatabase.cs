using System.Linq;
using System.Data;
using System.Data.SqlClient;
using Dapper;

namespace Generator
{
    public class PopulateFromMetadataDatabase
    {
        /// <summary>
        /// Populate DataModel from the Metadata database
        /// </summary>
        /// <param name="database">Identifies the source database for which we want to get the metadata</param>
        /// <param name="metadataConn">Connection to the metadata database</param>
        /// <returns></returns>
        public static DataModel PopulateDataModelFromMetadataDatabase(DatabaseInfo database, SqlConnection metadataConn)
        {
            DataModel dataModel = new DataModel();
            dataModel.Database = database;

            var dbParameters = new DynamicParameters();
            dbParameters.Add("@DatabaseInfoId", database.DatabaseInfoId);

            dataModel.Objects = metadataConn.Query<DatabaseObject>("Metadata.GetDatabaseObjects",
                   dbParameters,
                   commandType: CommandType.StoredProcedure).ToList();

            foreach (DatabaseObject table in dataModel.Objects)
            {
                var columnParameters = new DynamicParameters();
                columnParameters.Add("@DatabaseInfoId", database.DatabaseInfoId);
                columnParameters.Add("@SchemaName", table.SchemaName);
                columnParameters.Add("@TableName", table.DatabaseObjectName);

                table.Columns = metadataConn.Query<DatabaseColumn>("Metadata.GetDatabaseObjectColumns",
                    columnParameters,
                    commandType: CommandType.StoredProcedure).ToList();
            }

            return dataModel;
        }
    }
}
