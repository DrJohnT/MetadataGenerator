using System.Collections.Generic;

namespace Generator
{
    public interface IDatabaseObject
    {
        int ColumnCount { get; set; }
        int DatabaseInfoId { get; set; }
        string DatabaseName { get; set; }
        int DatabaseObjectId { get; set; }
        string DatabaseObjectName { get; set; }
        string DatabaseObjectType { get; set; }
        string LoadingPattern { get; set; }
        long NumberOfRows { get; set; }
        string SchemaName { get; set; }
        string StagingAreaSchema { get; set; }
    }
}