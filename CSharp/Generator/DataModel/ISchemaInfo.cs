namespace Generator
{
    public interface ISchemaInfo
    {
        int DatabaseInfoId { get; set; }
        string SchemaName { get; set; }
        string StagingAreaSchema { get; set; }
    }
}