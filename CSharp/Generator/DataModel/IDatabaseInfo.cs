namespace Generator
{
    public interface IDatabaseInfo
    {
        string DatabaseGroup { get; set; }
        int DatabaseInfoId { get; set; }
        string DatabaseDescription { get; set; }
        bool ImportMetadata { get; set; }
        string DatabaseName { get; set; }
        string ServerName { get; set; }
        string pkPrefix { get; set; }
    }
}