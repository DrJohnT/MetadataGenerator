namespace Generator
{
    public interface IDatabaseColumn
    {
        int DatabaseColumnId { get; set; }
        string DatabaseColumnName { get; set; }
        int DatabaseInfoId { get; set; }
        string DatabaseName { get; set; }
        int DatabaseObjectId { get; set; }
        string DatabaseObjectName { get; set; }
        string DataType { get; set; }
        bool IsIdentity { get; set; }
        bool IsNullable { get; set; }
        bool IsPrimaryKey { get; set; }
        int Length { get; set; }
        int? MaxLengthInTable { get; set; }
        int? MaxValueInTable { get; set; }
        int? NumberOfBlanks { get; set; }
        int? NumberOfNulls { get; set; }
        int Precision { get; set; }
        int Scale { get; set; }
        string SchemaName { get; set; }
        bool SpecificLookupsDone { get; set; }
        string TargetDataType { get; set; }
        int TargetLength { get; set; }
        int TargetPrecision { get; set; }
        int TargetScale { get; set; }
        bool TrimWhitespace { get; set; }
        bool UseColumn { get; set; }

        string GetDataTypeModifiers(bool makeAllNullable, bool useTargetLength);
        string GetUnknownValueForView(string unknownStringValue, string unknownShortStringValue, int unknownInt, string unknownDateTime, bool useNullIfPossible);
    }
}