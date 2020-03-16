CREATE PROCEDURE Metadata.GetDatabaseObjectColumns
	@DatabaseInfoId int,
	@SchemaName sysname,
	@TableName sysname
AS
-- Called by C# in PopulateDataModelFromMetadataDatabase
select distinct
	A.DatabaseInfoId, 
	DB.DatabaseGroupId,
	DB.DatabaseName,
    A.SchemaName,
	DatabaseObjectId, 
	A.DatabaseObjectName, 
	DatabaseColumnId, 
	A.DatabaseColumnName, 
	IsIdentity, 
	IsNullable, 
	IsPrimaryKey,
	DataType, 
	[Length], 
	[Precision], 
	Scale, 
	MaxLengthInTable, 
    MaxValueInTable,
	NumberOfNulls, 
	NumberOfBlanks,
	coalesce(D.TargetDataType,B.TargetDataType,A.DataType) as TargetDataType,
	case
		when coalesce(D.UseMaxLength,B.UseMaxLength,0) = 1 then A.MaxLengthInTable
		else coalesce(D.TargetLength,B.TargetLength,A.[Length]) 
	end as TargetLength,
	coalesce(D.TargetPrecision,B.TargetPrecision,A.[Precision]) as TargetPrecision,
	coalesce(D.TargetScale,B.TargetScale,A.Scale) as TargetScale,
    --cast(coalesce(D.TrimWhitespace,B.TrimWhitespace,0) as bit) as TrimWhitespace,
	cast(1 as bit) as TrimWhitespace,
	coalesce(D.UseMaxLength,B.UseMaxLength,0) as UseMaxLength,
	SpecificLookupsDone,
	A.UseColumn
from Metadata.DatabaseColumn A
	join Metadata.DatabaseInfo DB on
		A.DatabaseInfoId = DB.DatabaseInfoId
	left join Metadata.DataTypeTranslationsForSourceDatabase B on 
		A.DataType = B.SourceDataType
		and A.[Length] = B.SourceLength
		and A.[Precision] = B.SourcePrecision
		and A.Scale = B.SourceScale
	left join Metadata.DataTypeTranslationsForSpecificColumn D on
		A.DatabaseInfoId = D.DatabaseInfoId
		and A.DatabaseObjectName = D.DatabaseObjectName
		and A.SchemaName = D.SchemaName
		and A.DatabaseColumnName = D.DatabaseColumnName
where A.DatabaseInfoId = @DatabaseInfoId and A.SchemaName = @SchemaName and A.DatabaseObjectName = @TableName 
order by DatabaseColumnId