CREATE VIEW [Metadata].[ColumnInfo]
AS 
SELECT 
	T.DatabaseInfoId,
	S.DatabaseGroupId,
	S.DatabaseGroup,
	S.DatabaseDescription,
	S.DatabaseName,
	U.DatabaseUse,
	OT.DatabaseObjectTypeName as ObjectType,
	T.SchemaName,
	T.DatabaseObjectName, 
	DatabaseColumnId, 
	DatabaseColumnName, 
	IsIdentity, 
	IsNullable, 
	DataType, 
	case 
		when ([Length] = -1) then 'max'
		else cast([Length] as varchar)
	end as [Length],
	[Precision], 
	Scale, 
	case 
		when C.NumberOfNulls = T.NumberOfRows then null -- we have no indication of max length!!
		when C.NumberOfBlanks = T.NumberOfRows then null -- we have no indication of max length!!
		when [Length] > 30 then MaxLengthInTable
		else [Length]
	end as MaxLengthInTable,  
	C.MaxValueInTable,
	T.NumberOfRows,
	C.NumberOfNulls,
	case	
		when T.NumberOfRows > 0 then ISNULL(NumberOfNulls,0) * 100.0 / T.NumberOfRows
	end as PercentageNull,
	C.NumberOfBlanks,
	case	
		when T.NumberOfRows > 0 then ISNULL(NumberOfBlanks,0) * 100.0 / T.NumberOfRows
	end as PercentageBlank,
	(ISNULL(NumberOfNulls,0) + ISNULL(NumberOfBlanks,0)) as NumberEmpty,
	case	
		when T.NumberOfRows > 0 then (ISNULL(NumberOfNulls,0) + ISNULL(NumberOfBlanks,0)) * 100.0 / T.NumberOfRows
	end as PercentageEmpty,
	SpecificLookupsDone,
	UseColumn,
	UseObject
FROM [Metadata].DatabaseColumn C
	join [Metadata].DatabaseObject T on 
		C.DatabaseInfoId = T.DatabaseInfoId
		and C.SchemaName = T.SchemaName
		and C.DatabaseObjectName = T.DatabaseObjectName
	join Metadata.DatabaseObjectType OT on
		T.DatabaseObjectType = OT.DatabaseObjectType
	join [Metadata].DatabaseInfo S on
		S.DatabaseInfoId = T.DatabaseInfoId
	join [Metadata].[DatabaseUse] U on
		S.DatabaseUseId = U.DatabaseUseId
