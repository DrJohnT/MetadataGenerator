CREATE PROCEDURE [Metadata].[GetDatabaseObjects]
	@DatabaseInfoId int
AS
-- Called by C# in PopulateDataModelFromMetadataDatabase
select
    A.DatabaseInfoId,
	D.DatabaseGroupId,
	D.DatabaseName,
    A.DatabaseObjectId,
    upper(ltrim(rtrim(A.DatabaseObjectType))) as DatabaseObjectType,
    A.DatabaseObjectName,
    A.SchemaName,
    A.ColumnCount,
    A.NumberOfRows,
	case 
		when B.LoadingPattern is null then 
			case 
				when upper(left(A.DatabaseObjectName,4)) = N'FACT' then N'StandardMerge'
				when upper(left(A.DatabaseObjectName,3)) = N'DIM' then N'StandardMerge'
				else N'StandardInsert'
			end
		else B.LoadingPattern
	end as LoadingPattern,
	A.UseObject
from Metadata.DatabaseObject A
	join Metadata.DatabaseInfo D on
		A.DatabaseInfoId = D.DatabaseInfoId
	left join Metadata.OverrideTableLoadingPattern B on
		A.DatabaseInfoId = B.DatabaseId
		and A.DatabaseObjectName = B.DatabaseObjectName
		and A.SchemaName		= B.SchemaName
where A.DatabaseObjectType in ('U','V')
	and A.DatabaseInfoId = @DatabaseInfoId
go