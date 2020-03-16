CREATE VIEW [BIML].[SourceDatabaseTable]
AS 
with allColumns as (
	SELECT 
		C1.DatabaseInfoId,
		C1.DatabaseObjectName,
		( SELECT '[' + C.DatabaseColumnName  + '],' 
			   FROM [Metadata].DatabaseColumn C 
			  WHERE C.UseColumn = 1
				AND C.DatabaseInfoId = C1.DatabaseInfoId
				and C.DatabaseObjectName = C1.DatabaseObjectName
			  ORDER BY C.DatabaseInfoId, C.DatabaseObjectName
				FOR XML PATH('') 
		) AS AllColumns
	FROM [Metadata].DatabaseColumn C1 
	WHERE C1.UseColumn = 1
	GROUP BY  C1.DatabaseInfoId, C1.DatabaseObjectName
),
TablesToStage as (
	select 
		STG.DatabaseInfoId,
		STG.SchemaName,
		STG.DatabaseObjectName
	from Metadata.DatabaseInfo STGDB 
		join Metadata.DatabaseObject STG on
			STGDB.DatabaseInfoId = STG.DatabaseInfoId
	where STGDB.DatabaseDescription = N'EssentialsAccountsSTG'
		and STG.DatabaseObjectType in ('U','V')
)
select distinct
	DO.DatabaseInfoId,
	D.DatabaseGroupId,
	D.DatabaseName,
	REPLACE(D.DatabaseDescription, ' ','') as ConnectionName,
	D.DatabaseDescription,
	DO.StagingAreaSchema,
	DO.DatabaseObjectName,
	DO.SchemaName as SourceSchemaName,
	case 
		when L.LoadingPattern = 'IncrementalLoad' and L.TimestampColumnName is null and L.IdentityColumnName is null then 'TruncateAndLoad' 
		else L.LoadingPattern
	end as LoadingPattern,
	case when L.LoadingPattern = 'IncrementalLoad' then
		case 
			when L.TimestampColumnName is not null then 'TSTAMP'
			when L.IdentityColumnName is not null then 'IDENTITY'
		end
	end as LoadingPatternSubType,
	L.TimestampColumnName,
	L.IdentityColumnName,
	L.StartDateColumn,
	L.EndDateColumn,
	left(AllColumns, len(AllColumns)-1) as AllColumns  -- remove the trailing comma
from Metadata.DatabaseInfo D
	join Metadata.DatabaseUse U on
		D.DatabaseUseId = U.DatabaseUseId
	join Metadata.DatabaseObject DO on
		D.DatabaseInfoId = DO.DatabaseInfoId
	join Metadata.SuggestedStagingLoadingPattern L on
		DO.DatabaseInfoId = L.DatabaseInfoId
		and DO.DatabaseObjectName = L.DatabaseObjectName
	join allColumns on
		DO.DatabaseInfoId = allColumns.DatabaseInfoId
		and DO.DatabaseObjectName = allColumns.DatabaseObjectName
	join TablesToStage T on
		DO.StagingAreaSchema = T.SchemaName
		and DO.DatabaseObjectName = T.DatabaseObjectName
WHERE U.DatabaseUse = 'SOURCE'

