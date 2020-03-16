CREATE VIEW [Metadata].[SuggestedStagingLoadingPattern]
AS 
/*
 * Written by John Tunnicliffe, May 2017
 * 
 * Returns a suggested loading pattern plus a list of columns which could be used for incremental loading of the table.
 * [Metadata].[Config] determines the number of rows in the table used for the TruncateAndLoad option
 */
with tstamp as (
	select 
		C.DatabaseInfoId,
		C.DatabaseObjectName,
		C.SchemaName,
		C.DatabaseColumnName as TimestampColumnName
	from [Metadata].DatabaseColumn C where DataType = 'datetime' and (C.DatabaseColumnName = 'TSTAMP' or C.DatabaseColumnName like '%update%')
),
identityColumn as (
	select 
		C.DatabaseInfoId,
		C.DatabaseObjectName,
		C.SchemaName,
		C.DatabaseColumnName as IdentityColumnName
	from [Metadata].DatabaseColumn C 
	where C.IsIdentity = 1 
),
effectiveStart as (
	select 
		C.DatabaseInfoId,
		C.DatabaseObjectName,
		C.SchemaName,
		C.DatabaseColumnName 
	from [Metadata].DatabaseColumn C where DataType = 'datetime' and C.DatabaseColumnName like '%eff%' and C.DatabaseColumnName like '%start_date%'
	and not C.DatabaseColumnName = 'original_effective_period_start_date'
),
effectiveEnd as (
	select 
		C.DatabaseInfoId,
		C.DatabaseObjectName,
		C.SchemaName,
		C.DatabaseColumnName 
	from [Metadata].DatabaseColumn C where DataType = 'datetime' and C.DatabaseColumnName like '%eff%' and C.DatabaseColumnName like '%end_date%'
),
effectiveStartEnd as (
	select 
		C.DatabaseInfoId,
		C.DatabaseObjectName,
		C.SchemaName,
		C.DatabaseColumnName as StartDateColumn,
		E.DatabaseColumnName as EndDateColumn
	from effectiveStart C
		join effectiveEnd E on
			C.DatabaseInfoId = E.DatabaseInfoId
			and C.DatabaseObjectName = E.DatabaseObjectName
),
effectiveFrom as (
	select 
		C.DatabaseInfoId,
		C.DatabaseObjectName,
		C.SchemaName,
		C.DatabaseColumnName 
	from [Metadata].DatabaseColumn C where DataType = 'datetime' and C.DatabaseColumnName like '%eff%' and C.DatabaseColumnName like '%date_from%' 
),
effectiveTo as (
	select 
		C.DatabaseInfoId,
		C.DatabaseObjectName,
		C.SchemaName,
		C.DatabaseColumnName 
	from [Metadata].DatabaseColumn C where DataType = 'datetime' and C.DatabaseColumnName like '%eff%' and C.DatabaseColumnName like '%date_to%'
),
effectiveFromTo as (
	select 
		C.DatabaseInfoId,
		C.DatabaseObjectName,
		C.SchemaName,
		C.DatabaseColumnName as StartDateColumn,
		E.DatabaseColumnName as EndDateColumn
	from effectiveFrom C
		join effectiveTo E on
			C.DatabaseInfoId = E.DatabaseInfoId
			and C.DatabaseObjectName = E.DatabaseObjectName
)
select distinct
	DO.DatabaseInfoId,
	D.DatabaseName,
	D.DatabaseDescription,
	DO.DatabaseObjectName,
	DO.SchemaName,
	DO.NumberOfRows,
	case
		when DO.NumberOfRows <= C.TruncateAndLoadRows then 'TruncateAndLoad'
		else 'IncrementalLoad'
	end as LoadingPattern,
	case
		when DO.NumberOfRows <= C.TruncateAndLoadRows then null
		else TimestampColumnName
	end as TimestampColumnName,
	case
		when DO.NumberOfRows <= C.TruncateAndLoadRows then null
		else IdentityColumnName
	end as IdentityColumnName,
	case
		when DO.NumberOfRows <= C.TruncateAndLoadRows then null
		when effectiveStartEnd.StartDateColumn is not null then effectiveStartEnd.StartDateColumn
		else effectiveFromTo.StartDateColumn
	end as StartDateColumn,
	case
		when DO.NumberOfRows <= C.TruncateAndLoadRows then null
		when effectiveStartEnd.EndDateColumn is not null then effectiveStartEnd.EndDateColumn
		else effectiveFromTo.EndDateColumn
	end as EndDateColumn
from Metadata.DatabaseInfo D
	cross join Metadata.Config() C 
	join Metadata.DatabaseObject DO on
		D.DatabaseInfoId = DO.DatabaseInfoId
	left join tstamp on
		DO.DatabaseInfoId = tstamp.DatabaseInfoId
		and DO.DatabaseObjectName = tstamp.DatabaseObjectName
		and DO.SchemaName = tstamp.SchemaName
	left join identityColumn on
		DO.DatabaseInfoId = identityColumn.DatabaseInfoId
		and DO.DatabaseObjectName = identityColumn.DatabaseObjectName
		and DO.SchemaName = identityColumn.SchemaName
	left join effectiveStartEnd on
		DO.DatabaseInfoId = effectiveStartEnd.DatabaseInfoId
		and DO.DatabaseObjectName = effectiveStartEnd.DatabaseObjectName
		and DO.SchemaName = effectiveStartEnd.SchemaName
	left join effectiveFromTo on
		DO.DatabaseInfoId = effectiveFromTo.DatabaseInfoId
		and DO.DatabaseObjectName = effectiveFromTo.DatabaseObjectName
		and DO.SchemaName = effectiveFromTo.SchemaName
where DO.StagingAreaSchema is not null
	and rtrim(DO.DatabaseObjectType) in ('U','V')