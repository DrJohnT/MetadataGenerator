CREATE VIEW [Metadata].[MissingColumnMetadata]
AS 
with CountOfColumns as (
	select 
		DatabaseInfoId,
		DatabaseObjectName,
		count(*) as CountOfColumns		
	from Metadata.DatabaseColumn 
	group by DatabaseInfoId,
		DatabaseObjectName
)
select 
	T.DatabaseInfoId,
	T.DatabaseObjectName
from Metadata.DatabaseObject T
	 left join CountOfColumns C on 
		C.DatabaseInfoId = T.DatabaseInfoId
		and C.DatabaseObjectName = T.DatabaseObjectName
		and C.CountOfColumns = T.ColumnCount
where C.DatabaseObjectName is null


