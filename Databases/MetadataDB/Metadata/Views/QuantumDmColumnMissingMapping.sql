create view Metadata.QuantumDmColumnMissingMapping
as
select
        DatabaseObjectName as TableName,
        DatabaseColumnName as ColumnName
from    Metadata.ColumnInfo
where
        DatabaseInfoId = 15
        and SchemaName = 'qDm'
        and DatabaseColumnName not in ('LoadLogId', 'UpdatedLoadLogId', 'DatabaseInfoId')
        and DatabaseColumnName not like 'dim%'
        and DatabaseColumnName not like 'spk%'
        and DatabaseColumnName not like 'sfk%'
        and DatabaseObjectName <> 'dimCalendar'
except
select
        PotentiallyMappedToTable as TableName,
        ColumnNameInQuantumDM    as ColumnName
from    Metadata.QuantumDmPotentialColumnMapping
where   ColumnNameInQuantumDM is not null
