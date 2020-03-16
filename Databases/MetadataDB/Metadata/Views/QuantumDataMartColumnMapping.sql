create view Metadata.QuantumDataMartColumnMapping
as

select
            S.DatabaseInfoId,
            SDB.DatabaseName             as SourceDatabaseName,
            S.DatabaseObjectName         as SourceTableName,
            S.DatabaseColumnName         as SourceColumnName,
            TOT.DatabaseObjectTypeName   as ObjectTypeInQuantumDM,
            TOB.SchemaName               as SchemaNameInQuantumDM,
            STM.TargetDatabaseObjectName as ObjectNameInQuantumDM,
            TCN.TargetColumnName         as ColumnNameInQuantumDM,
            'Mapped'                     as Comment,
            case
                when S.DataType in ('nvarchar', 'nchar', 'varchar', 'char') then 'String'
                when S.DataType in ('decimal') then 'Numeric'
                when S.DataType in ('datetime', 'date') then 'Date'
                when S.DataType in ('bit') then 'Boolean'
                when S.DataType in ('int', 'bigint') then 'Integer'
                else S.DataType
            end                          as DataType,
            case
                when S.DataType in ('nvarchar', 'nchar', 'varchar', 'char') then CAST(S.[Length] as varchar)
                when S.DataType in ('decimal', 'numeric') then CAST(S.[Precision] as varchar)
                else 'N/A'
            end                          as [Length]
from        Metadata.DatabaseColumn      as S
    join    Metadata.DatabaseObject      SO
      on    S.DatabaseInfoId = SO.DatabaseInfoId
            and S.SchemaName = SO.SchemaName
            and S.DatabaseObjectName = SO.DatabaseObjectName
    join    Metadata.DatabaseInfo        SDB
      on    S.DatabaseInfoId = SDB.DatabaseInfoId
    join    Metadata.LogColumnRename     as TCN -- target column name
      on    S.DatabaseInfoId = TCN.DatabaseInfoId
            and S.SchemaName = TCN.SchemaName
            and S.DatabaseObjectName = TCN.DatabaseObjectName
            and S.DatabaseColumnName = TCN.DatabaseColumnName
    join    Metadata.SourceToTargetTable as STM
      on    S.DatabaseInfoId = STM.SourceDatabaseInfoId
            and S.DatabaseObjectName = STM.SourceDatabaseObjectName
    join    Metadata.DatabaseColumn      as TC
      on    TC.DatabaseInfoId = STM.TargetDatabaseInfoId
            and TC.DatabaseObjectName = STM.TargetDatabaseObjectName
            and TC.DatabaseColumnName = TCN.TargetColumnName
    join    Metadata.DatabaseObject      TOB
      on    TC.DatabaseInfoId = TOB.DatabaseInfoId
            and TC.SchemaName = TOB.SchemaName
            and TC.DatabaseObjectName = TOB.DatabaseObjectName
    join    Metadata.DatabaseObjectType  TOT
      on    TOB.DatabaseObjectType = TOT.DatabaseObjectType
where
            S.DatabaseInfoId in (1, 2)
            and STM.TargetDatabaseInfoId = 15
union
select
            S.DatabaseInfoId,
            SDB.DatabaseName             as SourceDatabaseName,
            S.DatabaseObjectName         as SourceTableName,
            S.DatabaseColumnName         as SourceColumnName,
            TOT.DatabaseObjectTypeName   as ObjectTypeInQuantumDM,
            TOB.SchemaName               as SchemaNameInQuantumDM,
            STM.TargetDatabaseObjectName as ObjectNameInQuantumDM,
            TCN.TargetColumnName         as ColumnNameInQuantumDM,
            N'Code Lookup'               as Comment,
            'String'                     as DataType,
            '100'                        as [Length]
from        Metadata.DatabaseColumn      as S
    join    Metadata.DatabaseObject      SO
      on    S.DatabaseInfoId = SO.DatabaseInfoId
            and S.SchemaName = SO.SchemaName
            and S.DatabaseObjectName = SO.DatabaseObjectName
    join    Metadata.DatabaseInfo        SDB
      on    S.DatabaseInfoId = SDB.DatabaseInfoId
    join    Metadata.LogColumnRename     as TCN -- target column name
      on    S.DatabaseInfoId = TCN.DatabaseInfoId
            and S.SchemaName = TCN.SchemaName
            and S.DatabaseObjectName = TCN.DatabaseObjectName
            and S.DatabaseColumnName = TCN.DatabaseColumnName
    join    Metadata.SourceToTargetTable as STM
      on    S.DatabaseInfoId = STM.SourceDatabaseInfoId
            and S.DatabaseObjectName = STM.SourceDatabaseObjectName
    join    Metadata.DatabaseColumn      as TC
      on    TC.DatabaseInfoId = STM.TargetDatabaseInfoId
            and TC.DatabaseObjectName = STM.TargetDatabaseObjectName
            and TC.DatabaseColumnName = REPLACE(TCN.TargetColumnName, 'Code', 'Description')
    join    Metadata.DatabaseObject      TOB
      on    TC.DatabaseInfoId = TOB.DatabaseInfoId
            --and S.SchemaName = TOB.SchemaName -- do not uncomment!
            and TC.DatabaseObjectName = TOB.DatabaseObjectName
    join    Metadata.DatabaseObjectType  TOT
      on    TOB.DatabaseObjectType = TOT.DatabaseObjectType
where
            S.DatabaseInfoId in (1, 2)
            and STM.TargetDatabaseInfoId = 15
            and RIGHT(S.DatabaseColumnName, 3) = N'_cd';
