create view Metadata.QuantumDmPotentialColumnMapping
as
select
                S.DatabaseInfoId,
                S.DatabaseName,
                S.DatabaseObjectName                     as TableName,
                S.DatabaseColumnName                     as ColumnName,
                B.TargetColumnName                       as WouldBeRenamedTo,
                STM.TargetDatabaseObjectName             as PotentiallyMappedToTable,
                TC.DatabaseColumnName                    as ColumnNameInQuantumDM,
                --S.IsNullable,
                case
                    when S.DataType in ('nvarchar', 'nchar', 'varchar', 'char') then 'String'
                    when S.DataType in ('decimal') then 'Numeric'
                    when S.DataType in ('datetime', 'date') then 'Date'
                    when S.DataType in ('bit') then 'Boolean'
                    when S.DataType in ('int', 'bigint') then 'Integer'
                    else S.DataType
                end                                      as DataType,
                case
                    when S.DataType in ('nvarchar', 'nchar', 'varchar', 'char') then S.[Length]
                    when S.DataType in ('decimal', 'numeric') then CAST(S.[Precision] as varchar)
                    else 'N/A'
                end                                      as [Length],
                --S.[Precision],
                --S.MaxLengthInTable,
                --S.MaxValueInTable,
                S.NumberOfRows,
                S.NumberEmpty,
                CAST(S.PercentageEmpty as numeric(5, 1)) as PercentageEmpty
from            Metadata.ColumnInfo          as S
    left join   Metadata.LogColumnRename     as B
      on        S.DatabaseInfoId = B.DatabaseInfoId
                and S.DatabaseObjectName = B.DatabaseObjectName
                and S.DatabaseColumnName = B.DatabaseColumnName
    left join   Metadata.SourceToTargetTable as STM
      on        S.DatabaseInfoId = STM.SourceDatabaseInfoId
                and S.DatabaseObjectName = STM.SourceDatabaseObjectName
    left join   Metadata.ColumnInfo          as TC
      on        TC.DatabaseInfoId = STM.TargetDatabaseInfoId
                and TC.DatabaseObjectName = STM.TargetDatabaseObjectName
                and TC.DatabaseColumnName = B.TargetColumnName
                and TC.ObjectType = 'View'
where
                S.DatabaseInfoId in (1, 2)
                and STM.TargetDatabaseInfoId = 15
                and S.UseObject = 1
                and S.UseColumn = 1
                and S.ObjectType = 'Table'
                and S.DatabaseColumnName not like '%id'
                and S.DatabaseObjectName not like '%audit%'
                and S.NumberOfRows > 0
                and S.PercentageEmpty < 100;
