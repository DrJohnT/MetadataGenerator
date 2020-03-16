set nocount on;

-- truncate table Metadata.SourceToTargetTable;
merge Metadata.SourceToTargetTable as T
using
(
    select
        SourceDatabaseInfoId,
        TargetDatabaseInfoId,
        SourceDatabaseObjectName,
        TargetDatabaseObjectName
    from
        (
            values
                -- dimAccount
                (1, 15, 'acct_item', 'dimAccount')

        ) as V (SourceDatabaseInfoId, TargetDatabaseInfoId, SourceDatabaseObjectName, TargetDatabaseObjectName)
) as S
on S.SourceDatabaseInfoId = T.SourceDatabaseInfoId
   and  S.TargetDatabaseInfoId = T.TargetDatabaseInfoId
   and  S.SourceDatabaseObjectName = T.SourceDatabaseObjectName
   and  S.TargetDatabaseObjectName = T.TargetDatabaseObjectName
when not matched by target then insert (
                                           SourceDatabaseInfoId,
                                           TargetDatabaseInfoId,
                                           SourceDatabaseObjectName,
                                           TargetDatabaseObjectName
                                       )
                                values
                                (
                                    S.SourceDatabaseInfoId,
                                    S.TargetDatabaseInfoId,
                                    S.SourceDatabaseObjectName,
                                    S.TargetDatabaseObjectName
                                );