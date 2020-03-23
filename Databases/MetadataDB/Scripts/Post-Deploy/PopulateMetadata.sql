set nocount on;

-- Populate DatabaseUse
merge Metadata.DatabaseUse as T
using
(
    select
        DatabaseUse
    from
        (
            values ('SOURCE'),
                   ('CSV'),
                   ('TARGET'),
                   ('METADATA'),
                   ('TABULAR'),
                   ('UNUSED')
        ) as V (DatabaseUse)
) as S
on S.DatabaseUse = T.DatabaseUse
when not matched by target then insert (
                                           DatabaseUse
                                       )
                                values
                                (
                                    S.DatabaseUse
                                );

-- Populate DatabaseObjectType
merge Metadata.DatabaseObjectType as T
using
(
    select
        DatabaseObjectType,
        DatabaseObjectTypeName
    from
        (
            values ('U', 'Table'),
                   ('V', 'View'),
                   ('TF', 'Table Value Function'),
                   ('P', 'Stored Procedure')
        ) as V (DatabaseObjectType, DatabaseObjectTypeName)
) as S
on S.DatabaseObjectType = T.DatabaseObjectType
when not matched by target then insert (
                                           DatabaseObjectType,
                                           DatabaseObjectTypeName
                                       )
                                values
                                (
                                    S.DatabaseObjectType,
                                    S.DatabaseObjectTypeName
                                );

-- Populate the Metadata.DatabaseInfo table with a list of SOURCE and TARGET databases
declare
    @DatabaseUseIdSource   int,
    @DatabaseUseIdTarget   int,
    @DatabaseUseIdTabular  int,
    @DatabaseUseIdMetadata int,
    @DatabaseUseIdUnused   int;

select
        @DatabaseUseIdSource = DatabaseUseId
from    metadata.DatabaseUse
where   DatabaseUse = 'SOURCE';

select
        @DatabaseUseIdTarget = DatabaseUseId
from    metadata.DatabaseUse
where   DatabaseUse = 'TARGET';

select
        @DatabaseUseIdTabular = DatabaseUseId
from    metadata.DatabaseUse
where   DatabaseUse = 'TABULAR';

select
        @DatabaseUseIdMetadata = DatabaseUseId
from    metadata.DatabaseUse
where   DatabaseUse = 'METADATA';

select
        @DatabaseUseIdUnused = DatabaseUseId
from    metadata.DatabaseUse
where   DatabaseUse = 'UNUSED';

-- Populate the Metadata.DatabaseInfo table with a list of TARGET databases
merge Metadata.DatabaseInfo as T
using
(
    select
        DatabaseUseId,
        DatabaseInfoId,
		DatabaseGroupId,
        DatabaseGroup,
        DatabaseDescription,
        DatabaseName,
        ServerName,
        pkPrefix,
        ImportMetadata
    from
        (
            -- Note that the DatabaseInfoId's must remain fixed.  DO NOT change them.  
            values
            -- DEV
                (@DatabaseUseIdSource, 1, 1, 'MDFL', 'MDFL DEV', 'MR_MDFL_DEV1', 'DEFRNCMUIMTSQ12.eyua.net\INST2', null, 1),
                (@DatabaseUseIdSource, 2, 1, 'MDFL', 'MDFL Source DEV', 'MR_SOURCE_DEV1', 'DEFRNCMUIMTSQ12.eyua.net\INST2', null, 1),
                (@DatabaseUseIdTarget, 3, 2, 'EssentialsAccounts', 'EssentialsAccountsSTG', 'EssentialsAccountsSTG', 'localhost', null, 1),
                (@DatabaseUseIdTarget, 4, 2, 'EssentialsAccounts', 'EssentialsAccountsDB', 'EssentialsAccountsDB', 'localhost', null, 1)
            -- UAT
            --    (@DatabaseUseIdSource, 1, 1, 'MDFL', 'MDFL UAT', 'MR_MDFL_UAT1', 'DEFRNCMUIMTSQ14.eyua.net\INST2', null, 1),
            --    (@DatabaseUseIdSource, 2, 1, 'MDFL', 'MDFL Source UAT', 'MR_Source_UAT1', 'DEFRNCMUIMTSQ14.eyua.net\INST2', null, 1)
        ) as V (DatabaseUseId, DatabaseInfoId, DatabaseGroupId, DatabaseGroup, DatabaseDescription, DatabaseName, ServerName, pkPrefix, ImportMetadata)
) as S
on S.DatabaseInfoId = T.DatabaseInfoId
when matched and (
                     T.DatabaseUseId <> S.DatabaseUseId
					 or T.DatabaseGroupId <> S.DatabaseGroupId
                     or T.DatabaseGroup <> S.DatabaseGroup
                     or T.DatabaseDescription <> S.DatabaseDescription
                     or T.DatabaseName <> S.DatabaseName
                     or T.ServerName <> S.ServerName
                     or T.pkPrefix <> S.pkPrefix
                 ) then update set
                            DatabaseUseId = S.DatabaseUseId,
							DatabaseGroupId = S.DatabaseGroupId,
                            DatabaseGroup = S.DatabaseGroup,
                            DatabaseDescription = S.DatabaseDescription,
                            DatabaseName = S.DatabaseName,
                            ServerName = S.ServerName,
                            pkPrefix = S.pkPrefix
when not matched by target then insert (
                                           DatabaseInfoId,
                                           DatabaseDescription,
                                           DatabaseUseId,
										   DatabaseGroupId,
                                           DatabaseGroup,
                                           ServerName,
                                           DatabaseName,
                                           ImportMetadata,
                                           pkPrefix
                                       )
                                values
                                (
                                    S.DatabaseInfoId,
                                    S.DatabaseDescription,
                                    S.DatabaseUseId,
									S.DatabaseGroupId,
                                    S.DatabaseGroup,
                                    S.ServerName,
                                    S.DatabaseName,
                                    S.ImportMetadata,
                                    S.pkPrefix
                                )
when not matched by source then delete;

merge Metadata.SchemaInfo as T
using
(
 select
        SchemaInfoId,
        DatabaseInfoId,
		SchemaName,
        StagingAreaSchema
    from
        (
            -- Note that the SchemaInfoId's must remain fixed.  DO NOT change them.  
            values (1, 1, 'mdfl', 'stgMdfl'),
                (2, 1, 'ods', 'stgOds'),
                (3, 1, 'stg', 'stgStg'),
                (4, 2, 'wec', 'stgWec')
        ) as V (SchemaInfoId, DatabaseInfoId, SchemaName, StagingAreaSchema)
) as S
on S.SchemaInfoId = T.SchemaInfoId
when matched and (
                     T.DatabaseInfoId <> S.DatabaseInfoId
					 or T.SchemaName <> S.SchemaName
                     or T.StagingAreaSchema <> S.StagingAreaSchema                    
                 ) then update set
                            DatabaseInfoId = S.DatabaseInfoId,
							SchemaName = S.SchemaName,
                            StagingAreaSchema = S.StagingAreaSchema
when not matched by target then insert (
                                           SchemaInfoId,
                                           DatabaseInfoId,
                                           SchemaName,
										   StagingAreaSchema
                                       )
                                values
                                (
                                    S.SchemaInfoId,
                                    S.DatabaseInfoId,
                                    S.SchemaName,
									S.StagingAreaSchema
                                )
when not matched by source then delete;