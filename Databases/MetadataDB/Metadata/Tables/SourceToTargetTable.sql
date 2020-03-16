CREATE TABLE Metadata.SourceToTargetTable
(
	SourceDatabaseInfoId int not null,
	SourceDatabaseObjectName sysname NOT NULL,
	TargetDatabaseInfoId int not null,
	TargetDatabaseObjectName sysname NOT NULL,
	CreatedDate datetime not null default getutcdate(),
    CONSTRAINT [PK_SourceToTargetTable] PRIMARY KEY (SourceDatabaseInfoId,SourceDatabaseObjectName,TargetDatabaseInfoId,TargetDatabaseObjectName), 
) with (data_compression = page);
