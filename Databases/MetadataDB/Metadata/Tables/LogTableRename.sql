CREATE TABLE [Metadata].[LogTableRename]
(
	DatabaseInfoId int not null,
	SchemaName sysname NOT NULL, 
	DatabaseObjectName sysname NOT NULL, 
	TargetObjectName sysname NOT NULL, 
	CreatedDate datetime not null default getutcdate(),
    CONSTRAINT [PK_LogTableRename] PRIMARY KEY (DatabaseInfoId, SchemaName, DatabaseObjectName),
) with (data_compression = page);
