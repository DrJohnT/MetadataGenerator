CREATE TABLE [Metadata].[LogColumnRename]
(
	DatabaseInfoId int not null,
	SchemaName sysname NOT NULL, 
	DatabaseObjectName sysname NOT NULL, 
	DatabaseColumnName sysname NOT NULL, 
	TargetColumnName sysname not null,
	CreatedDate datetime not null default getutcdate(),
    CONSTRAINT [PK_LogColumnRename] PRIMARY KEY (DatabaseInfoId, SchemaName, DatabaseObjectName, DatabaseColumnName),
) with (data_compression = page);

