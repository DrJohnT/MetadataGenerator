CREATE TABLE [Metadata].[DatabaseObject]
(
	-- Covers tables and views
	DatabaseInfoId int not null,
	DatabaseObjectId int null,  -- object_id
	DatabaseObjectType char(2) NOT NULL,  -- Table or View
	DatabaseObjectName sysname NOT NULL, 
	SchemaName sysname NOT NULL, 
	StagingAreaSchema sysname NULL, 
	ColumnCount int not null,
    [NumberOfRows] bigint NULL, 
	UseObject bit not null default(1),
	[CreatedDate] datetime not null default(getutcdate()),
    CONSTRAINT [FK_DatabaseObject_DatabaseObjectType] FOREIGN KEY (DatabaseObjectType) REFERENCES Metadata.DatabaseObjectType(DatabaseObjectType), 
    CONSTRAINT [PK_DatabaseObject] PRIMARY KEY (DatabaseInfoId, SchemaName, DatabaseObjectName), 
    CONSTRAINT [FK_DatabaseObject_Database] FOREIGN KEY (DatabaseInfoId) REFERENCES Metadata.DatabaseInfo(DatabaseInfoId),
) with (data_compression = page);

