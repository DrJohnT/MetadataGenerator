CREATE TABLE [Metadata].[DatabaseInfo]
(
	DatabaseInfoId int NOT NULL,
	DatabaseDescription varchar(50) NOT NULL,		-- simple name to make it human readable
	DatabaseUseId int not null,						-- is the database a SOURCE or TARGET
	DatabaseGroupId  int not null,					-- ID of the database group - used to differentiate two identical source systems
	DatabaseGroup varchar(20) NULL,					-- Name for a group of source databases
	ServerName varchar(200) NOT NULL,				-- Name of the database server holding the database identified by DatabaseName
	DatabaseName sysname NOT NULL,					-- actual name of the SQL database
    ImportMetadata BIT NOT NULL DEFAULT 0,			-- set to 1 to have Generator to import metadata from the source
	pkPrefix varchar(6) NULL,						-- prefix for primary keys
    CONSTRAINT [PK_DatabaseInfoId] PRIMARY KEY (DatabaseInfoId), 
    CONSTRAINT [FK_DatabaseInfo_DatabaseUse] FOREIGN KEY (DatabaseUseId) REFERENCES Metadata.DatabaseUse(DatabaseUseId), 
) with (data_compression = page);
go
CREATE UNIQUE INDEX [UX_DatabaseDescription] ON [Metadata].DatabaseInfo (DatabaseDescription);
go
--CREATE UNIQUE INDEX [UX_StagingAreaSchema] ON [Metadata].DatabaseInfo (StagingAreaSchema);
go
--CREATE UNIQUE INDEX [UX_pkPrefix] ON [Metadata].DatabaseInfo (pkPrefix);
go
