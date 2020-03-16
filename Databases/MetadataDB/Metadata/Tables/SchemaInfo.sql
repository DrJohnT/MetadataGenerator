CREATE TABLE [Metadata].[SchemaInfo]
(
	SchemaInfoId int NOT NULL, 
	DatabaseInfoId int NOT NULL,
	SchemaName sysname not null,
	StagingAreaSchema varchar(20) NULL,			-- schema to be used by Generator for the staging tables in the staging database - must be unique for the source!
	CONSTRAINT [PK_SchemaInfoId] PRIMARY KEY (SchemaInfoId), 
	CONSTRAINT [FK_SchemaInfoId_DatabaseInfo] FOREIGN KEY (DatabaseInfoId) REFERENCES Metadata.DatabaseInfo(DatabaseInfoId), 
    CONSTRAINT [AK_SchemaInfo_StagingAreaSchema] UNIQUE (StagingAreaSchema), 
)
