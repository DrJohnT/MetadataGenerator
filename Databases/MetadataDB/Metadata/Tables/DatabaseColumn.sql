CREATE TABLE [Metadata].[DatabaseColumn]
(
	DatabaseInfoId int not null,
	DatabaseObjectId int null, -- object_id
	SchemaName sysname NOT NULL, 
	DatabaseObjectName sysname NOT NULL, 
	DatabaseColumnId int NOT NULL,  -- from column_id in SQL
	DatabaseColumnName sysname NOT NULL, 
	[IsIdentity] bit not null,
	[IsNullable] bit not null,
	DataType sysname not null,
	[Length] int NOT NULL, 
	[Precision] tinyint NOT NULL,
	Scale tinyint NOT NULL,	
    [MaxLengthInTable] INT NULL, 
	[MaxValueInTable] bigint NULL, 
    [NumberOfNulls] INT NULL, 
    [NumberOfBlanks] INT NULL, 
	IsPrimaryKey bit null,
	SpecificLookupsDone bit not null default 0,
	UseColumn bit not null default 1,
	[CreatedDate] datetime not null default getutcdate(),
    CONSTRAINT [PK_DatabaseColumn] PRIMARY KEY (DatabaseInfoId,SchemaName,DatabaseObjectName,DatabaseColumnName), 
    CONSTRAINT [FK_DatabaseColumn_DatabaseObject] FOREIGN KEY (DatabaseInfoId,SchemaName,DatabaseObjectName) REFERENCES Metadata.DatabaseObject(DatabaseInfoId,SchemaName,DatabaseObjectName), 
) with (data_compression = page);

