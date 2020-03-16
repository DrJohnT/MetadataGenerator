CREATE TABLE [Metadata].[DataTypeTranslationsForSourceDatabase]
(
	DataTypeTranslationsForSourceDatabaseId int not null identity(1,1), -- for ease of update / delete
	DatabaseInfoId int not null,
	SourceDataType sysname not null,
	SourceLength int NOT NULL, 
	SourcePrecision tinyint NOT NULL,
	SourceScale tinyint NOT NULL,	
	TargetDataType sysname not null,
	TargetLength int NOT NULL, 
	TargetPrecision tinyint NOT NULL,
	TargetScale tinyint NOT NULL,	
	TrimWhitespace bit null,
	UseMaxLength bit null, 
    CONSTRAINT [PK_DataTypeTranslationsForSourceDatabase] PRIMARY KEY (DataTypeTranslationsForSourceDatabaseId),
) with (data_compression = page);

