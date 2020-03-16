CREATE TABLE [Metadata].[DataTypeTranslationsForSpecificColumn]
(
	DataTypeTranslationsForSpecificColumnId int not null identity(1,1), -- for ease of update / delete
	DatabaseInfoId int not null,
	DatabaseObjectName sysname NOT NULL, 
	SchemaName sysname NOT NULL, 
	DatabaseColumnName sysname NOT NULL, 
	TargetDataType sysname not null,
	TargetLength int NOT NULL, 
	TargetPrecision tinyint NOT NULL,
	TargetScale tinyint NOT NULL,	
	TrimWhitespace bit null, 
	UseMaxLength bit null, 
    CONSTRAINT [PK_DataTypeTranslationsForSpecificColumn] PRIMARY KEY (DataTypeTranslationsForSpecificColumnId),

) with (data_compression = page);

