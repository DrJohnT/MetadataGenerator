CREATE TABLE [Metadata].[DatabaseObjectType]
(
	DatabaseObjectType char(2) NOT NULL,  -- must match SQL type field -'U','V','TF' etc.
	DatabaseObjectTypeName char(50) NOT NULL, 
    CONSTRAINT [PK_DatabaseObjectType] PRIMARY KEY (DatabaseObjectType),
) with (data_compression = page);

