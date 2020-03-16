CREATE TABLE Metadata.OverrideTableLoadingPattern
(
	DatabaseId int not null,
	DatabaseObjectName sysname NOT NULL, 
	SchemaName sysname NOT NULL, 
	LoadingPattern varchar(255) not null,
	CreatedDate datetime not null default getutcdate(),
	CONSTRAINT [PK_TableLoadingPattern] PRIMARY KEY (DatabaseId, SchemaName, DatabaseObjectName), 
) with (data_compression = page);

