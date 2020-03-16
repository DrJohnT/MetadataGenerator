CREATE TABLE Metadata.DatabaseUse
(
	[DatabaseUseId] INT NOT NULL IDENTITY(1,1),
	[DatabaseUse] varchar(20) NOT NULL, -- SOURCE or TARGET
    CONSTRAINT [PK_DatabaseUse] PRIMARY KEY ([DatabaseUseId])
) with (data_compression = page);

