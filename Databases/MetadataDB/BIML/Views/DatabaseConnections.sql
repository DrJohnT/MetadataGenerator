CREATE VIEW [BIML].[DatabaseConnections]
AS 
SELECT  
		D.DatabaseInfoId,
		DatabaseName, 
		ServerName, 
		REPLACE(DatabaseDescription, ' ','') as ConnectionName,
		DatabaseDescription,
		U.DatabaseUse
FROM	Metadata.DatabaseInfo D
	JOIN	Metadata.DatabaseUse U 
		ON		D.DatabaseUseId = U.DatabaseUseId

				
