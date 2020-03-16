create procedure Metadata.CustomUpdateToColumnMetadata
as
begin
	-- use this stored proc to update the column metadata before creating staging tables 
	-- NOTE: this stored proc is run by Generator.CreateStagingObjects.cs automatically
	set nocount on;

	-- reset all use object flags
	update
				B
	set
				B.UseObject = 1
	from		Metadata.DatabaseObject B
		join	Metadata.DatabaseInfo	C
		  on	B.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where		D.DatabaseUse = 'SOURCE';

	-- reset all use column flags
	update
				A
	set
				A.UseColumn = 1
	from		Metadata.DatabaseColumn A
		join	Metadata.DatabaseObject B
		  on	A.DatabaseInfoId = B.DatabaseInfoId
				and A.DatabaseObjectId = B.DatabaseObjectId
		join	Metadata.DatabaseInfo	C
		  on	A.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where		D.DatabaseUse = 'SOURCE';

	-- remove objects which have odd names 
	update
				A
	set
				A.UseObject = 0
	from		Metadata.DatabaseObject A
		join	Metadata.DatabaseInfo	C
		  on	A.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where
				D.DatabaseUse = 'SOURCE'
				and (
						LOWER(DatabaseObjectName) like '%old%'
						or LOWER(DatabaseObjectName) like '%backup%'
						or LOWER(DatabaseObjectName) like '%_2019%'
						or LOWER(DatabaseObjectName) like 'temp%'
						or LOWER(DatabaseObjectName) like '%deleted%'
						or
						-- specific tables
						DatabaseObjectName in ('DimEngagementOldGFIS2019-10-17 00:51:09')
					);

	update
				A
	set
				A.UseObject = 1
	from		Metadata.DatabaseObject A
		join	Metadata.DatabaseInfo	C
		  on	A.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where
				D.DatabaseUse = 'SOURCE'
				and DatabaseObjectName in ('DimPurchasedSold','FactPurchasedSoldBase','FactPurchasedSoldCurrent')

	-- remove columns where the column is always set to null
	update
				A
	set
				A.UseColumn = 0
	from		Metadata.DatabaseColumn A
		join	Metadata.DatabaseObject B
		  on	A.DatabaseInfoId = B.DatabaseInfoId
				and A.DatabaseObjectId = B.DatabaseObjectId
		join	Metadata.DatabaseInfo	C
		  on	A.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where
				D.DatabaseUse = 'SOURCE'
				and A.SpecificLookupsDone = 1
				and B.NumberOfRows <= A.NumberOfNulls;

	-- remove columns where the character string is always set to blank
	update
				A
	set
				A.UseColumn = 0
	from		Metadata.DatabaseColumn A
		join	Metadata.DatabaseObject B
		  on	A.DatabaseInfoId = B.DatabaseInfoId
				and A.DatabaseObjectId = B.DatabaseObjectId
		join	Metadata.DatabaseInfo	C
		  on	A.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where
				D.DatabaseUse = 'SOURCE'
				and A.SpecificLookupsDone = 1
				and B.NumberOfRows <= A.NumberOfBlanks;

	-- remove columns where the max value of the number or integer is zero	
	update
				A
	set
				A.UseColumn = 0
	from		Metadata.DatabaseColumn A
		join	Metadata.DatabaseObject B
		  on	A.DatabaseInfoId = B.DatabaseInfoId
				and A.DatabaseObjectId = B.DatabaseObjectId
		join	Metadata.DatabaseInfo	C
		  on	A.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where
				D.DatabaseUse = 'SOURCE'
				and A.SpecificLookupsDone = 1
				and A.MaxValueInTable = 0;

	-- exclude all varchar(max) and nvarchar(max) columns from import
	update
				A
	set
				A.UseColumn = 0
	from		Metadata.DatabaseColumn A
		join	Metadata.DatabaseObject B
		  on	A.DatabaseInfoId = B.DatabaseInfoId
				and A.DatabaseObjectId = B.DatabaseObjectId
		join	Metadata.DatabaseInfo	C
		  on	A.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where
				D.DatabaseUse = 'SOURCE'
				and A.[Length] = -1;	-- max

	-- update metadata for varchar(max) fields we want in staging
	update
				A
	set
				A.UseColumn = 1
	from		Metadata.DatabaseColumn A
		join	Metadata.DatabaseObject B
		  on	A.DatabaseInfoId = B.DatabaseInfoId
				and A.DatabaseObjectId = B.DatabaseObjectId
		join	Metadata.DatabaseInfo	C
		  on	A.DatabaseInfoId = C.DatabaseInfoId
		join	Metadata.DatabaseUse	D
		  on	C.DatabaseUseId = D.DatabaseUseId
	where
				D.DatabaseUse = 'SOURCE'
				and A.DatabaseColumnName = 'claim_action_plan'
				and A.DatabaseObjectName = 'clm_diary'
				and A.[Length] = -1;  -- max


end;