CREATE PROCEDURE [Metadata].[DeleteDatabaseObjects]
	@DatabaseInfoId int
AS
begin
	delete from Metadata.DatabaseColumn where DatabaseInfoId = @DatabaseInfoId;

	delete from Metadata.DatabaseObject where DatabaseInfoId = @DatabaseInfoId;
end