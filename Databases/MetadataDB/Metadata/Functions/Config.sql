CREATE FUNCTION [Metadata].[Config] ()
RETURNS @returntable TABLE
(
	TruncateAndLoadRows int
)
AS
BEGIN
	-- this function MUST return ONE row - don't be 'special' and change it to return more!
	INSERT @returntable
	select 200000
	RETURN
END