create procedure [{0}].[Load_{4}{1}]
    @LoadLogId bigint
as 
begin
	/* 
	 * This is auto-generated code from template: Load_StandardExcept.sql  
	 * Generated by the Generator C# program which can be found in the \Code\Generator folder 
	 * DO NOT MODIFY!
	 */
	set nocount on;

	declare @CountOfInsertedRows int = null,
		@CountOfUpdatedRows int = null,
		@CountOfDeletedRows int = null,
		@errorMsg nvarchar(max),
		@errorLine int;

	declare @SqlAction table(SqlAction nvarchar(20));

    exec Logging.Log_ProcedureCall 
		@LoadLogId=@LoadLogId, 
		@ProcedureObjectID=@@PROCID, 
		@ActionType=N'START';
	
	begin try

		insert into [{2}].[{1}]
		output  $action into @SqlAction
		({3}
		)
		select {3}
		from [{0}].[{1}Insert] i;

		select @CountOfInsertedRows = COUNT(*) from @SqlAction where SqlAction = N'INSERT';
		select @CountOfUpdatedRows = COUNT(*) from @SqlAction where SqlAction = N'UPDATE';
		select @CountOfDeletedRows = COUNT(*) from @SqlAction where SqlAction = N'DELETE';			
	end try
	begin catch

		select @errorMsg = error_message(), @errorLine = error_line();
		exec Logging.Log_ProcedureCall 
			@LoadLogId=@LoadLogId, 
			@ProcedureObjectID=@@PROCID, 
			@ActionType=N'ERROR';
		throw 50000, @errorMsg, 16;

	end catch

	exec Logging.Log_ProcedureCall 
		@LoadLogId=@LoadLogId, 
		@ProcedureObjectID=@@PROCID, 
		@ActionType=N'END', 
		@CountOfInsertedRows = @CountOfInsertedRows,
		@CountOfUpdatedRows = @CountOfUpdatedRows,
		@CountOfDeletedRows = @CountOfDeletedRows;
end	