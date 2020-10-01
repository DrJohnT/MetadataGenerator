create procedure [{0}].[Load_{7}{1}]
	@LoadLogId bigint,
	@BatchId int = null,
	@LoadGroup int = null
as 
begin
	/* 
	 * This is auto-generated code from template: Load_SimpleMerge.sql  
	 * Generated by the Generator C# program which can be found at https://github.com/DrJohnT/MetadataGenerator
	 * DO NOT MODIFY!
	 */
	set nocount on;

	declare @CountOfInsertedRows int = null,
		@CountOfUpdatedRows int = null,
		@CountOfDeletedRows int = null,
		@errorMsg nvarchar(max),
		@errorLine int;

    declare @SqlAction table
    (
        SqlAction nvarchar(20)
    );

    exec Logging.Log_ProcedureCall 
		@LoadLogId=@LoadLogId, 
		@ProcedureObjectID=@@PROCID, 
		@ActionType=N'START',
		@BatchId = @BatchId,
		@LoadGroup = @LoadGroup;
;

	begin try

		merge [{2}].[{1}] as T 
		using [{0}].[{1}Insert] as S
		on {3} 
		when matched
		then update set 
				{4}
		when not matched by target then 
		insert ({5},
			LoadLogId
		) 
		values ({6},
			@LoadLogId
		)
		--when not matched by source {8} then 
		--     delete
		output
            $action
        into @SqlAction;

        select
                @CountOfInsertedRows = COUNT(*)
        from    @SqlAction
        where   SqlAction = N'INSERT';

        select
                @CountOfUpdatedRows = COUNT(*)
        from    @SqlAction
        where   SqlAction = N'UPDATE';

        select
                @CountOfDeletedRows = COUNT(*)
        from    @SqlAction
        where   SqlAction = N'DELETE';
		
	end try
	begin catch

		select @errorMsg = error_message(), @errorLine = error_line();

		exec Logging.Log_ProcedureCall 
			@LoadLogId=@LoadLogId, 
			@ProcedureObjectID=@@PROCID, 
			@ActionType=N'ERROR',
			@BatchId = @BatchId,
			@LoadGroup = @LoadGroup;

		throw 50000, @errorMsg, 16;

	end catch

	exec Logging.Log_ProcedureCall 
		@LoadLogId=@LoadLogId, 
		@ProcedureObjectID=@@PROCID, 
		@ActionType=N'END', 
		@CountOfInsertedRows = @CountOfInsertedRows,
		@CountOfUpdatedRows = @CountOfUpdatedRows,
		@CountOfDeletedRows = @CountOfDeletedRows,
		@BatchId = @BatchId,
		@LoadGroup = @LoadGroup;
end	

