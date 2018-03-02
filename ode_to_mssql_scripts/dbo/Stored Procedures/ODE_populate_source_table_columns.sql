CREATE PROCEDURE [dbo].[ODE_populate_source_table_columns]
(
	 @vault_source_unique_name				varchar(128)
	,@vault_release_number					int				= 0
	,@DoGenerateError						bit				= 0
	,@DoThrowError							bit				= 1
)
AS
BEGIN
SET NOCOUNT ON;

-- Internal use variables

declare @stage_table_key					 int
       ,@stage_schema_key                    int
	   ,@system_key							 int
	   ,@source_database_name				 varchar(128)
	   ,@SourceSchema						 varchar(128)
	   ,@SourceTable						 varchar(128)
	   ,@procedure_fully_qualified			 nvarchar(512)
	   ,@table_fully_qualified				 nvarchar(512)
	   ,@OPENQUERY							 nvarchar(4000)
       ,@SQL								 nvarchar(4000)
       ,@LinkedServer						 nvarchar(4000)
	   ,@conn_type							 VARCHAR(50)
DECLARE @SourceColumns [dbo].[dv_column_type]


-- log4TSQL Journal Constants 
DECLARE @SEVERITY_CRITICAL      smallint = 1;
DECLARE @SEVERITY_SEVERE        smallint = 2;
DECLARE @SEVERITY_MAJOR         smallint = 4;
DECLARE @SEVERITY_MODERATE      smallint = 8;
DECLARE @SEVERITY_MINOR         smallint = 16;
DECLARE @SEVERITY_CONCURRENCY   smallint = 32;
DECLARE @SEVERITY_INFORMATION   smallint = 256;
DECLARE @SEVERITY_SUCCESS       smallint = 512;
DECLARE @SEVERITY_DEBUG         smallint = 1024;
DECLARE @NEW_LINE               char(1)  = CHAR(10);

-- log4TSQL Standard/ExceptionHandler variables
DECLARE	  @_Error         int
		, @_RowCount      int
		, @_Step          varchar(128)
		, @_Message       nvarchar(512)
		, @_ErrorContext  nvarchar(512)

-- log4TSQL JournalWriter variables
DECLARE   @_FunctionName			varchar(255)
		, @_SprocStartTime			datetime
		, @_JournalOnOff			varchar(3)
		, @_Severity				smallint
		, @_ExceptionId				int
		, @_StepStartTime			datetime
		, @_ProgressText			nvarchar(max)

SET @_Error             = 0;
SET @_FunctionName      = OBJECT_NAME(@@PROCID);
SET @_Severity          = @SEVERITY_INFORMATION;
SET @_SprocStartTime    = sysdatetimeoffset();
SET @_ProgressText      = '' 
SET @_JournalOnOff      = [$(ConfigDatabase)].log4.GetJournalControl(@_FunctionName, 'HOWTO');  -- left Group Name as HOWTO for now.

--set the Parameters for logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @vault_source_unique_name     : ' + COALESCE(@vault_source_unique_name					, '<NULL>')
						+ @NEW_LINE + '    @vault_release_number         : ' + COALESCE(cast(@vault_release_number as varchar)		, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError : ' + COALESCE(CAST(@DoGenerateError AS varchar)						, '<NULL>')
						+ @NEW_LINE + '    @DoThrowError    : ' + COALESCE(CAST(@DoThrowError AS varchar)							, '<NULL>')
						+ @NEW_LINE

BEGIN TRANSACTION
BEGIN TRY   
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';

SET @_Step = 'Initialise Variables';
SELECT @LinkedServer			= ss.[source_system_name]      
	  ,@system_key				= ss.[source_system_key]
	  ,@source_database_name	= ss.[source_database_name]
	  ,@conn_type				= c.[connection_db_type]
FROM [$(ConfigDatabase)].[dbo].[dv_source_table] st
INNER JOIN [$(ConfigDatabase)].[dbo].[dv_source_system] ss ON ss.[source_system_key] = st.[system_key]
LEFT JOIN [$(ConfigDatabase)].[dbo].[dv_connection] c
ON ss.project_connection_name = c.connection_name
where st.[source_unique_name] = @vault_source_unique_name

SET @_Step = 'Create Config For Table';

select @stage_table_key			= s.[source_table_key] 
      ,@stage_schema_key		= ssc.[stage_schema_key]
	  ,@SourceSchema			= s.[source_table_schma]
	  ,@SourceTable				= s.[source_table_nme]
from [$(ConfigDatabase)].[dbo].[dv_source_table] s
inner join [$(ConfigDatabase)].[dbo].[dv_stage_schema] ssc on ssc.[stage_schema_key] = s.[stage_schema_key]
inner join [$(ConfigDatabase)].[dbo].[dv_stage_database] d on d.[stage_database_key] = ssc.[stage_database_key]	
where [source_unique_name] = @vault_source_unique_name
		

--declare @column_list table(column_name varchar(128));

declare @columns table(
		 column_name				varchar(128)
		,column_type				varchar(30)
		,column_length				int 
		,column_precision			int 
		,column_scale				int 
		,Collation_Name				nvarchar(128) 
		,bk_ordinal_position		int
		,source_ordinal_position	int) 

IF (@conn_type = 'MSSQLServer')
BEGIN

	SET @OPENQUERY = 'SELECT * FROM OPENQUERY('+ @LinkedServer + ','''
	SET @SQL = [dbo].[fn_get_MSSQL_metadata_source_statement](@source_database_name, @SourceSchema, @SourceTable, 'stg', NULL)
	SET @SQL = @OPENQUERY + @SQL + ''')'
	PRINT @SQL
END
ELSE IF (@conn_type = 'Oracle') 
BEGIN 
	SET @OPENQUERY = '
		SELECT 
			OQ.COLUMN_NAME,
			Map.DataType,
			Map.DataSize,
			Map.DataPrecision,
			Map.DataScale, 
			OQ.collation_name,
			OQ.bk_ordinal_position,
			OQ.source_ordinal_position,
			OQ.satellite_ordinal_position,
			OQ.abbreviation,
			OQ.object_type

		FROM OPENQUERY('+ @LinkedServer + ','''
	SET @SQL = [dbo].[fn_get_Oracle_metadata_source_statement](@source_database_name, @SourceSchema, @SourceTable, 'stg', NULL)


	-- This following line needs to be modified to replace the datatypes
	SET @SQL = @OPENQUERY + @SQL + ''') AS OQ
		OUTER APPLY $(DatabaseName).[dbo].[fn_map_Oracle_to_SQLServer_DataType](OQ.DATA_TYPE, OQ.DATA_LENGTH, OQ.DATA_PRECISION, OQ.DATA_SCALE) AS Map'
	
END
ELSE 
BEGIN
	-- This condition shouldn't be tripped - but who knows.
	PRINT 'You shouldnt be here.';
END
--print @SQL
INSERT INTO @SourceColumns 
EXEC (@SQL)
declare
@column_name				varchar(128),
@column_type				varchar(30),
@column_length				int = NULL,
@column_precision			int = NULL,
@column_scale				int = NULL,
@Collation_Name				nvarchar(128) = NULL,
--@bk_ordinal_position		int,
@source_ordinal_position	int,
@is_source_date				bit,
@is_retired					bit

--select @sql, @parm_definition, @vault_stage_schema, @vault_stage_table
if @_JournalOnOff = 'ON'
	set @_ProgressText  = @_ProgressText + @NEW_LINE + @sql + @NEW_LINE;
declare Col_Cursor cursor forward_only for 
SELECT column_name,  
       column_type,
       column_length,
       column_precision,
       column_scale,
	   collation_name,
	   source_ordinal_position,
	   0,
	   0
FROM @SourceColumns
order by source_ordinal_position
open Col_Cursor
fetch next from Col_Cursor into  @column_name				
								,@column_type				
								,@column_length				
								,@column_precision			
								,@column_scale				
								,@Collation_Name				
								,@source_ordinal_position	
								,@is_source_date
								,@is_retired	

while @@FETCH_STATUS = 0
begin								

EXECUTE [$(ConfigDatabase)].[dbo].[dv_column_insert] 
	    @table_key					= @stage_table_key
	   ,@satellite_col_key          = NULL
	   ,@release_number				= @vault_release_number
	   ,@column_name				= @column_name					
	   ,@column_type				= @column_type				
	   ,@column_length				= @column_length				
	   ,@column_precision			= @column_precision			
	   ,@column_scale				= @column_scale				
	   ,@Collation_Name				= @Collation_Name				
	   ,@source_ordinal_position	= @source_ordinal_position	
	   ,@is_source_date				= @is_source_date	
	   ,@is_retired			        = @is_retired

fetch next from Col_Cursor into  @column_name				
								,@column_type				
								,@column_length				
								,@column_precision			
								,@column_scale				
								,@Collation_Name				
								,@source_ordinal_position	
								,@is_source_date			
								,@is_retired
end
close Col_Cursor
deallocate Col_Cursor


/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Populated Config for Table: ' + @table_fully_qualified

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Populate Config for Table: ' + @table_fully_qualified
IF (XACT_STATE() = -1) -- uncommitable transaction
OR (@@TRANCOUNT > 0) -- AND XACT_STATE() != 1) -- undocumented uncommitable transaction
	BEGIN
		ROLLBACK TRAN;
		SET @_ErrorContext = @_ErrorContext + ' (Forced rolled back of all changes)';
	END
	
EXEC [$(ConfigDatabase)].log4.ExceptionHandler
		  @ErrorContext  = @_ErrorContext
		, @ErrorNumber   = @_Error OUT
		, @ReturnMessage = @_Message OUT
		, @ExceptionId   = @_ExceptionId OUT
;
END CATCH

--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	--! Clean up

	--!
	--! Use dbo.udf_FormatElapsedTime() to get a nicely formatted run time string e.g.
	--! "0 hr(s) 1 min(s) and 22 sec(s)" or "1345 milliseconds"
	--!
	IF @_Error = 0
		BEGIN
			SET @_Step			= 'OnComplete'
			SET @_Severity		= @SEVERITY_SUCCESS
			SET @_Message		= COALESCE(@_Message, @_Step)
								+ ' in a total run time of ' + [$(ConfigDatabase)].log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
			SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
		END
	ELSE
		BEGIN
			SET @_Step			= COALESCE(@_Step, 'OnError')
			SET @_Severity		= @SEVERITY_SEVERE
			SET @_Message		= COALESCE(@_Message, @_Step)
								+ ' after a total run time of ' + [$(ConfigDatabase)].log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
			SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
		END

	IF @_JournalOnOff = 'ON'
		EXEC [$(ConfigDatabase)].log4.JournalWriter
				  @Task				= @_FunctionName
				, @FunctionName		= @_FunctionName
				, @StepInFunction	= @_Step
				, @MessageText		= @_Message
				, @Severity			= @_Severity
				, @ExceptionId		= @_ExceptionId
				--! Supply all the progress info after we've gone to such trouble to collect it
				, @ExtraInfo        = @_ProgressText

	--! Finally, throw an exception that will be detected by the caller
	IF @DoThrowError = 1 AND @_Error > 0
		RAISERROR(@_Message, 16, 99);

	SET NOCOUNT OFF;

	--! Return the value of @@ERROR (which will be zero on success)
	RETURN (@_Error);
END