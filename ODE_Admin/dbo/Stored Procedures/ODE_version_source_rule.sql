
CREATE PROCEDURE [dbo].[ODE_version_source_rule]
--
(@SourceUniqueName				VARCHAR(128)				
	-- The Source for which you are going to create a new Bespoke Procedure
,@SourceProcedureName           VARCHAR(128)	= NULL
	-- The Name of the new Bespoke Procedure.
	-- Note that if no name is provided, this process will assume that the last 4 digits in the Stored Procedure name are the Version, and will make them the same as the "Source_Version".
,@SourceType					VARCHAR(50)	
    -- Currently must be "BespokeProc" as this is the only type of Rule which this proc can Version. More to follow.
,@SprintDate					CHAR(8)			--= '20170116'
	-- Start Date of the current Sprint in Integer yyyymmdd (this depends on having set up a Sprint Release with the key yyymmdd00
	-- e.g. EXECUTE [dv_release].[dv_release_master_insert] 2016080100, 'Test Sprint release', 'US001', 'Jira'
,@ReleaseReference				VARCHAR(50)		--= 'HR-304'
	-- User Story and/or Task numbers for the Satellite which you are building.
,@ReleaseSource					VARCHAR(50)		--= 'Jira'
	-- system the reference number refers to, Rally
,@dogenerateerror				bit				= 0
,@dothrowerror					bit				= 1
) AS
BEGIN
SET NOCOUNT ON
/*******************************************
WORKING STORAGE
*******************************************/
--
DECLARE @source_version_key			INT
       ,@source_version_key_prior	INT
       ,@source_table_key			INT
       ,@source_version				INT
       ,@source_procedure_name		VARCHAR(128)
       ,@pass_load_type_to_proc		BIT
       ,@is_current					BIT
	   ,@seqint						INT
	   ,@release_number				INT
	   ,@Description				VARCHAR(256)
	   ,@release_key				INT
	   ,@source_filter				NVARCHAR(MAX)
	   ,@source_type				VARCHAR(50)

/********************************************
Log4TSQL Journal Constants
********************************************/
--  										
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

-- Log4TSQL Standard/ExceptionHandler variables
DECLARE	  @_Error         int
		, @_RowCount      int
		, @_Step          varchar(128)
		, @_Message       nvarchar(512)
		, @_ErrorContext  nvarchar(512)

-- Log4TSQL JournalWriter variables
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


-- set Log4TSQL Parameters for Logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @SourceUniqueName     : ' + COALESCE(@SourceUniqueName, 'NULL')
						+ @NEW_LINE + '    @SourceProcedureName  : ' + COALESCE(@SourceProcedureName, 'NULL')
						+ @NEW_LINE + '    @SourceType           : ' + COALESCE(@SourceType, 'NULL')						
						+ @NEW_LINE + '    @SprintDate           : ' + COALESCE(@SprintDate, 'NULL')
						+ @NEW_LINE + '    @ReleaseReference     : ' + COALESCE(@ReleaseReference, 'NULL')
						+ @NEW_LINE + '    @ReleaseSource        : ' + COALESCE(@ReleaseSource, 'NULL')
						+ @NEW_LINE + '    @DoGenerateError      : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError         : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0

BEGIN TRANSACTION
/*******************************************/
SET @_Step = 'Validate inputs';
/*******************************************/
IF @SourceType NOT IN ('BespokeProc') RAISERROR('Invalid or Unsupported @SourceType provided: %s', 16, 1, @SourceType)
SELECT @source_version_key_prior	= sv.source_version_key 
	  ,@source_table_key			= st.source_table_key
	  ,@source_version				= sv.source_version_key
	  ,@source_procedure_name		= sv.source_procedure_name
	  ,@pass_load_type_to_proc		= sv.pass_load_type_to_proc
	  ,@is_current					= sv.is_current
	  ,@source_filter				= sv.source_filter
	  ,@source_type					= sv.source_type
FROM [$(ConfigDatabase)].[dbo].[dv_source_table] st
inner join [$(ConfigDatabase)].[dbo].[dv_source_version] sv on sv.source_table_key = st.source_table_key
  WHERE st.[source_unique_name] = @SourceUniqueName
  AND sv.source_type = @SourceType
  AND sv.is_current = 1
IF @@ROWCOUNT <> 1 RAISERROR('Invalid @SourceUniqueName provided: %s', 16, 1, @SourceUniqueName);


/*******************************************/
SET @_Step = 'Build the Release';
/*******************************************/
--'Find the Next Release for the Sprint'
SELECT TOP 1 @seqint = cast(right(cast([release_number] AS VARCHAR(100)), len(cast([release_number] AS VARCHAR(100))) - 8) AS INT)
FROM [$(ConfigDatabase)].[dv_release].[dv_release_master]
WHERE left(cast([release_number] AS VARCHAR(100)), 8) = @sprintdate
ORDER BY 1 DESC
IF @@rowcount = 0
SET @release_number = cast(@sprintdate + '01' AS INT)
ELSE
SET @release_number = cast(@sprintdate + right('00' + cast(@seqint + 1 AS VARCHAR(100)), 2) AS INT)
SELECT @release_number
SET @Description = 'Version Bespoke Proc for Source: ' + quotename(@SourceUniqueName) 

/*******************************************/
SET @_Step = 'Create the Release:';
/*******************************************/

EXECUTE  @release_key = [$(ConfigDatabase)].[dv_release].[dv_release_master_insert]  
						@release_number			= @release_number	-- date of the Sprint Start + ad hoc release number
					   ,@release_description	= @Description		-- what the release is for																
					   ,@reference_number		= @ReleaseReference
					   ,@reference_source		= @ReleaseSource

/*******************************************/
SET @_Step = 'Expire the Current Version:';
/*******************************************/

EXECUTE [$(ConfigDatabase)].[dbo].[dv_source_version_update] 
   @source_version_key		= @source_version_key_prior
  ,@source_table_key		= @source_table_key
  ,@source_version			= @source_version
  ,@source_type				= @source_type
  ,@source_procedure_name	= @source_procedure_name
  ,@source_filter			= @source_filter
  ,@pass_load_type_to_proc  = @pass_load_type_to_proc
  ,@is_current				= 0
--Because the Update Proc doesn't update the Release key (yet):
UPDATE [$(ConfigDatabase)].[dbo].[dv_source_version] 
	SET [release_key] = @release_key
	WHERE [source_version_key] = @source_version_key_prior

/*******************************************/
SET @_Step = 'Increment the Version by 1:';
/*******************************************/
set @source_version = @source_version + 1
-- When Naming the Bespoke Proc,
--     IF a Name is provided, use it
--     OTHERWISE, suffix the existing Proc name with "_nnnn" using the Version Number.
IF ISNULL(@SourceProcedureName, '') = ''
BEGIN
    -- When No Procedure Name is Provided, Use the prior Procedure Name and Suffix it with the Version:
    -- Strip off any Trailing Version Number
	WHILE RIGHT(@source_procedure_name, 1) IN ('_','0','1','2','3','4','5','6','7','8','9')
	BEGIN
		SELECT @source_procedure_name = LEFT(@source_procedure_name, LEN(@source_procedure_name)-1)
	END
	-- Now Add the New Version Number
	SELECT @source_procedure_name += '_' + FORMAT(@source_version, 'd4')
END
ELSE
	-- When a Procedure Name is provided - overide the Version Suffix Method:
	SET @source_procedure_name = @SourceProcedureName
--
/*******************************************/
SET @_Step = 'Create the New Version:';
/*******************************************/ 

EXECUTE [$(ConfigDatabase)].[dbo].[dv_source_version_insert] 
   @source_table_key		= @source_table_key
  ,@source_version			= @source_version
  ,@source_procedure_name   = @source_procedure_name
  ,@source_type				= @source_type
  ,@source_filter			= @source_filter
  ,@pass_load_type_to_proc  = @pass_load_type_to_proc
  ,@is_current				= 1
  ,@release_number			= @release_number

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Versioned ' + @SourceUniqueName

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Version ' + @SourceUniqueName 
IF (XACT_STATE() = -1) -- uncommitable transaction
OR (@@TRANCOUNT > 0 AND XACT_STATE() != 1) -- undocumented uncommitable transaction
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