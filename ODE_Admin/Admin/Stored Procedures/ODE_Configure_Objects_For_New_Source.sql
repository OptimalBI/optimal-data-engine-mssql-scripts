CREATE PROCEDURE [Admin].[ODE_Configure_Objects_For_New_Source]

(
--********************************************************************************************************************************************************************
 @source_system_name			VARCHAR(128) -- 'TestResults'
,@source_database_name			VARCHAR(50)  -- 'TestResults'
,@source_connection_string		VARCHAR(256) -- 'Data Source=ABCXXXDB01;User ID=User1;Initial Catalog=TestResults;Provider=SQLNCLI11.1;Persist Security Info=True;Connect Timeout=30;'
-- Note that Connections created here will NOT be promoted during a release. You will need to create the necessary connections in each environment.
,@source_connection_password	VARCHAR(128) -- 'qwerty'
,@source_database_type			VARCHAR(128) = 'MSSQLServer' -- current available values are MSSQLServer and Oracle
,@package_folder				VARCHAR(256) -- Integration Services project folder
,@SprintDate CHAR(8)				--= '20170116'
	-- Start Date of the current Sprint in Integer yyyymmdd (this depends on having set up a Sprint Release with the key yyymmdd00
	-- e.g. EXECUTE [dv_release].[dv_release_master_insert] 2016080100, 'Test Sprint release', 'US001', 'Jira'
,@ReleaseReference VARCHAR(50)		--= 'HR-304'
	-- User Story and/or Task numbers for the Satellite which you are building.
,@ReleaseSource VARCHAR(50)			--= 'Jira'
	-- system the reference number refers to e.g. Jira, Rally
--********************************************************************************************************************************************************************
) AS
BEGIN
SET NOCOUNT ON


/********************************************
Begin:
********************************************/
-- Defaults:
DECLARE @source_connection_name		VARCHAR(50)  = @source_system_name
DECLARE @stage_schema_name			VARCHAR(50)  = 'Stage' --Default
DECLARE @package_project			VARCHAR(256) = 'DV_' + @source_system_name
DECLARE @stage_database_name		VARCHAR(50)  = 'ODE_' + @source_system_name + '_Stage'
DECLARE @stage_connection_name      VARCHAR(50)  = @stage_database_name
DECLARE @stage_connection_string	VARCHAR(256) = 'Provider=SQLNCLI11;Data Source=' + @@SERVERNAME + ';Initial Catalog=' + @stage_database_name + ';Integrated Security=SSPI;Connect Timeout=30'

PRINT '@source_system_name:         '+ @source_system_name	
PRINT '@source_database_name:       '+ 	@source_database_name
PRINT '@source_connection_name:     '+ 	@source_connection_name
PRINT '@source_connection_string:   '+ 	@source_connection_string
PRINT '@source_connection_password: '+ 	@source_connection_password	
PRINT ''						 
PRINT '@stage_database_name:        '+ @stage_database_name
PRINT '@stage_schema_name:          '+ @stage_schema_name
PRINT '@stage_connection_name:      '+ @stage_connection_name
PRINT '@stage_connection_string:    '+ @stage_connection_string
PRINT ''							

--Working Storage
DECLARE @RC							INT
       ,@ReleaseKey					INT
	   ,@seqint						INT
	   ,@release_number				INT
	   ,@Description				VARCHAR(256)
SET NOCOUNT ON;
BEGIN TRANSACTION;
BEGIN TRY


/********************************************
Validation:
********************************************/
-- Uncomment this to ensure that this build only happens in the correct place.
--if @ServerName <> @DevServerName
--   begin
--   raiserror( 'This Process may only be run in the Development environment!!', 16, 1)
--   end
--
/********************************************
Release:
********************************************/
--'Find the Next Release for the Sprint'
SELECT TOP 1 @seqint = CAST(RIGHT(CAST([release_number] AS VARCHAR(100)), LEN(CAST([release_number] AS VARCHAR(100))) - 8) AS INT)
FROM [$(ConfigDatabase)].[dv_release].[dv_release_master]
WHERE LEFT(CAST([release_number] AS VARCHAR(100)), 8) = @sprintdate
ORDER BY 1 DESC
IF @@rowcount = 0
SET @release_number = CAST(@sprintdate + '01' AS INT)
ELSE
SET @release_number = CAST(@sprintdate + RIGHT('00' + CAST(@seqint + 1 AS VARCHAR(100)), 2) AS INT)
SELECT @release_number
SET @Description = 'Configuring metadata for ' + QUOTENAME(@source_system_name) + ' data source'
-- Create the Release:
EXECUTE  @ReleaseKey = [$(ConfigDatabase)].[dv_release].[dv_release_master_insert]  
@release_number		= @release_number	-- date of the Sprint Start + ad hoc release number
,@release_description	= @Description		-- what the release is for
,@reference_number		= @ReleaseReference
,@reference_source		= @ReleaseSource

/********************************************
 Register the Source System in Config
********************************************/ 
EXECUTE [$(ConfigDatabase)].[dbo].[dv_source_system_insert] 
   @source_system_name		= @source_system_name
  ,@source_database_name	= @source_database_name
  ,@package_folder			= @package_folder
  ,@package_project			= @package_project
  ,@project_connection_name = @source_connection_name --same as @connection_name under dv_connection_insert
  ,@is_retired				= 0 --Has no real effect, documentary only
  ,@release_number			= @release_number --default

/********************************************
 Register the Stage Database in Config
********************************************/ 
EXECUTE @RC = [$(ConfigDatabase)].[dbo].[dv_stage_database_insert] 
   @stage_database_name		= @stage_database_name
  ,@stage_connection_name	= @stage_connection_name
  ,@is_retired				= 0
  ,@release_number			= @release_number

EXECUTE [$(ConfigDatabase)].[dbo].[dv_stage_schema_insert] 
   @stage_database_key		= @RC
  ,@stage_schema_name		= @stage_schema_name
  ,@is_retired				= 0
  ,@release_number			= @release_number


/********************************************
 Add a Connections
********************************************/
EXECUTE [$(ConfigDatabase)].[dbo].[dv_connection_insert] 
   @connection_name			= @source_connection_name
  ,@connection_string		= @source_connection_string
  ,@connection_password		= @source_connection_password
  ,@connection_db_type		= @source_database_type
  -- Add a Connection for the Stage Database:
EXECUTE [$(ConfigDatabase)].[dbo].[dv_connection_insert] 
   @connection_name			= @stage_connection_name
  ,@connection_string		= @stage_connection_string
  ,@connection_password		= '' 
PRINT '';
PRINT '--succeeded';
-- Commit if successful:
COMMIT;
END TRY
BEGIN CATCH
-- Return any error and Roll Back is there was a problem:
PRINT 'failed';
SELECT 'failed'
,ERROR_NUMBER() AS [errornumber]
,ERROR_SEVERITY() AS [errorseverity]
,ERROR_STATE() AS [errorstate]
,ERROR_PROCEDURE() AS [errorprocedure]
,ERROR_LINE() AS [errorline]
,ERROR_MESSAGE() AS [errormessage];
ROLLBACK;
END CATCH;
END
