CREATE PROCEDURE [Admin].[ODE_Build_Objects_For_New_Source]

(
--********************************************************************************************************************************************************************
 @source_system_name			VARCHAR(128) -- 'TestResults'
,@source_database_name			VARCHAR(50)  -- 'TestResults'
,@source_connection_string		VARCHAR(256) -- 'Data Source=ABCXXXDB01;User ID=User1;Initial Catalog=TestResults;Provider=SQLNCLI11.1;Persist Security Info=True;'
-- Note that Connections created here will NOT be promoted during a release. You will need to create the necessary connections in each environment.
,@source_connection_password	VARCHAR(128) -- ''
,@source_database_type			VARCHAR(128) = 'MSSQLServer' -- current available values are MSSQLServer and Oracle
,@package_folder				VARCHAR(256) -- Integration Services project folder
,@release_number				INT			 = 0
--********************************************************************************************************************************************************************
) AS
BEGIN
SET NOCOUNT ON


/********************************************
Begin:
********************************************/
-- Defaults:
DECLARE 
       @source_connection_name		VARCHAR(50)  = @source_system_name
	   ,@stage_schema_name          VARCHAR(50)  = 'Stage' --Default
	   ,@schedule_name              VARCHAR(128) = @source_system_name + '_Incremental' -- Creates new schedule per data source

DECLARE @package_project			VARCHAR(256) = 'DV_' + @source_system_name
DECLARE @stage_database_name		VARCHAR(50)  = 'ODE_' + @source_system_name + '_Stage'
DECLARE @stage_connection_name      VARCHAR(50)  = @stage_database_name
DECLARE @schedule_description		VARCHAR(256) = 'Collection of ' + @source_system_name + ' Tables to be Loaded Daily'
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
PRINT '@schedule_name:              '+ @schedule_name
PRINT '@schedule_description:       '+ @schedule_description
--Working Storage
DECLARE @RC							INT
       ,@ReleaseKey					INT
	   ,@seqint						INT

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
if @release_number > -1 select @ReleaseKey = [release_key] from [$(ConfigDatabase)].[dv_release].[dv_release_master] where [release_number] = @release_number  
                else set @ReleaseKey = -1

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
 Create a Schedule for the new Source System
********************************************/
EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_insert] 
   @schedule_name			= @schedule_name 
  ,@schedule_description	= @schedule_description
  ,@schedule_frequency		= 'Daily' --documentary only
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
