
CREATE PROCEDURE [Admin].[ODE_Configure_Source_Table_Single]
--
(
 @release_key					INT			= 0
 ,@KeyDetectionType				VARCHAR(20) = 'Primary' --Valid values are Primary or Unique
,@SourceSystemName              VARCHAR(128) 
,@SourceSchema					VARCHAR(128)			
,@SourceTable					VARCHAR(128)			
,@StageDatabase					VARCHAR(128)		--= e.g. 'ODE_Sales_Stage'
,@StageSchema					VARCHAR(128)		--= 'Stage'
,@StageLoadType					VARCHAR(50)         --= 'Full' or 'Delta'
,@VaultDatabase					VARCHAR(128)        --= e.g. 'ODE_Sales_Vault'
,@ScheduleFullName				VARCHAR(128)
,@ScheduleDeltaName				VARCHAR(128)
,@PrintCreateStatements			BIT		= 0			--0 means don't output the statements, 1 means output the create statements
	--the schedule the load is to run in. This schedule needs to exist prior to running this script.

) AS
BEGIN
SET NOCOUNT ON

/********************************************
Defaults:
********************************************/
DECLARE
 @sat_is_columnstore	BIT = 1
,@sat_is_compressed		BIT = 0
,@stage_is_columnstore	BIT = 1
,@stage_is_compressed	BIT = 0
,@hub_is_compressed		BIT = 1
	-- Note that Columnstore is only available in SQL Server Enterprise Edition.
,@DevServerName			SYSNAME				= 'Ignore'
,@QueueName				VARCHAR(50)			= 'Agent001'
	-- You can provide a Server Name here to prevent accidentally creating keys and objects in the wrong environment.
DECLARE @SourceType		VARCHAR(50) = 'SSISPackage'
DECLARE @ExcludeColumns TABLE (ColumnName VARCHAR(128))
--INSERT @ExcludeColumns  VALUES ('dv_stage_datetime')
	--Insert columns which should never be included in the satellites.

/********************************************
Begin:
********************************************/
--
--Working Storage
DECLARE 
 @seqint						INT
,@release_number				INT
,@abbn							VARCHAR(4)
,@hub_name						VARCHAR(128)
,@column_name					VARCHAR(128)
,@column_type					VARCHAR(30)
,@column_length					INT
,@column_precision				INT
,@column_scale					INT
,@Collation_Name				SYSNAME
,@bk_ordinal_position			INT
,@hub_database					SYSNAME
,@hub_key						INT
,@satellite_key					INT
,@satellite_name				VARCHAR(128)
,@hub_key_column_key			INT
,@source_database_name          VARCHAR(128)
,@source_table_name				VARCHAR(128)
,@source_table_key				INT
,@stage_schema_key				INT
,@source_version_key			INT
,@hub_source_column_key			INT
,@system_key					INT
,@ServerName					SYSNAME
,@HubKeyName					VARCHAR(128)
,@OrdinalPosition				INT
,@source_procedure_name         VARCHAR(128) 
,@pass_load_type_to_proc		INT = 1
,@SQL							NVARCHAR(4000)
,@OpenQuery						NVARCHAR(4000)
,@LinkedServer					NVARCHAR(4000)
,@conn_type						VARCHAR(50)
,@crlf							CHAR(2)	= CHAR(13) + CHAR(10)
DECLARE @HubKeyNames			[dbo].[dv_column_type]

BEGIN TRANSACTION;
BEGIN TRY
select @ServerName = @@servername

/********************************************
Validation:
********************************************/
-- Uncomment this to ensure that this build only happens in the correct place.
--if @ServerName <> @DevServerName
--   begin
--   raiserror( 'This Process may only be run in the Development environment!!', 16, 1)
--   end
--
IF @KeyDetectionType NOT IN ('Primary','Unique','None') raiserror( 'Invalid Key Detection Type (%s) Provided', 16, 1, @KeyDetectionType)
SELECT @release_number = [release_number] 
FROM [$(ConfigDatabase)].[dv_release].[dv_release_master] 
WHERE [release_key] = @release_key
IF @@ROWCOUNT <> 1 raiserror( 'Release key %i does not exist', 16, 1, @release_key)

SELECT 1 FROM [$(ConfigDatabase)].[dbo].[dv_stage_database] sd
INNER JOIN [$(ConfigDatabase)].[dbo].[dv_stage_schema] ss ON ss.[stage_database_key] = sd.[stage_database_key]
WHERE sd.[stage_database_name] = @StageDatabase
AND ss.[stage_schema_name] = @StageSchema
IF @@ROWCOUNT <> 1 raiserror( 'Stage Database %s or Stage Schema %s does not exist', 16, 1, @StageDatabase, @StageSchema)

IF @StageLoadType NOT IN ('Full', 'Delta', 'MSSQLcdc') raiserror( '%s is not a valid Load Type', 16, 1, @StageLoadType)
--if @VaultName =  @BusinessVaultName
--INSERT @ExcludeColumns select column_name from @HubKeyNames
SELECT @LinkedServer = ss.[source_system_name] 
      ,@source_database_name = ss.[source_database_name]
	  ,@system_key = ss.[source_system_key]
	  ,@conn_type = c.[connection_db_type]
FROM [$(ConfigDatabase)].[dbo].[dv_source_system] ss
LEFT JOIN [$(ConfigDatabase)].[dbo].[dv_connection] c
ON ss.project_connection_name = c.connection_name
WHERE ss.[source_system_name] = @SourceSystemName
if @@ROWCOUNT <> 1 raiserror( 'Source System %s does not exist', 16, 1, @SourceSystemName)

SELECT @stage_schema_key = s.[stage_schema_key]
FROM [$(ConfigDatabase)].[dbo].[dv_stage_database] d
INNER JOIN [$(ConfigDatabase)].[dbo].[dv_stage_schema] s ON s.stage_database_key = d.stage_database_key
WHERE d.[stage_database_name] = @StageDatabase AND s.[stage_schema_name] = @StageSchema
if @@ROWCOUNT <> 1 raiserror( 'Stage Schema %s.%s does not exist', 16, 1, @StageSchema, @StageDatabase)

/*
  Currently we are only differentiating between MSSQL (Microsoft SQL Server) and Oracle. Should this
  list of source systems be added to, then it may be worthwhile breaking this logic out to a collection
  of reference tables.
*/

-- IF statement based on database type
IF (@conn_type = 'MSSQLServer')
BEGIN
	-- This is the standard approach, in which we do not need to modify any of the field datatypes

	-- This is a generic open query statement against our defined source
	SET @OPENQUERY = 'SELECT * FROM OPENQUERY('+ @LinkedServer + ','''

	-- Now we go off and get the MSSQL form of the metadata query for the hub key fields (this can be Primary or Unique)
	SET @SQL = [dbo].[fn_get_MSSQL_metadata_source_statement](@source_database_name, @SourceSchema, @SourceTable, 'hub', @KeyDetectionType)

	-- Now we close out the statement ready for it's eventual execution.
	SET @SQL = @OPENQUERY + @SQL + ''')'
	
--	PRINT @SQL
END
ELSE IF (@conn_type = 'Oracle') 
BEGIN 
	/*
	   This approach requires special handling as we need to replace the Oracle source system datatypes
	   with the equivalent mapped alternatives.  Instead of the datatypes from the OpenQuery we want to
	   use the ones returned by the table valued function [fn_map_Oracle_to_SQLServer_DataType].
	*/

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

	-- Now we go off and get the Oracle form of the metadata query for the hub key fields (this can be Primary or Unique)
	SET @SQL = [dbo].[fn_get_Oracle_metadata_source_statement](@source_database_name, @SourceSchema, @SourceTable, 'hub', @KeyDetectionType)

	-- This following line needs to be modified to replace the datatypes
	SET @SQL = @OPENQUERY + @SQL + ''') AS OQ
		OUTER APPLY $(DatabaseName).[dbo].[fn_map_Oracle_to_SQLServer_DataType](OQ.DATA_TYPE, OQ.DATA_LENGTH, OQ.DATA_PRECISION, OQ.DATA_SCALE) AS Map'
	
--	PRINT @SQL

END
ELSE 
BEGIN
	-- This condition shouldn't be tripped - but who knows.
	PRINT 'You shouldn''t be here.';
	PRINT @SQL
	PRINT @conn_type
END


IF @KeyDetectionType IN ('Unique','Primary')
BEGIN	
	INSERT INTO @HubKeyNames EXEC (@SQL)
	IF @@ROWCOUNT < 1 RAISERROR('%s.%s.%s could not be configured successfully', 16, 1, @source_database_name, @SourceSchema, @SourceTable)
END


/********************************************
Hub:
********************************************/
-- Configure the Hub:


BEGIN
SELECT @abbn = [$(ConfigDatabase)].[dbo].[fn_get_next_abbreviation]()
SET @hub_name = @SourceSystemName + '__' + @SourceSchema + '__' + @SourceTable
EXECUTE @hub_key = [$(ConfigDatabase)].[dbo].[dv_hub_insert] 
				   @hub_name = @hub_name
				  ,@hub_abbreviation = @abbn
				  ,@hub_schema = 'hub'
				  ,@hub_database = @VaultDatabase
				  ,@is_compressed = @hub_is_compressed
				  ,@is_retired = 0
				  ,@release_number = @release_number
END		

--
/********************************************
Satellite:
********************************************/

-- Configure the Satellite:
SELECT @abbn = [$(ConfigDatabase)].[dbo].[fn_get_next_abbreviation]()
SET @satellite_name = @SourceSystemName + '__' + @SourceSchema + '__' + @SourceTable
EXECUTE @satellite_key = [$(ConfigDatabase)].[dbo].[dv_satellite_insert] 
						 @hub_key					= @hub_key
						,@link_key					= 0 --Dont fill in for a Hub
						,@link_hub_satellite_flag	= 'H'
						,@satellite_name			= @satellite_name
						,@satellite_abbreviation	= @abbn
						,@satellite_schema			= 'sat'
						,@satellite_database		= @VaultDatabase
						,@duplicate_removal_threshold = 0
						,@is_columnstore			= @sat_is_columnstore
						,@is_compressed				= @sat_is_compressed
						,@is_retired				= 0
						,@release_number			= @release_number

/********************************************
Stage Table:
********************************************/
set @source_table_name = @SourceSystemName + '__' + @SourceSchema + '__' + @SourceTable
EXECUTE @source_table_key = [$(ConfigDatabase)].[dbo].[dv_source_table_insert] 
   @source_unique_name		= @source_table_name
  ,@load_type				= @StageLoadType
  ,@system_key				= @system_key				
  ,@source_table_schema		= @SourceSchema
  ,@source_table_name		= @SourceTable
  ,@stage_schema_key		= @stage_schema_key	
  ,@stage_table_name		= @source_table_name
  ,@is_columnstore			= @stage_is_columnstore
  ,@is_compressed			= @stage_is_compressed
  ,@is_retired				= 0
  ,@release_number			= @release_number	

SELECT 'Populate the Source Table Columns: '
EXECUTE [$(DatabaseName)].[dbo].[ODE_populate_source_table_columns] 
   @vault_source_unique_name	= @source_table_name
  ,@vault_release_number		= @release_number

-- Add a Current Source Version with a "Version" of 1 
SELECT @source_procedure_name = 'Stage__' + @SourceSystemName + '__' + @SourceSchema + '__' + @SourceTable
EXECUTE @source_version_key = [$(ConfigDatabase)].[dbo].[dv_source_version_insert] 
   @source_table_key		= @source_table_key
  ,@source_version			= 1
  ,@source_type				= @SourceType
  ,@source_procedure_name   = @source_procedure_name
  ,@source_filter			= ''
  ,@pass_load_type_to_proc	= @pass_load_type_to_proc
  ,@is_current				= 1
  ,@release_number			= @release_number


SELECT 'Hook the Source Columns up to the Satellite:'
EXECUTE [$(ConfigDatabase)].[dv_config].[dv_populate_satellite_columns] 
   @vault_source_unique_name	= @source_table_name
  ,@vault_satellite_name		= @source_table_name
  ,@vault_release_number		= @release_number
  ,@vault_rerun_satellite_column_insert = 0
--
/********************************************/
select 'Hub Key:'
/********************************************/

DECLARE curHubKey CURSOR FOR  
SELECT column_name,column_type,column_length,column_precision,column_scale,collation_name,bk_ordinal_position
FROM @HubKeyNames
ORDER BY bk_ordinal_position

OPEN curHubKey   
FETCH NEXT FROM curHubKey 
INTO @column_name,@column_type,@column_length,@column_precision,@column_scale,@collation_name,@bk_ordinal_position  

WHILE @@FETCH_STATUS = 0   
BEGIN 
-- Create the Hub Key based on the Source Column:

BEGIN
    SELECT @hub_source_column_key		= [column_key]
		FROM [$(ConfigDatabase)].[dbo].[dv_column] c
		WHERE [column_key] IN (
				SELECT c.[column_key]
				FROM [$(ConfigDatabase)].[dbo].[dv_source_table] st 
				INNER JOIN [$(ConfigDatabase)].[dbo].[dv_column] c	ON c.[table_key] = st.[source_table_key]
				WHERE 1=1
				AND st.source_table_key = @source_table_key
				AND c.column_name = @column_name)

	EXECUTE @hub_key_column_key = [$(ConfigDatabase)].[dbo].[dv_hub_key_insert] 
								 @hub_key					= @hub_key
								,@hub_key_column_name		= @column_name
								,@hub_key_column_type		= @column_type
								,@hub_key_column_length		= @column_length
								,@hub_key_column_precision	= @column_precision
								,@hub_key_column_scale		= @column_scale
								,@hub_key_Collation_Name	= @Collation_Name
								,@hub_key_ordinal_position	= @bk_ordinal_position
								,@release_number			= @release_number
END
-- hook the Hub Key up to the Source Column which will populate it:

EXECUTE [$(ConfigDatabase)].[dbo].[dv_hub_column_insert] 
	 @hub_key_column_key	= @hub_key_column_key
	,@link_key_column_key	= NULL
	,@column_key			= @hub_source_column_key
	,@release_number		= @release_number

FETCH NEXT FROM curHubKey INTO @column_name,@column_type,@column_length,@column_precision,@column_scale,@collation_name,@bk_ordinal_position  
END   

CLOSE curHubKey   
DEALLOCATE curHubKey
--
/********************************************
Tidy Up:
********************************************/

/********************************************
Scheduler:
********************************************/

-- Add the Source table to the required full Schedule:
EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_source_table_insert] 
   @schedule_name				= @ScheduleFullName
  ,@source_unique_name			= @source_table_name
  ,@source_table_load_type		= 'Full'
  ,@priority					= 'Low'
  ,@queue						= @QueueName
  ,@release_number				= @release_number

-- Add the Source table to the required incremental Schedule:
EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_source_table_insert] 
   @schedule_name				= @ScheduleDeltaName
  ,@source_unique_name			= @source_table_name
  ,@source_table_load_type		= 'Delta'
  ,@priority					= 'Low'
  ,@queue						= @QueueName
  ,@release_number				= @release_number
--
/********************************************
Create the necessary objects:
********************************************/
--EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_hub_table] @VaultDatabase, @hub_name, 'N'
--EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_sat_table] @VaultDatabase, @satellite_name, 'N'
--EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_stage_table] @source_table_name, 'N'

--
/********************************************
Useful Commands:
********************************************/
--Output commands to Build the Tables and test the Load:

IF @PrintCreateStatements = 1
BEGIN
	DECLARE  @myTable table (myStatement varchar(2048))
	DECLARE  @myStatements varchar(max) = ''
	INSERT @myTable
	SELECT 
	'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_hub_table] ''' + @VaultDatabase + ''',''' + @hub_name + ''',''N''' 
	UNION
	SELECT 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_sat_table] ''' + @VaultDatabase + ''',''' + @satellite_name + ''',''N'''
	UNION
	SELECT 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_stage_table] ''' + @source_table_name + ''',''N'''
	UNION
	SELECT 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_load_source_table]
	 @vault_source_unique_name = ''' + @source_table_name + '''
	,@vault_source_load_type = ''Full'''
	UNION
	SELECT 'SELECT TOP 1000 * FROM ' + quotename(hub_database) + '.' + quotename(hub_schema) + '.' + quotename([$(ConfigDatabase)].[dbo].[fn_get_object_name] (hub_name, 'hub'))
	from [$(ConfigDatabase)].[dbo].[dv_hub] where hub_name = @hub_name
	UNION
	SELECT 'SELECT TOP 1000 * FROM ' + quotename(satellite_database) + '.' + quotename(satellite_schema) + '.' + quotename([$(ConfigDatabase)].[dbo].[fn_get_object_name]	(satellite_name, 'sat'))
	from [$(ConfigDatabase)].[dbo].[dv_satellite] where satellite_name =  @source_table_name
	SELECT @myStatements+=myStatement + char(10) FROM @mytable
	PRINT @myStatements
	SELECT @myStatements
	PRINT '/**********************************************/'
	PRINT '/**********************************************/'
END
--
PRINT 'succeeded';
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


GO
