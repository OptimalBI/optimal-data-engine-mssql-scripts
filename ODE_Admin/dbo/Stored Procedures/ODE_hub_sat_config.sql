CREATE PROCEDURE [dbo].[ODE_hub_sat_config]
--
(
 @SatelliteOnly char(1)				--= 'Y'
	-- when set to "N", the script will create a Hub and Satellite combination.
	-- "Y" will cause the script to create a Satellite and hook it up to the specified Hub.
,@SprintDate CHAR(8)				--= '20170116'
	-- Start Date of the current Sprint in Integer yyyymmdd (this depends on having set up a Sprint Release with the key yyymmdd00
	-- e.g. EXECUTE [dv_release].[dv_release_master_insert] 2016080100, 'Test Sprint release', 'US001', 'Jira'
,@ReleaseReference VARCHAR(50)		--= 'HR-304'
	-- User Story and/or Task numbers for the Satellite which you are building.
,@ReleaseSource VARCHAR(50)			--= 'Jira'
	-- system the reference number refers to e.g. Jira, Rally
,@StageDatabase VARCHAR(128)		--= 'ODE_TempStage'
	-- the name of the Stage Database which holds the table on which the Load will be modelled (this Stage table  needs to exist. The script will use the meta data of the table to build the required Configuration in ODE)
	-- To check, select * from ODE_Config.dbo.dv_source_table where table_name = 'YourSourceTableName' eg. 'Adventureworks__Sales__SalesOrderHeader'
,@StageSchema VARCHAR(128)			--= 'Stage'
,@StageTable VARCHAR(128)			--= 'Sale_Match_Test'
,@StageSourceType VARCHAR(50)		--= 'BespokeProc', 'ExternalStage', 'LeftRightComparison', 'SSISPackage'
,@StageLoadType VARCHAR(50)         -- 'Full' , 'Delta', 'ODEcdc' or 'MSSQLcdc'
,@StagePassLoadTypeToProc BIT		= 0 -- 0 = Dont Pass it on, 1 = Pass Delta / Full to Proc - Only applicable to BespokeProc
,@SourceDataFilter NVARCHAR(MAX)	= NULL -- = 'RecordStatus = 1' - Only applicable to SSISPackage
,@HubName VARCHAR(128)				--= 'link_Sale_Match_Test'  --'Customer'--NULL -- to Default the Hub Name to the sat name - good for pure Raw Vault.
	-- For completely Raw Hub Sat combinations, you can leave this column as null. The Script will create a Hub using the same name as the source table.
	-- For Business hubs, specify the name of the Hub of the Ensemble, which you are adding to.
,@SatelliteName VARCHAR(128)		--= 'link_Sale_Match_Test'
,@VaultName VARCHAR(128)            --=  'ODE_Vault'
	--the name of the vault where the Hub and Satellite will be created.
,@FullScheduleName VARCHAR(128)		= NULL --=  'Full_Load' or leave NULL if job doesn't require to be scheduled
,@IncrementScheduleName VARCHAR(128) = NULL --= 'Increment_Load' or leave NULL if job doesn't require to be scheduled
	--the schedule the load is to run in. This schedule needs to exist prior to running this script.
,@HubKeyNames	[dbo].[dv_column_list] READONLY
--declare @HubKeyNames table(HubKeyName VARCHAR(128)
--                          ,OrdinalPosition INT IDENTITY (1,1)) 
--insert @HubKeyNames values('dv_match_key')
--                         ,('dv_match_date') 
--						 ,('dv_match_row')  
----The name of the unique Key columns. The columns need to exist in your Stage Table, and should be appropriately named for the Hub, which you are building.
-- List the Columns in the order in which you want them to appear in the Hub.
,@SourceSystemName              VARCHAR(128) = NULL
-- The name of the source system as at the table dv_source_system. If the source system is new for this ensemble, it should be created manually first
,@SouceTableSchema				VARCHAR(128) = NULL
-- The schema of the source table. In case of SSIS package source, this should be a schema of the source CDC function
,@SourceTableName				VARCHAR(128) = NULL
-- The source table name. In case of SSIS package source, this should be the source CDC funtion base name, without "_all" suffix though
,@SSISPackageName				VARCHAR(128) = NULL
-- Only required if source type is SSIS package
,@DerivedColumnsList [dbo].[dv_column_matching_list] READONLY
-- The list of derived columns, i.e. columns that don't exist in source, but require to be created on the way to ODE.
-- For example, a part of multi-part hub key that represents a source system.

) AS
BEGIN
SET NOCOUNT ON

--SELECT * FROM @HubKeyNames
/********************************************
Defaults:
********************************************/
DECLARE
 @sat_is_columnstore  BIT = 1
,@sat_is_compressed   BIT = 0
,@hub_is_compressed   BIT = 1
,@stage_is_columnstore BIT = 1
,@stage_is_compressed BIT = 0
	-- Note that Columnstore is only available in SQL Server Enterprise Edition.
,@duplicate_removal_threshold INT = 0
,@hub_schema VARCHAR(128) = 'hub' --or rawhub
,@sat_schema VARCHAR(128) = 'sat' --or rawsat
-- Make sure that schema exists in the Vault
,@DevServerName SYSNAME				= 'Ignore'
	-- You can provide a Server Name here to prevent accidentally creating keys and objects in the wrong environment.
,@BusinessVaultName VARCHAR(128)	= 'Ignore'
	-- You can provide a name here to cause the Business key to be excluded from the Sat, in a specific Vault.
DECLARE @ExcludeColumns TABLE (ColumnName VARCHAR(128))
INSERT @ExcludeColumns  VALUES ('dv_stage_datetime')
								, ('dv_stage_date_time')
								, ('dv_source_version_key')
								, ('dv_cdc_action')
								, ('dv_cdc_high_water_date')
								, ('dv_cdc_start_date')
	--Insert columns which should never be included in the satellites.
--print 'begin'
/********************************************
Begin:
********************************************/

-- Exclude the Hub Key from the Satellite if it is in Business Vault. Otherwise keep it.

SELECT 1 FROM [$(ConfigDatabase)].[dbo].[dv_stage_database] sd
INNER JOIN [$(ConfigDatabase)].[dbo].[dv_stage_schema]ss ON ss.[stage_database_key] = sd.[stage_database_key]
WHERE sd.[stage_database_name] = @StageDatabase
AND ss.[stage_schema_name] = @StageSchema
IF @@ROWCOUNT <> 1 RAISERROR( 'Stage Database %s or Stage Schema %s does not exist', 16, 1, @StageDatabase, @StageSchema)
IF @VaultName =  @BusinessVaultName
INSERT @ExcludeColumns SELECT column_name FROM @HubKeyNames
SELECT @HubName = CASE WHEN ISNULL(@HubName, '') = '' THEN @StageTable ELSE @HubName END
--
--Working Storage
DECLARE 
 @seqint						INT
,@release_number				INT
,@Description					VARCHAR(256)
,@abbn							VARCHAR(4)
,@hub_key_column_type			VARCHAR(30)
,@hub_key_column_length			INT
,@hub_key_column_precision		INT
,@hub_key_column_scale			INT
,@hub_key_Collation_Name		SYSNAME
,@hub_database					SYSNAME
,@release_key					INT
,@hub_key						INT
,@satellite_key					INT
,@hub_key_column_key			INT
,@source_table_key				INT
,@source_version_key			INT
,@match_key						INT
,@left_hub_key_column_key		INT
,@left_link_key_column_key		INT
,@left_satellite_col_key		INT
,@left_column_key				INT
,@left_column_name				VARCHAR(128)
,@right_hub_key_column_key		INT
,@right_link_key_column_key		INT
,@right_satellite_col_key		INT
,@right_column_key				INT
,@right_column_name				VARCHAR(128)
,@hub_source_column_key			INT
,@ServerName					SYSNAME
,@HubKeyName					VARCHAR(128)
,@DerColName					VARCHAR(128)
,@DerColValue					VARCHAR(50)
,@DerColKey						INT
,@DerColType					VARCHAR(30)
,@DerColLength					INT
,@DerColPrecision				INT
,@DerColScale					INT
,@DerColCollation				NVARCHAR(128)
,@DerColOrdinalPos				INT
,@OrdinalPosition				INT
,@StageTableKey					VARCHAR(128)
,@source_procedure_name         VARCHAR(128) = CASE WHEN @StageSourceType = 'BespokeProc' THEN 'usp_' + @StageTable 
												WHEN @StageSourceType = 'SSISPackage' THEN @SSISPackageName ELSE NULL END
,@pass_load_type_to_proc		BIT = CASE WHEN @StageSourceType = 'BespokeProc' THEN @StagePassLoadTypeToProc ELSE 0 END

SELECT @StagePassLoadTypeToProc, @pass_load_type_to_proc
BEGIN TRANSACTION;
BEGIN TRY
SELECT @ServerName = @@SERVERNAME

-- Uncomment this to ensure that this build only happens in the correct place.
--if @ServerName <> @DevServerName
--   begin
--   raiserror( 'This Process may only be run in the Development environment!!', 16, 1)
--   end
--

IF @StageLoadType NOT IN ('Full', 'Delta', 'ODEcdc', 'MSSQLcdc') RAISERROR( '%s is not a valid Load Type', 16, 1, @StageLoadType)
/********************************************
Release:
********************************************/
--'Find the Next Release for the Sprint'
SELECT TOP 1 @seqint = CAST(RIGHT(CAST([release_number] AS VARCHAR(100)), LEN(CAST([release_number] AS VARCHAR(100))) - 8) AS INT)
FROM [$(ConfigDatabase)].[dv_release].[dv_release_master]
WHERE LEFT(CAST([release_number] AS VARCHAR(100)), 8) = @sprintdate
ORDER BY 1 DESC
IF @@ROWCOUNT = 0
SET @release_number = CAST(@sprintdate + '01' AS INT)
ELSE
SET @release_number = CAST(@sprintdate + RIGHT('00' + CAST(@seqint + 1 AS VARCHAR(100)), 2) AS INT)

SELECT @release_number
SET @Description = 'Load Stage Table: ' + QUOTENAME(@StageTable) + ' into ' + QUOTENAME(@VaultName)

-- Create the Release:
EXECUTE  @release_key = [$(ConfigDatabase)].[dv_release].[dv_release_master_insert]  
@release_number		= @release_number	-- date of the Sprint Start + ad hoc release number
,@release_description	= @Description		-- what the release is for
,@reference_number		= @ReleaseReference
,@reference_source		= @ReleaseSource
--
/********************************************
Hub:
********************************************/
-- Configure the Hub:
IF @SatelliteOnly = 'N'
BEGIN
	SELECT @abbn = [$(ConfigDatabase)].[dbo].[fn_get_next_abbreviation]()
	EXECUTE @hub_key = [$(ConfigDatabase)].[dbo].[dv_hub_insert] 
				   @hub_name = @HubName
				  ,@hub_abbreviation = @abbn
				  ,@hub_schema = 'hub'
				  ,@hub_database = @VaultName
				  ,@is_compressed = @hub_is_compressed
				  ,@is_retired = 0
				  ,@release_number = @release_number
END				  
ELSE
BEGIN
	SELECT @hub_key			= [hub_key]
		,@hub_database	= [hub_database]
	FROM [$(ConfigDatabase)].[dbo].[dv_hub] 
	WHERE [hub_name] = @HubName
	IF @hub_database <> @VaultName
		RAISERROR( 'The Hub and Satellite have to exist in the same database', 16, 1)
END
--
/********************************************
Satellite:
********************************************/
-- Configure the Satellite:
SELECT @abbn = [$(ConfigDatabase)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @satellite_key = [$(ConfigDatabase)].[dbo].[dv_satellite_insert] 
						 @hub_key					= @hub_key
						,@link_key					= 0 --Dont fill in for a Hub
						,@link_hub_satellite_flag	= 'H'
						,@satellite_name			= @SatelliteName
						,@satellite_abbreviation	= @abbn
						,@satellite_schema			= 'sat'
						,@satellite_database		= @VaultName
						,@duplicate_removal_threshold = @duplicate_removal_threshold
						,@is_columnstore			= @sat_is_columnstore
						,@is_compressed				= @sat_is_compressed
						,@is_retired				= 0
						,@release_number			= @release_number

/********************************************
Stage Table:
********************************************/
SELECT 'Build the Stage Table with its columns: '
EXECUTE [$(ConfigDatabase)].[dv_config].[dv_populate_source_table_columns] 
   @vault_stage_database		= @StageDatabase
  ,@vault_stage_schema			= @StageSchema
  ,@vault_stage_table			= @StageTable
  ,@vault_source_unique_name	= @StageTable
  --,@vault_source_type			= @StageSourceType
  ,@vault_stage_table_load_type = @StageLoadType
  ,@vault_source_system_name	= @SourceSystemName
  ,@vault_source_table_schema	= @SouceTableSchema
  ,@vault_source_table_name		= @SourceTableName
  ,@vault_release_number		= @release_number
  ,@vault_rerun_column_insert	= 0
  ,@is_columnstore				= @stage_is_columnstore
  ,@is_compressed				= @stage_is_compressed

SELECT  @source_table_key = source_table_key 
FROM [$(ConfigDatabase)].[dbo].[dv_source_table] 
WHERE [source_unique_name] = @StageTable

-- Add a Current Source Version with a "Version" of 1 
EXECUTE @source_version_key = [$(ConfigDatabase)].[dbo].[dv_source_version_insert] 
   @source_table_key		= @source_table_key
  ,@source_version			= 1
  ,@source_type				= @StageSourceType
  ,@source_procedure_name   = @source_procedure_name
  ,@source_filter			= @SourceDataFilter
  ,@pass_load_type_to_proc	= @pass_load_type_to_proc
  ,@is_current				= 1
  ,@release_number			= @release_number
  
--Add derived column constraints

DECLARE curDerCol CURSOR FOR  
SELECT left_column_name, right_column_name
FROM @DerivedColumnsList
ORDER BY left_column_name

OPEN curDerCol   
FETCH NEXT FROM curDerCol INTO @DerColName, @DerColValue  

WHILE @@FETCH_STATUS = 0   
BEGIN 

SELECT @DerColKey = [column_key]
      ,@DerColType = [column_type]
	  ,@DerColLength = [column_length]
	  ,@DerColPrecision = [column_precision]
	  ,@DerColScale = [column_scale]
	  ,@DerColCollation = [Collation_Name]
	  ,@DerColOrdinalPos = [source_ordinal_position]
FROM [$(ConfigDatabase)].[dbo].[dv_column]
WHERE [column_key] IN (
SELECT c.[column_key]
FROM [$(ConfigDatabase)].[dbo].[dv_source_table] st 
INNER JOIN [$(ConfigDatabase)].[dbo].[dv_column] c	ON c.[table_key] = st.[source_table_key]
WHERE 1=1
AND st.source_table_key = @source_table_key
AND c.column_name = @DerColName
)

EXECUTE [$(ConfigDatabase)].[dbo].[dv_column_update]
@column_key = @DerColKey
,@table_key = @source_table_key
,@satellite_col_key = NULL
,@column_name = @DerColName
,@column_type = @DerColType
,@column_length = @DerColLength
,@column_precision = @DerColPrecision
,@column_scale = @DerColScale
,@Collation_Name = @DerColCollation
,@is_derived = 1
,@derived_value = @DerColValue
,@source_ordinal_position = @DerColOrdinalPos
,@is_source_date = 0
,@is_retired = 0

FETCH NEXT FROM curDerCol INTO @DerColName, @DerColValue   
END   

CLOSE curDerCol   
DEALLOCATE curDerCol

-----
SELECT 'Hook the Source Columns up to the Satellite:'
EXECUTE [$(ConfigDatabase)].[dv_config].[dv_populate_satellite_columns] 
   @vault_source_unique_name	= @StageTable
  ,@vault_satellite_name		= @SatelliteName
  ,@vault_release_number		= @release_number
  ,@vault_rerun_satellite_column_insert = 0
--
/********************************************/
SELECT 'Hub Key:'
/********************************************/
SELECT * FROM @HubKeyNames

DECLARE curHubKey CURSOR FOR  
SELECT column_name, ordinal_position
FROM @HubKeyNames
ORDER BY ordinal_position

OPEN curHubKey   
FETCH NEXT FROM curHubKey INTO @HubKeyName, @OrdinalPosition  

WHILE @@FETCH_STATUS = 0   
BEGIN 
--select 'hello;' ,* from [$(ConfigDatabase)].[dbo].[dv_column] where table_key = @source_table_key
SELECT @HubKeyName, @OrdinalPosition, @source_table_key 
-- Create the Hub Key based on the Source Column:
SELECT @hub_key_column_type			= 'varchar'	--[column_type]
	  ,@hub_key_column_length		= 128		--[column_length]
	  ,@hub_key_column_precision	= 0			--[column_precision]
	  ,@hub_key_column_scale		= 0			--[column_scale]
	  ,@hub_key_Collation_Name	    = NULL		--[Collation_Name]
      ,@hub_source_column_key		= [column_key]
FROM [$(ConfigDatabase)].[dbo].[dv_column] c
WHERE [column_key] IN (
SELECT c.[column_key]
FROM [$(ConfigDatabase)].[dbo].[dv_source_table] st 
INNER JOIN [$(ConfigDatabase)].[dbo].[dv_column] c	ON c.[table_key] = st.[source_table_key]
WHERE 1=1
AND st.source_table_key = @source_table_key
AND c.column_name = @HubKeyName
)

SELECT *
FROM [$(ConfigDatabase)].[dbo].[dv_column] c
WHERE [column_key] IN (
SELECT c.[column_key]
FROM [$(ConfigDatabase)].[dbo].[dv_source_table] st 
INNER JOIN [$(ConfigDatabase)].[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE 1=1
AND st.source_table_key = @source_table_key
AND c.column_name = @HubKeyName
)
IF @@ROWCOUNT <> 1 RAISERROR( 'Hub key column %s could not be found in %s stage table', 16, 1, @HubKeyName, @StageTable)
--
IF @SatelliteOnly = 'N'
BEGIN

	SELECT @hub_key
		   ,@HubKeyName
		   ,@hub_key_column_type
		   ,@hub_key_column_length
		   ,@hub_key_column_precision
		   ,@hub_key_column_scale
		   ,@hub_key_Collation_Name
		   ,@OrdinalPosition
		   ,@release_number
	
	EXECUTE @hub_key_column_key = [$(ConfigDatabase)].[dbo].[dv_hub_key_insert] 
								 @hub_key					= @hub_key
								,@hub_key_column_name		= @HubKeyName
								,@hub_key_column_type		= @hub_key_column_type
								,@hub_key_column_length		= @hub_key_column_length
								,@hub_key_column_precision	= @hub_key_column_precision
								,@hub_key_column_scale		= @hub_key_column_scale
								,@hub_key_Collation_Name	= @hub_key_Collation_Name
								,@hub_key_ordinal_position	= @OrdinalPosition
								,@release_number			= @release_number
END
ELSE
BEGIN
	SELECT @hub_key_column_key = [hub_key_column_key]
	FROM [$(ConfigDatabase)].[dbo].[dv_hub_key_column]
	WHERE [hub_key] = @hub_key
	AND [hub_key_column_name] = @HubKeyName

END
-- hook the Hub Key up to the Source Column which will populate it:
SELECT  hub_key_column_key		 = @hub_key_column_key
	   ,hub_source_column_key	 = @hub_source_column_key
	   ,HubKeyName				 = @HubKeyName

EXECUTE [$(ConfigDatabase)].[dbo].[dv_hub_column_insert] 
	 @hub_key_column_key	= @hub_key_column_key
	,@link_key_column_key	= NULL
	,@column_key			= @hub_source_column_key
	,@release_number		= @release_number

FETCH NEXT FROM curHubKey INTO @HubKeyName, @OrdinalPosition   
END   

CLOSE curHubKey   
DEALLOCATE curHubKey
--
/********************************************
Tidy Up:
********************************************/
 SELECT @StageTableKey = REPLACE(REPLACE(column_name, '[', ''), ']','') FROM [$(ConfigDatabase)].[dbo].[fn_get_key_definition] (@StageTable,'stg')
 INSERT INTO @ExcludeColumns VALUES (@StageTableKey)

-- Remove the Columns in the Exclude List from the Satellite:
UPDATE [$(ConfigDatabase)].[dbo].[dv_column]
SET [satellite_col_key] = NULL
WHERE [column_name] IN (
SELECT *
FROM @ExcludeColumns)
AND [column_key] IN (SELECT c.column_key FROM [$(ConfigDatabase)].[dbo].[dv_column] c 
                    INNER JOIN [$(ConfigDatabase)].[dbo].[dv_satellite_column] sc ON sc.[satellite_col_key] = c.[satellite_col_key]
					WHERE sc.[satellite_key] = @satellite_key)

-- If you don't want Keys in the satellites:
	DELETE
	FROM [$(ConfigDatabase)].[dbo].[dv_satellite_column]
	WHERE [satellite_col_key] IN (
		SELECT sc.[satellite_col_key]
		FROM [$(ConfigDatabase)] .[dbo].[dv_satellite_column] sc
		LEFT JOIN [$(ConfigDatabase)] .[dbo].[dv_column] c	ON sc.[satellite_col_key] = c.[satellite_col_key]
		WHERE c.[satellite_col_key] IS NULL
		  AND sc.[satellite_key] = @satellite_key
		  )
/********************************************
Scheduler:
********************************************/
IF @FullScheduleName IS NOT NULL
-- Add the Source the the required Full Schedule:
EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_source_table_insert] 
   @schedule_name				= @FullScheduleName
  ,@source_unique_name			= @StageTable
  ,@source_table_load_type		= 'Full'
  ,@priority					= 'Low'
  ,@queue						= 'Agent001'
  ,@release_number				= @release_number
--
IF @IncrementScheduleName IS NOT NULL
-- Add the Source the the required Increment Schedule:
EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_source_table_insert] 
   @schedule_name				= @IncrementScheduleName
  ,@source_unique_name			= @StageTable
  ,@source_table_load_type		= 'Delta'
  ,@priority					= 'Low'
  ,@queue						= 'Agent001'
  ,@release_number				= @release_number
/********************************************
Useful Commands:
********************************************/
--Output commands to Build the Tables and test the Load:
SELECT CASE WHEN @SatelliteOnly = 'N' THEN 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_hub_table] ''' + @VaultName + ''',''' + @HubName + ''',''N''' ELSE '' END
UNION
SELECT 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_sat_table] ''' + @VaultName + ''',''' + @SatelliteName + ''',''N'''
UNION
 SELECT CASE WHEN @StageLoadType IN ('ODEcdc' , 'MSSQLcdc') THEN 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_stage_table] ''' + @StageTable + ''', ''Y''' END
UNION
 SELECT CASE WHEN @StageSourceType = 'BespokeProc' THEN 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_load_source_table]
 @vault_source_unique_name = ''' + @StageTable + '''
,@vault_source_load_type = ''Full''' ELSE 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_stage_table] ''' + @StageTable + ''',''Y''' END
UNION
SELECT 'select top 1000 * from ' + QUOTENAME(hub_database) + '.' + QUOTENAME(hub_schema) + '.' + QUOTENAME([$(ConfigDatabase)].[dbo].[fn_get_object_name] (hub_name, 'hub'))
FROM [$(ConfigDatabase)].[dbo].[dv_hub] WHERE hub_name = @HubName AND hub_database = @VaultName
UNION
SELECT 'select top 1000 * from ' + QUOTENAME(satellite_database) + '.' + QUOTENAME(satellite_schema) + '.' + QUOTENAME([$(ConfigDatabase)].[dbo].[fn_get_object_name] (satellite_name, 'sat'))
FROM [$(ConfigDatabase)].[dbo].[dv_satellite] WHERE satellite_name =  @SatelliteName AND satellite_database = @VaultName

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