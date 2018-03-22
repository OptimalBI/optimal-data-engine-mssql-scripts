
-------------------------------------------------------------------------------------------
--Populate the following Parameters:
CREATE PROCEDURE [dbo].[ODE_link_sat_config]
--
(
@SatelliteOnly					CHAR(1)	--= 'N'
	-- when set to "N", the script will create a Hub and Satellite combination.
	-- "Y" will cause the script to create a Satellite and hook it up to the specified Hub.
,@SprintDate					CHAR(8)	--= '20170111'
	-- Start Date of the current Sprint in Integer yyyymmdd (this depends on having set up a Sprint Release with the key yyymmdd00
	-- e.g. EXECUTE [dv_release].[dv_release_master_insert] 2016080100, 'Test Sprint release', 'US001', 'Jira'
,@ReleaseReference				VARCHAR(50)--= 'HR-309'
	-- User Story and/or Task numbers for the Satellite which you are building.
,@ReleaseSource					VARCHAR(50)	--= 'Jira'
	-- system the reference number refers to, Rally
,@StageDatabase					VARCHAR(128)--= 'ode_stage_MAGODE_40'
	-- the name of the Stage Database which holds the table on which the Load will be modelled (this Stage table  needs to exist. The script will use the meta data of the table to build the required Configuration in ODE)
	-- To check, select * from ODE_Config.dbo.dv_source_table where table_name = 'YourSourceTableName' eg. 'Adventureworks__Sales__SalesOrderHeader'
,@StageSchema					VARCHAR(128)--= 'Stage'
,@StageTable					VARCHAR(128)--= 'link_Sale'
,@StageSourceType				VARCHAR(50) --= 'BespokeProc', 'ExternalStage'
,@StageLoadType					VARCHAR(50) -- 'Full' , 'Delta'
,@StagePassLoadTypeToProc		BIT		-- 0 = Dont Pass it on, 1 = Pass Delta / Full to Proc
,@SourceDataFilter				NVARCHAR(MAX) = NULL -- = 'RecordStatus = 1' - Only applicable to SSISPackage
,@LinkName						VARCHAR(128)				--= 'Sale_Customer_Order'
	-- For completely Raw Links, you can leave this column as null. The Script will create a Link using the same name as the source table.
	-- For Business Links, specify the name of the Link in the Ensemble.
,@SatelliteName					VARCHAR(128)		--= 'link_Sale_Customer_Order'
,@VaultName						VARCHAR(128)			--=  'ODE_vault'
	--the name of the vault where the Hub and Satellite will be created.
,@FullScheduleName				VARCHAR(128)	= NULL	--=  'Full_Load' or leave NULL if job doesn't require to be scheduled
,@IncrementScheduleName			VARCHAR(128) = NULL --= 'Increment_Load' or leave NULL if job doesn't require to be scheduled
	--the schedule the load is to run in. This schedule needs to exist prior to running this script.
--EXECUTE [dv_scheduler].[dv_schedule_insert] 'Link_Schedule', 'For Testing Purposes', 'Ad Hoc', 0
,@Hub_key_list [dbo].[dv_link_detail_list] READONLY
-- A list of hub keys
/*
Fields are:
 hub key as it will be shown at link
 hub actual name
 actual hub column name
 stage column name
INSERT INTO @Hub_key_list VALUES ('Customer', 'Customer', 'CustomerID', 'CUST_ID')
*/
,@SourceSystemName				VARCHAR(128) = NULL
-- The name of the source system as at the table dv_source_system. If the source system is new for this ensemble, it should be created manually first
,@SouceTableSchema				VARCHAR(128) = NULL
-- The schema of the source table. In case of SSIS package source, this should be a schema of the source CDC function
,@SourceTableName				VARCHAR(128) = NULL
-- The source table name. In case of SSIS package source, this should be the source CDC funtion base name, without "_all" suffix though
--,@DerivedColumnsList [dbo].[dv_column_matching_list] READONLY
-- The list of derived columns, i.e. columns that don't exist in source, but require to be created on the way to ODE.
-- For example, a part of multi-part hub key that represents a source system.
)
AS 
BEGIN
SET NOCOUNT ON

--INSERT @Hub_Key_List  VALUES ('Adventureworks__Sales__SalesOrderDetail', 'SalesOrderDetailID_R')
select * from @Hub_Key_List

/********************************************
Defaults:
********************************************/
DECLARE
 @sat_is_columnstore  BIT = 1
,@sat_is_compressed   BIT = 0
,@link_is_compressed  BIT = 1
,@stage_is_columnstore BIT = 1
,@stage_is_compressed BIT = 0
	-- Note that Columnstore is only available in SQL Server Enterprise Edition.
,@duplicate_removal_threshold INT = 0
,@link_schema VARCHAR(128) = 'lnk' --or rawlnk
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
	-- Exclude the Hub Key from the Satellite if it is in Business Vault. Otherwise keep it.

/********************************************
Begin:
********************************************/

SELECT 1 FROM [$(ConfigDatabase)].[dbo].[dv_stage_database] sd
INNER JOIN [$(ConfigDatabase)].[dbo].[dv_stage_schema] ss ON ss.[stage_database_key] = sd.[stage_database_key]
WHERE sd.[stage_database_name] = @StageDatabase
AND ss.[stage_schema_name] = @StageSchema
IF @@ROWCOUNT <> 1 RAISERROR( 'Stage Database %s or Stage Schema %s does not exist', 16, 1, @StageDatabase, @StageSchema)

SELECT @LinkName = CASE WHEN ISNULL(@LinkName, '') = '' THEN @StageTable ELSE @LinkName END
--Working Storage
DECLARE @seqint					INT
,@release_number				INT
,@Description					VARCHAR(256)
,@uspName						VARCHAR(256)
,@abbn							VARCHAR(4)
--
,@link_key						INT
,@satellite_key					INT
,@link_database					SYSNAME
,@release_key					INT
--
,@hub_name						varchar(128)
,@hub_key						int
,@column_name					varchar(128)
,@hub_key_column_key			INT
,@link_key_column_key			INT
,@hub_source_column_key			INT
--
,@curLinkHub_hub_name			varchar(128)
,@curLinkHub_link_key_name		varchar(128)
,@curLinkHub_column_name		varchar(128)
,@curLinkHub_hub_column_name	varchar(128)
,@thisHub						varchar(128)
,@thisLink_Key					varchar(128)
--
,@DerColName					VARCHAR(128)
,@DerColValue					VARCHAR(50)
,@DerColKey						INT
,@DerColType					VARCHAR(30)
,@DerColLength					INT
,@DerColPrecision				INT
,@DerColScale					INT
,@DerColCollation				NVARCHAR(128)
,@DerColOrdinalPos				INT
,@ServerName					SYSNAME
,@source_table_key				INT
,@source_version_key			INT
,@StageTableKey					VARCHAR(128)
,@source_procedure_name         VARCHAR(128) = CASE WHEN @StageSourceType = 'BespokeProc' THEN 'usp_' + @StageTable 
												ELSE NULL END
,@pass_load_type_to_proc		BIT = CASE WHEN @StageLoadType = 'BespokeProc' THEN @StagePassLoadTypeToProc ELSE 0 END
--
--SET @uspName = 'usp_' + @SourceTable
BEGIN TRANSACTION;
BEGIN TRY
SELECT @ServerName = @@SERVERNAME
-- Uncomment this to ensure that this build only happens in the correct place.
--if @ServerName <> @DevServerName
--   begin
--   raiserror( 'This Process may only be run in the Development environment!!', 16, 1)
--   end
IF @StageLoadType NOT IN ('Full', 'Delta') RAISERROR( '%s is not a valid Load Type', 16, 1, @StageLoadType)
/********************************************
Release:
********************************************/
--Find the Next Release for the Sprint
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
EXECUTE @release_key = [$(ConfigDatabase)].[dv_release].[dv_release_master_insert] 
			 @release_number = @release_number -- date of the Sprint Start + ad hoc release number
			,@release_description = @Description -- what the release is for
			,@reference_number = @ReleaseReference
			,@reference_source = @ReleaseSource
 
/********************************************
Link:
********************************************/
-- Configure the Link:
IF @SatelliteOnly = 'N'
BEGIN
	SELECT @abbn = [$(ConfigDatabase)].[dbo].[fn_get_next_abbreviation]()
	EXECUTE @link_key = [$(ConfigDatabase)].[dbo].[dv_link_insert] 
			 @link_name = @LinkName
			,@link_abbreviation = @abbn
			,@link_schema = @link_schema
			,@link_database = @VaultName
			,@is_compressed = @link_is_compressed
			,@is_retired = 0
			,@release_number = @release_number
END
ELSE
BEGIN
	SELECT @link_key = [link_key]
		,@link_database = [link_database]
	FROM [$(ConfigDatabase)].[dbo].[dv_link] 
	WHERE [link_name] = @LinkName
	IF @link_database <> @VaultName
		RAISERROR( 'The Link and Satellite have to exist in the same database', 16, 1)
END
/********************************************
Satellite:
********************************************/
-- Configure the Satellite:
SELECT @abbn = [$(ConfigDatabase)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @satellite_key = [$(ConfigDatabase)].[dbo].[dv_satellite_insert] 
						 @hub_key					= 0 --Dont fill in for a Link
						,@link_key					= @link_key
						,@link_hub_satellite_flag	= 'L'
						,@satellite_name			= @SatelliteName
						,@satellite_abbreviation	= @abbn
						,@satellite_schema			= @sat_schema
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

SELECT @source_table_key = source_table_key 
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

--DECLARE curDerCol CURSOR FOR  
--SELECT left_column_name, right_column_name
--FROM @DerivedColumnsList
--ORDER BY left_column_name

--OPEN curDerCol   
--FETCH NEXT FROM curDerCol INTO @DerColName, @DerColValue  

--WHILE @@FETCH_STATUS = 0   
--BEGIN 

--SELECT @DerColKey = [column_key]
--      ,@DerColType = [column_type]
--	  ,@DerColLength = [column_length]
--	  ,@DerColPrecision = [column_precision]
--	  ,@DerColScale = [column_scale]
--	  ,@DerColCollation = [Collation_Name]
--	  ,@DerColOrdinalPos = [source_ordinal_position]
--FROM [$(ConfigDatabase)].[dbo].[dv_column]
--WHERE [column_key] IN (
--SELECT c.[column_key]
--FROM [$(ConfigDatabase)].[dbo].[dv_source_table] st 
--inner join [$(ConfigDatabase)].[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
--WHERE 1=1
--and st.source_table_key = @source_table_key
--and c.column_name = @DerColName
--)

--EXECUTE [$(ConfigDatabase)].[dbo].[dv_column_update]
--@column_key = @DerColKey
--,@table_key = @source_table_key
--,@satellite_col_key = NULL
--,@column_name = @DerColName
--,@column_type = @DerColType
--,@column_length = @DerColLength
--,@column_precision = @DerColPrecision
--,@column_scale = @DerColScale
--,@Collation_Name = @DerColCollation
--,@is_derived = 1
--,@derived_value = @DerColValue
--,@source_ordinal_position = @DerColOrdinalPos
--,@is_source_date = 0
--,@is_retired = 0

--FETCH NEXT FROM curDerCol INTO @DerColName, @DerColValue   
--END   

--CLOSE curDerCol   
--DEALLOCATE curDerCol

SELECT 'Hook the Source Columns up to the Satellite:'
EXECUTE [$(ConfigDatabase)].[dv_config].[dv_populate_satellite_columns] 
   @vault_source_unique_name	= @StageTable
  ,@vault_satellite_name		= @SatelliteName
  ,@vault_release_number		= @release_number
  ,@vault_rerun_satellite_column_insert = 0
--
/********************************************
Hub Keys:
********************************************/
SELECT 'Hook up each of the Hub Keys:'

DECLARE curLinkHub CURSOR FOR  
SELECT  hub_name
       ,link_key_name = MAX(link_key_name) OVER (PARTITION BY link_key_name, hub_name)
	   ,hub_column_name
	   ,column_name
	FROM @Hub_Key_List
	ORDER BY OrdinalPosition
OPEN curLinkHub   
FETCH NEXT FROM curLinkHub INTO  @curLinkHub_hub_name	   
								,@curLinkHub_link_key_name
								,@curLinkHub_hub_column_name
								,@curLinkHub_column_name
SELECT  @curLinkHub_hub_name	   
	   ,@curLinkHub_link_key_name
	   ,@curLinkHub_hub_column_name
	   ,@curLinkHub_column_name



-- Each Hub == Each Link Key. First loop through the Hubs:
WHILE @@FETCH_STATUS = 0   
BEGIN   
      SET @thisHub = @curLinkHub_hub_name
	  SET @thisLink_Key = @curLinkHub_link_key_name
	  IF @SatelliteOnly = 'N'
			BEGIN
			EXECUTE  @link_key_column_key = [$(ConfigDatabase)].[dbo].[dv_link_key_insert] 
											 @link_key				= @link_key
											,@link_key_column_name  = @curLinkHub_link_key_name
											,@release_number		= @release_number
				END
			ELSE
				BEGIN
					SELECT @link_key_column_key = lkc.link_key_column_key
					FROM [$(ConfigDatabase)].[dbo].[dv_hub_column] hc
					INNER JOIN [$(ConfigDatabase)].[dbo].[dv_link_key_column] lkc	ON lkc.link_key_column_key = hc.link_key_column_key
					INNER JOIN [$(ConfigDatabase)].[dbo].[dv_hub_key_column] hkc	ON hkc.hub_key_column_key = hc.hub_key_column_key
					INNER JOIN [$(ConfigDatabase)].[dbo].[dv_hub] h					ON h.hub_key = hkc.hub_key
					WHERE h.hub_name = @curLinkHub_hub_name
					AND isnull(lkc.link_key_column_name, h.hub_name) = @curLinkHub_column_name
				END
	  -- Now loop through the Columns for the Hub (to deal with Multi Column Hub Keys).
	  WHILE @thisHub = @curLinkHub_hub_name
	    AND @thisLink_Key = @curLinkHub_link_key_name
	    AND @@FETCH_STATUS = 0 
	  BEGIN	
	        SELECT 'bb', @StageTable, @curLinkHub_column_name, * 
			FROM [$(ConfigDatabase)].[dbo].[dv_column] c
			INNER JOIN [$(ConfigDatabase)].[dbo].[dv_source_table] st  ON st.source_table_key  = c.table_key
			WHERE 1=1
			--and st.source_unique_name = @StageTable
			AND c.column_name = @curLinkHub_column_name

	  		SELECT @hub_source_column_key = c.column_key
			FROM [$(ConfigDatabase)].[dbo].[dv_column] c
			INNER JOIN [$(ConfigDatabase)].[dbo].[dv_source_table] st  ON st.source_table_key  = c.table_key
			WHERE st.source_unique_name = @StageTable
			AND c.column_name = @curLinkHub_column_name

			SELECT @hub_key_column_key	= hkc.hub_key_column_key
				  ,@hub_key				= h.hub_key
			FROM [$(ConfigDatabase)].[dbo].[dv_hub] h
			INNER JOIN [$(ConfigDatabase)].[dbo].[dv_hub_key_column] hkc ON hkc.[hub_key] = h.[hub_key]
			WHERE 1=1
			  AND h.hub_name = @curLinkHub_hub_name
			  AND hkc.hub_key_column_name = @curLinkHub_hub_column_name
SELECT  hub_key_column_key	= @hub_key_column_key
	   ,link_key_column_key   = @link_key_column_key
	   ,column_key			= @hub_source_column_key
	   ,release_number		= @release_number

	
			EXECUTE [$(ConfigDatabase)].[dbo].[dv_hub_column_insert] 
				 @hub_key_column_key	= @hub_key_column_key
				,@link_key_column_key   = @link_key_column_key
				,@column_key			= @hub_source_column_key
				,@release_number		= @release_number
			
			FETCH NEXT FROM curLinkHub INTO  @curLinkHub_hub_name	   
											,@curLinkHub_link_key_name
											,@curLinkHub_hub_column_name
											,@curLinkHub_column_name

	  END 
END 
CLOSE curLinkHub   
DEALLOCATE curLinkHub
---------------------------------------------------------------------
SELECT 'Remove the Columns in the Exclude List from the Satellite:'

 SELECT @StageTableKey = REPLACE(REPLACE(column_name, '[', ''), ']','') FROM [$(ConfigDatabase)].[dbo].[fn_get_key_definition] (@StageTable,'stg')
 INSERT INTO @ExcludeColumns VALUES (@StageTableKey)


UPDATE [$(ConfigDatabase)].[dbo].[dv_column]
SET [satellite_col_key] = NULL
WHERE [column_name] IN (
SELECT *
FROM @ExcludeColumns)
AND [column_key] IN (SELECT c.column_key FROM [$(ConfigDatabase)].[dbo].[dv_column] c 
                    INNER JOIN [$(ConfigDatabase)].[dbo].[dv_satellite_column] sc ON sc.[satellite_col_key] = c.[satellite_col_key]
					WHERE sc.[satellite_key] = @satellite_key)
DELETE
FROM [$(ConfigDatabase)].[dbo].[dv_satellite_column]
WHERE [satellite_col_key] IN (
	SELECT sc.[satellite_col_key]
	FROM [$(ConfigDatabase)].[dbo].[dv_satellite_column] sc
	LEFT JOIN [$(ConfigDatabase)].[dbo].[dv_column] c
	ON sc.[satellite_col_key] = c.[satellite_col_key]
	WHERE c.[satellite_col_key] is null
	AND sc.[satellite_key] = @satellite_key)

-- hook the Hub Key up to the Source Column which will populate it:
--EXECUTE [dbo].[dv_hub_column_insert] @hub_key_column_key = @hub_key_column_key
--,@column_key = @hub_source_column_key
--,@release_number = @release_number
--
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
  ,@source_table_load_type		= 'Full' 
  -- Full load here as switching link to load in portions not a good idea due to the nature of the link: 
  --it allows many-to-many relationships and in case of change it won't retire the previous relationship 
  --unless some extra logic is implemented. Feel free to change it back to Delta in case if you have implemented thi logic in your custom stored proc
  ,@priority					= 'Low'
  ,@queue						= 'Agent001'
  ,@release_number				= @release_number
--
/********************************************
Useful Commands:
********************************************/
--Output commands to Build the Tables and test the Load:
SELECT case when @SatelliteOnly = 'N' then 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_link_table] ''' + @VaultName + ''',''' + @LinkName + ''',''N''' else '' end
UNION
SELECT 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_create_sat_table] ''' + @VaultName + ''',''' + @SatelliteName + ''',''N'''
UNION
--SELECT 'EXECUTE [dbo].[dv_create_stage_table] ''' + @StageTable + ''',''Y'''
--UNION
SELECT 'EXECUTE [$(ConfigDatabase)].[dbo].[dv_load_source_table]
 @vault_source_unique_name = ''' + @StageTable + '''
,@vault_source_load_type = ''full'''
UNION
SELECT 'select top 1000 * from ' + quotename(link_database) + '.' + quotename(link_schema) + '.' + quotename([$(ConfigDatabase)].[dbo].[fn_get_object_name] (link_name, 'lnk'))
from [$(ConfigDatabase)].[dbo].[dv_link] where link_name = @LinkName AND link_database = @VaultName
UNION
SELECT 'select top 1000 * from ' + quotename(satellite_database) + '.' + quotename(satellite_schema) + '.' + quotename([$(ConfigDatabase)].[dbo].[fn_get_object_name] (satellite_name, 'sat'))
from [$(ConfigDatabase)].[dbo].[dv_satellite] where satellite_name =  @SatelliteName AND satellite_database = @VaultName
--
PRINT 'succeeded';
-- Commit if successful:
COMMIT;
END TRY
/********************************************
Error Handling:
********************************************/
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