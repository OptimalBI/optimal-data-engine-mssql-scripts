
-------------------------------------------------------------------------------------------
--Populate the following Parameters:
CREATE PROCEDURE [dbo].[ODE_link_sat_config]
--
(
@SatelliteOnly char(1)				--= 'N'
	-- when set to "N", the script will create a Hub and Satellite combination.
	-- "Y" will cause the script to create a Satellite and hook it up to the specified Hub.
,@sprintdate CHAR(8)				--= '20170111'
	-- Start Date of the current Sprint in Integer yyyymmdd (this depends on having set up a Sprint Release with the key yyymmdd00
	-- e.g. EXECUTE [dv_release].[dv_release_master_insert] 2016080100, 'Test Sprint release', 'US001', 'Jira'
,@ReleaseReference VARCHAR(50)		--= 'HR-309'
	-- User Story and/or Task numbers for the Satellite which you are building.
,@ReleaseSource VARCHAR(50)			--= 'Jira'
	-- system the reference number refers to, Rally
,@StageDatabase VARCHAR(128)		--= 'ode_stage_MAGODE_40'
	-- the name of the Stage Database which holds the table on which the Load will be modelled (this Stage table  needs to exist. The script will use the meta data of the table to build the required Configuration in ODE)
	-- To check, select * from ODE_Config.dbo.dv_source_table where table_name = 'YourSourceTableName' eg. 'Adventureworks__Sales__SalesOrderHeader'
,@StageSchema VARCHAR(128)			--= 'Stage'
,@StageTable VARCHAR(128)			--= 'link_Sale'
,@StageSourceType VARCHAR(50)		--= 'BespokeProc', 'ExternalStage', 'LeftRightComparison'
,@StageLoadType VARCHAR(50)         -- 'Full' or 'Delta'
,@SourceSystemName VARCHAR(128) = NULL
,@StagePassLoadTypeToProc BIT		-- 0 = Dont Pass it on, 1 = Pass Delta / Full to Proc
,@LinkName VARCHAR(128)				--= 'Sale_Customer_Order'
	-- For completely Raw Links, you can leave this column as null. The Script will create a Link using the same name as the source table.
	-- For Business Links, specify the name of the Link in the Ensemble.
,@SatelliteName VARCHAR(128)		--= 'link_Sale_Customer_Order'
,@VaultName VARCHAR(128)			--=  'ode_vault_MAGODE_40'
	--the name of the vault where the Hub and Satellite will be created.
,@ScheduleName VARCHAR(128)			--=  'Full_load'
	--the schedule the load is to run in. This schedule needs to exist prior to running this script.
--EXECUTE [dv_scheduler].[dv_schedule_insert] 'Link_Schedule', 'For Testing Purposes', 'Ad Hoc', 0
,@Hub_key_list [dbo].[dv_link_detail_list] READONLY
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
	-- Note that Columnstore is only available in SQL Server Enterprise Edition.
,@duplicate_removal_threshold INT = 0
,@DevServerName SYSNAME				= 'Ignore'
	-- You can provide a Server Name here to prevent accidentally creating keys and objects in the wrong environment.
,@BusinessVaultName VARCHAR(128)	= 'Ignore'
	-- You can provide a name here to cause the Business key to be excluded from the Sat, in a specific Vault.
DECLARE @ExcludeColumns TABLE (ColumnName VARCHAR(128))
INSERT @ExcludeColumns  VALUES ('dv_stage_datetime')
	--Insert columns which should never be included in the satellites.
	-- Exclude the Hub Key from the Satellite if it is in Business Vault. Otherwise keep it.

/********************************************
Begin:
********************************************/

select @LinkName = case when isnull(@LinkName, '') = '' then @StageTable else @LinkName end
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
,@ServerName					SYSNAME
,@source_table_key				INT
,@source_version_key			INT
,@source_procedure_name         varchar(128) = case when @StageSourceType = 'BespokeProc' then 'usp_' + @StageTable else NULL end
,@pass_load_type_to_proc		BIT = case when @StageLoadType = 'BespokeProc' then @StagePassLoadTypeToProc else 0 end
--
--SET @uspName = 'usp_' + @SourceTable
BEGIN TRANSACTION;
BEGIN TRY
select @ServerName = @@servername
-- Uncomment this to ensure that this build only happens in the correct place.
--if @ServerName <> @DevServerName
--   begin
--   raiserror( 'This Process may only be run in the Development environment!!', 16, 1)
--   end
if @StageLoadType not in ('Full', 'Delta') raiserror( '%s is not a valid Load Type', 16, 1, @StageLoadType)
/********************************************
Release:
********************************************/
--Find the Next Release for the Sprint
SELECT TOP 1 @seqint = cast(right(cast([release_number] AS VARCHAR(100)), len(cast([release_number] AS VARCHAR(100))) - 8) AS INT)
FROM [ODE_Config].[dv_release].[dv_release_master]
WHERE left(cast([release_number] AS VARCHAR(100)), 8) = @sprintdate
ORDER BY 1 DESC
IF @@rowcount = 0
SET @release_number = cast(@sprintdate + '01' AS INT)
ELSE
SET @release_number = cast(@sprintdate + right('00' + cast(@seqint + 1 AS VARCHAR(100)), 2) AS INT)
SELECT @release_number
SET @Description = 'Load Stage Table: ' + quotename(@StageTable) + ' into ' + quotename(@VaultName)
-- Create the Release:
EXECUTE @release_key = [ODE_Config].[dv_release].[dv_release_master_insert] 
			 @release_number = @release_number -- date of the Sprint Start + ad hoc release number
			,@release_description = @Description -- what the release is for
			,@reference_number = @ReleaseReference
			,@reference_source = @ReleaseSource
 
/********************************************
Link:
********************************************/
-- Configure the Link:
if @SatelliteOnly = 'N'
begin
SELECT @abbn = [ODE_Config].[dbo].[fn_get_next_abbreviation]()
EXECUTE @link_key = [ODE_Config].[dbo].[dv_link_insert] 
			 @link_name = @LinkName
			,@link_abbreviation = @abbn
			,@link_schema = 'lnk'
			,@link_database = @VaultName
			,@is_compressed = @link_is_compressed
			,@is_retired = 0
			,@release_number = @release_number
end
else
begin
select @link_key = [link_key]
	  ,@link_database = [link_database]
from [ODE_Config].[dbo].[dv_link] where [link_name] = @LinkName
if @link_database <> @VaultName
begin
raiserror( 'The Link and Satellite have to exist in the same database', 16, 1)
end
end
/********************************************
Satellite:
********************************************/
-- Configure the Satellite:
SELECT @abbn = [ODE_Config].[dbo].[fn_get_next_abbreviation]()
EXECUTE @satellite_key = [ODE_Config].[dbo].[dv_satellite_insert] 
						 @hub_key					= 0 --Dont fill in for a Hub
						,@link_key					= @link_key
						,@link_hub_satellite_flag	= 'L'
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
EXECUTE [ODE_Config].[dv_config].[dv_populate_source_table_columns] 
   @vault_stage_database		= @StageDatabase
  ,@vault_stage_schema			= @StageSchema
  ,@vault_stage_table			= @StageTable
  ,@vault_source_unique_name	= @StageTable
  ,@vault_source_type			= @StageSourceType
  ,@vault_stage_table_load_type = @StageLoadType
  ,@vault_source_system_name	= @SourceSystemName
  ,@vault_release_number		= @release_number
  ,@vault_rerun_column_insert	= 0
select @source_table_key = source_table_key from [ODE_Config].[dbo].[dv_source_table] where [source_unique_name] = @StageTable
select * from [ODE_Config].dbo.dv_column where table_key = @source_table_key
-- Add a Current Source Version with a "Version" of 1 
EXECUTE @source_version_key = [ODE_Config].[dbo].[dv_source_version_insert] 
   @source_table_key		= @source_table_key
  ,@source_version			= 1
  ,@source_procedure_name   = @source_procedure_name
  ,@pass_load_type_to_proc	= @pass_load_type_to_proc
  ,@is_current				= 1
  ,@release_number			= @release_number
SELECT 'Hook the Source Columns up to the Satellite:'
EXECUTE [ODE_Config].[dv_config].[dv_populate_satellite_columns] 
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
       ,link_key_name = max(link_key_name) over (partition by link_key_name, hub_name)
	   ,hub_column_name
	   ,column_name
	FROM @Hub_Key_List
	order by OrdinalPosition
OPEN curLinkHub   
FETCH NEXT FROM curLinkHub INTO  @curLinkHub_hub_name	   
								,@curLinkHub_link_key_name
								,@curLinkHub_hub_column_name
								,@curLinkHub_column_name
select  @curLinkHub_hub_name	   
	   ,@curLinkHub_link_key_name
	   ,@curLinkHub_hub_column_name
	   ,@curLinkHub_column_name



-- Each Hub == Each Link Key. First loop through the Hubs:
WHILE @@FETCH_STATUS = 0   
BEGIN   
      set @thisHub = @curLinkHub_hub_name
	  set @thisLink_Key = @curLinkHub_link_key_name
	  if @SatelliteOnly = 'N'
			begin
			EXECUTE  @link_key_column_key = [ODE_Config].[dbo].[dv_link_key_insert] 
											 @link_key				= @link_key
											,@link_key_column_name  = @curLinkHub_link_key_name
											,@release_number		= @release_number
				end
			else
				begin
					select @link_key_column_key = lkc.link_key_column_key
					from [ODE_Config].[dbo].[dv_hub_column] hc
					inner join [ODE_Config].[dbo].[dv_link_key_column] lkc	on lkc.link_key_column_key = hc.link_key_column_key
					inner join [ODE_Config].[dbo].[dv_hub_key_column] hkc	on hkc.hub_key_column_key = hc.hub_key_column_key
					inner join [ODE_Config].[dbo].[dv_hub] h					on h.hub_key = hkc.hub_key
					where h.hub_name = @curLinkHub_hub_name
					and isnull(lkc.link_key_column_name, h.hub_name) = @curLinkHub_column_name
				end
	  -- Now loop through the Columns for the Hub (to deal with Multi Column Hub Keys).
	  WHILE @thisHub = @curLinkHub_hub_name
	    and @thisLink_Key = @curLinkHub_link_key_name
	    and @@FETCH_STATUS = 0 
	  BEGIN	
	  --      select 'bb', @StageTable, @curLinkHub_column_name, * from [ODE_Config].[dbo].[dv_column] c
			--inner join [ODE_Config].[dbo].[dv_source_table] st  on st.source_table_key  = c.table_key
			--where 1=1
			----and st.source_unique_name = @StageTable
			--and c.column_name = @curLinkHub_column_name

	  		select @hub_source_column_key = c.column_key
			from [ODE_Config].[dbo].[dv_column] c
			inner join [ODE_Config].[dbo].[dv_source_table] st  on st.source_table_key  = c.table_key
			where st.source_unique_name = @StageTable
			and c.column_name = @curLinkHub_column_name

			select @hub_key_column_key	= hkc.hub_key_column_key
				  ,@hub_key				= h.hub_key
			from [ODE_Config].[dbo].[dv_hub] h
			inner join [ODE_Config].[dbo].[dv_hub_key_column] hkc on hkc.[hub_key] = h.[hub_key]
			where 1=1
			  and h.hub_name = @curLinkHub_hub_name
			  and hkc.hub_key_column_name = @curLinkHub_hub_column_name
select  hub_key_column_key	= @hub_key_column_key
	   ,link_key_column_key = @link_key_column_key
	   ,column_key			= @hub_source_column_key
	   ,release_number		= @release_number
	   ,curLinkHub_hub_name = @curLinkHub_hub_name
	   ,curLinkHub_hub_column_name = @curLinkHub_hub_column_name

			EXECUTE [ODE_Config].[dbo].[dv_hub_column_insert] 
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
--
SELECT 'Remove the Columns in the Exclude List from the Satellite:'

update [ODE_Config].[dbo].[dv_column]
set [satellite_col_key] = NULL
where [column_name] IN (
SELECT *
FROM @ExcludeColumns)
and [column_key] in(select c.column_key from [ODE_Config].[dbo].[dv_column] c 
                    inner join [ODE_Config].[dbo].[dv_satellite_column] sc on sc.[satellite_col_key] = c.[satellite_col_key]
					where sc.[satellite_key] = @satellite_key)
DELETE
FROM [ODE_Config].[dbo].[dv_satellite_column]
WHERE [satellite_col_key] IN (
	select sc.[satellite_col_key]
	from [ODE_Config].[dbo].[dv_satellite_column] sc
	left join [ODE_Config].[dbo].[dv_column] c
	on sc.[satellite_col_key] = c.[satellite_col_key]
	where c.[satellite_col_key] is null
	and sc.[satellite_key] = @satellite_key)

-- hook the Hub Key up to the Source Column which will populate it:
--EXECUTE [dbo].[dv_hub_column_insert] @hub_key_column_key = @hub_key_column_key
--,@column_key = @hub_source_column_key
--,@release_number = @release_number
--
/********************************************
Scheduler:
********************************************/
-- Add the Source the the required Schedule:
EXECUTE [ODE_Config].[dv_scheduler].[dv_schedule_source_table_insert] 
   @schedule_name				= @ScheduleName
  ,@source_unique_name			= @StageTable
  ,@source_table_load_type		= 'Full'
  ,@priority					= 'Low'
  ,@queue						= '001'
  ,@release_number				= @release_number
--
/********************************************
Useful Commands:
********************************************/
--Output commands to Build the Tables and test the Load:
SELECT case when @SatelliteOnly = 'N' then 'EXECUTE [dbo].[dv_create_link_table] ''' + @VaultName + ''',''' + @LinkName + ''',''N''' else '' end
UNION
SELECT 'EXECUTE [dbo].[dv_create_sat_table] ''' + @VaultName + ''',''' + @SatelliteName + ''',''N'''
UNION
SELECT 'EXECUTE [dbo].[dv_load_source_table]
 @vault_source_unique_name = ''' + @StageTable + '''
,@vault_source_load_type = ''full'''
UNION
SELECT 'select top 1000 * from ' + quotename(link_database) + '.' + quotename(link_schema) + '.' + quotename([ODE_Config].[dbo].[fn_get_object_name] (link_name, 'lnk'))
from [ODE_Config].[dbo].[dv_link] where link_name = @LinkName
UNION
SELECT 'select top 1000 * from ' + quotename(satellite_database) + '.' + quotename(satellite_schema) + '.' + quotename([ODE_Config].[dbo].[fn_get_object_name] (satellite_name, 'sat'))
from [ODE_Config].[dbo].[dv_satellite] where satellite_name =  @SatelliteName
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