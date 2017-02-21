USE [ODE_Config]
GO

-------------------------------------------------------------------------------------------
DECLARE
 @SatelliteOnly char(1)				= 'Y'
	-- when set to "N", the script will create a Hub and Satellite combination.
	-- "Y" will cause the script to create a Satellite and hook it up to the specified Hub.
,@SprintDate CHAR(8)				= '20170131'
	-- Start Date of the current Sprint in Integer yyyymmdd (this depends on having set up a Sprint Release with the key yyymmdd00
	-- e.g. EXECUTE [dv_release].[dv_release_master_insert] 2016080100, 'Test Sprint release', 'US001', 'Jira'
,@ReleaseReference VARCHAR(50)		= 'HR-538'
	-- User Story and/or Task numbers for the Satellite which you are building.
,@ReleaseSource VARCHAR(50)			= 'Jira'
	-- system the reference number refers to, Rally, Jira etc.
,@SourceSystemName VARCHAR(128)		= 'Sales'
	-- Optional; Documentation purposes only in case if data from multiple sources get loaded to the same stage area
,@StageDatabase VARCHAR(128)		= 'ODE_Stage'
	-- the name of the Stage Database which holds the table on which the Load will be modelled (this Stage table  needs to exist. The script will use the meta data of the table to build the required Configuration in ODE)
	-- To check, select * from ODE_Config.dbo.dv_source_table where table_name = 'YourSourceTableName' eg. 'Adventureworks__Sales__SalesOrderHeader'
,@StageSchema VARCHAR(128)			= 'Stage'
,@StageTable VARCHAR(128)			= 'Sales_CustomerName'
,@StageSourceType VARCHAR(50)		= 'BespokeProc'--, 'ExternalStage', 'LeftRightComparison'
,@StageLoadType VARCHAR(50)         = 'Full' --or 'Delta'
,@StagePassLoadTypeToProc BIT		= 0--- = Dont Pass it on, 1 = Pass Delta / Full to Proc
,@HubName VARCHAR(128)				= 'Customer'  --'Customer'--NULL -- to Default the Hub Name to the sat name - good for pure Raw Vault.
	-- For completely Raw Hub Sat combinations, you can leave this column as null. The Script will create a Hub using the same name as the source table.
	-- For Business hubs, specify the name of the Hub of the Ensemble, which you are adding to.
,@SatelliteName VARCHAR(128)		= 'Sales_CustomerName'

,@VaultName VARCHAR(128)            =  'ODE_Vault'
	--the name of the vault where the Hub and Satellite will be created.
,@ScheduleName VARCHAR(128)			=  'Full_load'
	--the schedule the load is to run in. This schedule needs to exist prior to running this script.
--,@HubKeyNames	[dbo].[dv_column_list] READONLY
declare @HubKeyNames table (column_name VARCHAR(128)
                          ,ordinal_position INT IDENTITY (1,1)) 
insert @HubKeyNames values('Customer_KeyType')
                         ,('CustomerID') 
						 --,('dv_match_row')  
----The name of the unique Key columns. The columns need to exist in your Stage Table, and should be appropriately named for the Hub, which you are building.
-- List the Columns in the order in which you want them to appear in the Hub.
declare @MatchingLeftObjectType		VARCHAR(50)  = NULL --'hub','lnk', 'sat', 'stg'
,@MatchingLeftObjectDatabase	VARCHAR(128) = NULL
,@MatchingLeftObjectSchema		VARCHAR(128) = NULL
,@MatchingLeftObjectName		VARCHAR(128) = NULL
,@MatchingTemporalPitLeft		DATETIMEOFFSET(7) = NULL

,@MatchingRightObjectType		VARCHAR(50)  = NULL --'hub','lnk', 'sat', 'stg'
,@MatchingRightObjectDatabase	VARCHAR(128) = NULL
,@MatchingRightObjectSchema		VARCHAR(128) = NULL
,@MatchingRightObjectName		VARCHAR(128) = NULL
,@MatchingTemporalPitRight		DATETIMEOFFSET(7) = NULL

,@ColumnMatching [dbo].[dv_column_matching_list] 
-- This Table is only used for setting up a match - @StageSourceType = 'LeftRightComparison'



SET NOCOUNT ON

select * from @HubKeyNames
/********************************************
Defaults:
********************************************/
DECLARE
 @sat_is_columnstore  BIT = 1
,@sat_is_compressed   BIT = 0
,@hub_is_compressed   BIT = 1
	-- Note that Columnstore is only available in SQL Server Enterprise Edition.
,@duplicate_removal_threshold INT = 0
,@DevServerName SYSNAME				= 'Ignore'
	-- You can provide a Server Name here to prevent accidentally creating keys and objects in the wrong environment.
,@BusinessVaultName VARCHAR(128)	= 'Ignore'
	-- You can provide a name here to cause the Business key to be excluded from the Sat, in a specific Vault.
DECLARE @ExcludeColumns TABLE (ColumnName VARCHAR(128))
INSERT @ExcludeColumns  VALUES ('dv_stage_datetime')
	--Insert columns which should never be included in the satellites.
--print 'begin'
/********************************************
Begin:
********************************************/

-- Exclude the Hub Key from the Satellite if it is in Business Vault. Otherwise keep it.

select 1 from [ode_config].[dbo].[dv_stage_database] sd
inner join [ode_config].[dbo].[dv_stage_schema] ss on ss.[stage_database_key] = sd.[stage_database_key]
where sd.[stage_database_name] = @StageDatabase
and ss.[stage_schema_name] = @StageSchema
if @@ROWCOUNT <> 1 raiserror( 'Stage Database %s or Stage Schema %s does not exist', 16, 1, @StageDatabase, @StageSchema)
if @VaultName =  @BusinessVaultName
INSERT @ExcludeColumns select column_name from @HubKeyNames
select @HubName = case when isnull(@HubName, '') = '' then @StageTable else @HubName end
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
,@OrdinalPosition				INT
,@source_procedure_name         varchar(128) = case when @StageSourceType = 'BespokeProc' then 'usp_' + @StageTable else NULL end
,@pass_load_type_to_proc		BIT = case when @StageSourceType = 'BespokeProc' then @StagePassLoadTypeToProc else 0 end

select @StagePassLoadTypeToProc, @pass_load_type_to_proc
BEGIN TRANSACTION;
BEGIN TRY
select @ServerName = @@servername

-- Uncomment this to ensure that this build only happens in the correct place.
--if @ServerName <> @DevServerName
--   begin
--   raiserror( 'This Process may only be run in the Development environment!!', 16, 1)
--   end
--

if @StageLoadType not in ('Full', 'Delta') raiserror( '%s is not a valid Load Type', 16, 1, @StageLoadType)
/********************************************
Release:
********************************************/
--'Find the Next Release for the Sprint'
SELECT TOP 1 @seqint = cast(right(cast([release_number] AS VARCHAR(100)), len(cast([release_number] AS VARCHAR(100))) - 8) AS INT)
FROM [ode_config].[dv_release].[dv_release_master]
WHERE left(cast([release_number] AS VARCHAR(100)), 8) = @sprintdate
ORDER BY 1 DESC
IF @@rowcount = 0
SET @release_number = cast(@sprintdate + '01' AS INT)
ELSE
SET @release_number = cast(@sprintdate + right('00' + cast(@seqint + 1 AS VARCHAR(100)), 2) AS INT)
SELECT @release_number
SET @Description = 'Load Stage Table: ' + quotename(@StageTable) + ' into ' + quotename(@VaultName)
-- Create the Release:
EXECUTE  @release_key = [ode_config].[dv_release].[dv_release_master_insert]  @release_number		= @release_number	-- date of the Sprint Start + ad hoc release number
																,@release_description	= @Description		-- what the release is for
																,@reference_number		= @ReleaseReference
																,@reference_source		= @ReleaseSource
--
/********************************************
Hub:
********************************************/
-- Configure the Hub:
if @SatelliteOnly = 'N'
begin
SELECT @abbn = [ode_config].[dbo].[fn_get_next_abbreviation]()
EXECUTE @hub_key = [ode_config].[dbo].[dv_hub_insert] 
				   @hub_name = @HubName
				  ,@hub_abbreviation = @abbn
				  ,@hub_schema = 'hub'
				  ,@hub_database = @VaultName
				  ,@is_compressed = @hub_is_compressed
				  ,@is_retired = 0
				  ,@release_number = @release_number
end				  
else
begin
select @hub_key			= [hub_key]
      ,@hub_database	= [hub_database]
from [ode_config].[dbo].[dv_hub] 
where [hub_name] = @HubName
if @hub_database <> @VaultName
begin
raiserror( 'The Hub and Satellite have to exist in the same database', 16, 1)
end
end
--
/********************************************
Satellite:
********************************************/
-- Configure the Satellite:
SELECT @abbn = [ode_config].[dbo].[fn_get_next_abbreviation]()
EXECUTE @satellite_key = [ode_config].[dbo].[dv_satellite_insert] 
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
EXECUTE [ode_config].[dv_config].[dv_populate_source_table_columns] 
   @vault_stage_database		= @StageDatabase
  ,@vault_stage_schema			= @StageSchema
  ,@vault_stage_table			= @StageTable
  ,@vault_source_unique_name	= @StageTable
  ,@vault_source_type			= @StageSourceType
  ,@vault_stage_table_load_type = @StageLoadType
  ,@vault_source_system_name	= @SourceSystemName
  ,@vault_release_number		= @release_number
  ,@vault_rerun_column_insert	= 0
select @source_table_key = source_table_key from [ode_config].[dbo].[dv_source_table] where [source_unique_name] = @StageTable
-- Add a Current Source Version with a "Version" of 1 

EXECUTE @source_version_key = [ode_config].[dbo].[dv_source_version_insert] 
   @source_table_key		= @source_table_key
  ,@source_version			= 1
  ,@source_procedure_name   = @source_procedure_name
  ,@pass_load_type_to_proc	= @pass_load_type_to_proc
  ,@is_current				= 1
  ,@release_number			= @release_number

-- If it is a "LeftRight" Match:

IF @StageSourceType = 'LeftRightComparison'
			/********************************************
			Left Right Match
			********************************************/
			BEGIN
			select 'hook up the left right columns for matching'
			EXECUTE @match_key = [ode_config].[dbo].[dv_object_match_insert] 
			   @source_version_key = @source_version_key
			  ,@temporal_pit_left = @MatchingTemporalPitLeft
			  ,@temporal_pit_right = @MatchingTemporalPitRight
			  ,@is_retired = 0
			  ,@release_number = @release_number

			DECLARE curMatchKey CURSOR FOR  
					SELECT [left_column_name], [right_column_name] from @ColumnMatching

			OPEN curMatchKey   
			FETCH NEXT FROM curMatchKey INTO @left_column_name, @right_column_name  
			
			WHILE @@FETCH_STATUS = 0   
			BEGIN 
			set @left_hub_key_column_key	= NULL
			set @left_link_key_column_key	= NULL
			set @left_satellite_col_key		= NULL
			set @left_column_key			= NULL
			set @right_hub_key_column_key	= NULL
			set @right_link_key_column_key	= NULL
			set @right_satellite_col_key	= NULL
			set @right_column_key			= NULL

			-- Get Key for the Left Object
			IF @MatchingLeftObjectType = 'hub'
			begin
			--select @MatchingLeftObjectDatabase
			--	   ,@MatchingLeftObjectSchema
			--	   ,@MatchingLeftObjectName
			--	   ,@left_column_name
				select @left_hub_key_column_key = hub_key_column_key
				from [ode_config].[dbo].[dv_hub] h
				inner join [ode_config].[dbo].[dv_hub_key_column] hkc on hkc.hub_key = h.hub_key
				where h.[hub_database]			= @MatchingLeftObjectDatabase
				  and h.[hub_schema]			= @MatchingLeftObjectSchema
				  and h.[hub_name]				= @MatchingLeftObjectName
				  and hkc.[hub_key_column_name] = @left_column_name
				if @@ROWCOUNT <> 1 raiserror( 'Left Matching %s Column %s not found in Config', 16, 1, @MatchingLeftObjectType,@left_column_name)
			end
			else IF @MatchingLeftObjectType = 'lnk'
			begin
				select @left_link_key_column_key = link_key_column_key
				from [ode_config].[dbo].[dv_link] l
				inner join [ode_config].[dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
				where l.[link_database]			 = @MatchingLeftObjectDatabase
				  and l.[link_schema]			 = @MatchingLeftObjectSchema
				  and l.[link_name]				 = @MatchingLeftObjectName
				  and lkc.[link_key_column_name] = @left_column_name
				if @@ROWCOUNT <> 1 raiserror( 'Left Matching %s Column %s not found in Config', 16, 1, @MatchingLeftObjectType,@left_column_name)
			end
			else IF @MatchingLeftObjectType = 'sat'
			begin 
			
				select @left_satellite_col_key = satellite_col_key
				from [ode_config].[dbo].[dv_satellite] s
				inner join [ode_config].[dbo].[dv_satellite_column] sc on sc.satellite_key = s.satellite_key
				where s.[satellite_database]		 = @MatchingLeftObjectDatabase
				  and s.[satellite_schema]			 = @MatchingLeftObjectSchema
				  and s.[satellite_name]			 = @MatchingLeftObjectName
				  and sc.[column_name]				 = @left_column_name
				
				if @@ROWCOUNT <> 1 raiserror( 'Left Matching %s Column %s not found in Config', 16, 1, @MatchingLeftObjectType,@left_column_name)
			end
			else IF @MatchingLeftObjectType = 'stg'
			begin
			--select @MatchingLeftObjectDatabase
			--	  , @MatchingLeftObjectSchema
			--	  , @MatchingLeftObjectName
			--	  , @left_column_name
				select @left_column_key = column_key
				from [ode_config].[dbo].[dv_source_table] st
				inner join [ode_config].[dbo].[dv_stage_schema] ss on ss.[stage_schema_key] = st.[stage_schema_key]
				inner join [ode_config].[dbo].[dv_stage_database] sd on sd.[stage_database_key] = ss.[stage_database_key]
				inner join [ode_config].[dbo].[dv_column] c on c.table_key = st.source_table_key
				where sd.[stage_database_name]		 = @MatchingLeftObjectDatabase
				  and ss.[stage_schema_name]		 = @MatchingLeftObjectSchema
				  and st.[stage_table_name]			 = @MatchingLeftObjectName
				  and c.[column_name]				 = @left_column_name
				if @@ROWCOUNT <> 1 raiserror( 'Left Matching %s Column %s not found in Config', 16, 1, @MatchingLeftObjectType,@left_column_name)
			end
			else raiserror( 'Left Matching %s is not a valid Matching Object', 16, 1, @MatchingLeftObjectType)

			-- Get Key for the Right Object
			IF @MatchingrightObjectType = 'hub'
			begin
				select @right_hub_key_column_key = hub_key_column_key
				from [ode_config].[dbo].[dv_hub] h
				inner join [ode_config].[dbo].[dv_hub_key_column] hkc on hkc.hub_key = h.hub_key
				where h.[hub_database]			= @MatchingrightObjectDatabase
				  and h.[hub_schema]			= @MatchingrightObjectSchema
				  and h.[hub_name]				= @MatchingrightObjectName
				  and hkc.[hub_key_column_name] = @right_column_name
				if @@ROWCOUNT <> 1 raiserror( 'right Matching %s Column %s not found in Config', 16, 1, @MatchingrightObjectType,@right_column_name)
			end
			else IF @MatchingrightObjectType = 'lnk'
			begin
				select @right_link_key_column_key = link_key_column_key
				from [ode_config].[dbo].[dv_link] l
				inner join [ode_config].[dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
				where l.[link_database]			 = @MatchingrightObjectDatabase
				  and l.[link_schema]			 = @MatchingrightObjectSchema
				  and l.[link_name]				 = @MatchingrightObjectName
				  and lkc.[link_key_column_name] = @right_column_name
				if @@ROWCOUNT <> 1 raiserror( 'right Matching %s Column %s not found in Config', 16, 1, @MatchingrightObjectType,@right_column_name)
			end
			else IF @MatchingrightObjectType = 'sat'
			begin
				select @right_satellite_col_key = satellite_col_key
				from [ode_config].[dbo].[dv_satellite] s
				inner join [ode_config].[dbo].[dv_satellite_column] sc on sc.satellite_key = s.satellite_key
				where s.[satellite_database]		 = @MatchingrightObjectDatabase
				  and s.[satellite_schema]			 = @MatchingrightObjectSchema
				  and s.[satellite_name]			 = @MatchingrightObjectName
				  and sc.[column_name]				 = @right_column_name
				if @@ROWCOUNT <> 1 raiserror( 'right Matching %s Column %s not found in Config', 16, 1, @MatchingrightObjectType,@right_column_name)
			end
			else IF @MatchingrightObjectType = 'stg'
			begin
				select @right_column_key = column_key
				from [ode_config].[dbo].[dv_source_table] st
				inner join [ode_config].[dbo].[dv_stage_schema] ss on ss.[stage_schema_key] = st.[stage_schema_key]
				inner join [ode_config].[dbo].[dv_stage_database] sd on sd.[stage_database_key] = ss.[stage_database_key]
				inner join [ode_config].[dbo].[dv_column] c on c.table_key = st.source_table_key
				where sd.[stage_database_name]		 = @MatchingrightObjectDatabase
				  and ss.[stage_schema_name]		 = @MatchingrightObjectSchema
				  and st.[stage_table_name]			 = @MatchingrightObjectName
				  and c.[column_name]				 = @right_column_name
				if @@ROWCOUNT <> 1 raiserror( 'right Matching %s Column %s not found in Config', 16, 1, @MatchingrightObjectType,@right_column_name)
			end
			else raiserror( 'right Matching %s is not a valid Matching Object', 16, 1, @MatchingrightObjectType)

			EXECUTE [ode_config].[dbo].[dv_column_match_insert] 
					 @match_key					= @match_key
					,@left_hub_key_column_key	= @left_hub_key_column_key
					,@left_link_key_column_key	= @left_link_key_column_key
					,@left_satellite_col_key	= @left_satellite_col_key	
					,@left_column_key			= @left_column_key			
					,@right_hub_key_column_key	= @right_hub_key_column_key	
					,@right_link_key_column_key	= @right_link_key_column_key	
					,@right_satellite_col_key	= @right_satellite_col_key	
					,@right_column_key			= @right_column_key			
					,@release_number			= @release_number
			FETCH NEXT FROM curMatchKey INTO @left_column_name, @right_column_name
			END
			CLOSE curMatchKey   
			DEALLOCATE curMatchKey

			END
			/********************************************
			End of Left Right Match
			********************************************/
--
SELECT 'Hook the Source Columns up to the Satellite:'
EXECUTE [ode_config].[dv_config].[dv_populate_satellite_columns] 
   @vault_source_unique_name	= @StageTable
  ,@vault_satellite_name		= @SatelliteName
  ,@vault_release_number		= @release_number
  ,@vault_rerun_satellite_column_insert = 0
--
/********************************************/
select 'Hub Key:'
/********************************************/

DECLARE curHubKey CURSOR FOR  
SELECT column_name, ordinal_position
FROM @HubKeyNames
ORDER BY ordinal_position

OPEN curHubKey   
FETCH NEXT FROM curHubKey INTO @HubKeyName, @OrdinalPosition  

WHILE @@FETCH_STATUS = 0   
BEGIN 
select 'hello;' ,* from [ode_config].[dbo].[dv_column] where table_key = @source_table_key
select @HubKeyName, @OrdinalPosition, @source_table_key 
-- Create the Hub Key based on the Source Column:
SELECT @hub_key_column_type			= 'varchar'	--[column_type]
	  ,@hub_key_column_length		= 128		--[column_length]
	  ,@hub_key_column_precision	= 0			--[column_precision]
	  ,@hub_key_column_scale		= 0			--[column_scale]
	  ,@hub_key_Collation_Name	    = null		--[Collation_Name]
      ,@hub_source_column_key		= [column_key]
FROM [ode_config].[dbo].[dv_column] c
WHERE [column_key] IN (
SELECT c.[column_key]
FROM [ode_config].[dbo].[dv_source_table] st 
inner join [ode_config].[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE 1=1
and st.source_table_key = @source_table_key
and c.column_name = @HubKeyName
)


SELECT *
FROM [ode_config].[dbo].[dv_column] c
WHERE [column_key] IN (
SELECT c.[column_key]
FROM [ode_config].[dbo].[dv_source_table] st 
inner join [ode_config].[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE 1=1
and st.source_table_key = @source_table_key
--and c.column_name = @HubKeyName
)




--
if @SatelliteOnly = 'N'
begin

	select @hub_key
		   ,@HubKeyName
		   ,@hub_key_column_type
		   ,@hub_key_column_length
		   ,@hub_key_column_precision
		   ,@hub_key_column_scale
		   ,@hub_key_Collation_Name
		   ,@OrdinalPosition
		   ,@release_number
	
	EXECUTE @hub_key_column_key = [ode_config].[dbo].[dv_hub_key_insert] 
								 @hub_key					= @hub_key
								,@hub_key_column_name		= @HubKeyName
								,@hub_key_column_type		= @hub_key_column_type
								,@hub_key_column_length		= @hub_key_column_length
								,@hub_key_column_precision	= @hub_key_column_precision
								,@hub_key_column_scale		= @hub_key_column_scale
								,@hub_key_Collation_Name	= @hub_key_Collation_Name
								,@hub_key_ordinal_position	= @OrdinalPosition
								,@release_number			= @release_number
end
else
begin
	select @hub_key_column_key = [hub_key_column_key]
	from [ode_config].[dbo].[dv_hub_key_column]
	where [hub_key] = @hub_key
	and [hub_key_column_name] = @HubKeyName
	--if @@rowcount > 1
	--begin
	--raiserror( 'This script does not deal with multi part Hub Keys. ', 16, 1)
	--end
end
-- hook the Hub Key up to the Source Column which will populate it:
select  hub_key_column_key		 = @hub_key_column_key
	   ,hub_source_column_key	 = @hub_source_column_key
	   ,HubKeyName				 = @HubKeyName

EXECUTE [ode_config].[dbo].[dv_hub_column_insert] 
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
-- Remove the Columns in the Exclude List from the Satellite:
update [ode_config].[dbo].[dv_column]
set [satellite_col_key] = NULL
where [column_name] IN (
SELECT *
FROM @ExcludeColumns)
and [column_key] in(select c.column_key from [ode_config].[dbo].[dv_column] c 
                    inner join [ode_config].[dbo].[dv_satellite_column] sc on sc.[satellite_col_key] = c.[satellite_col_key]
					where sc.[satellite_key] = @satellite_key)

-- If you don't want Keys in the satellites:
	DELETE
	FROM [ode_config].[dbo].[dv_satellite_column]
	WHERE [satellite_col_key] IN (
		select sc.[satellite_col_key]
		from [ode_config].[dbo].[dv_satellite_column] sc
		left join [ode_config].[dbo].[dv_column] c	on sc.[satellite_col_key] = c.[satellite_col_key]
		where c.[satellite_col_key] is null
		  and sc.[satellite_key] = @satellite_key
		  )
/********************************************
Scheduler:
********************************************/
-- Add the Source the the required Schedule:
EXECUTE [ode_config].[dv_scheduler].[dv_schedule_source_table_insert] 
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
SELECT case when @SatelliteOnly = 'N' then 'EXECUTE [dbo].[dv_create_hub_table] ''' + @VaultName + ''',''' + @HubName + ''',''N''' else '' end
UNION
SELECT 'EXECUTE [dbo].[dv_create_sat_table] ''' + @VaultName + ''',''' + @SatelliteName + ''',''N'''
UNION
SELECT 'EXECUTE [dbo].[dv_load_source_table]
 @vault_source_unique_name = ''' + @StageTable + '''
,@vault_source_load_type = ''full'''
UNION
SELECT 'select top 1000 * from ' + quotename(hub_database) + '.' + quotename(hub_schema) + '.' + quotename([ode_config].[dbo].[fn_get_object_name] (hub_name, 'hub'))
from [ode_config].[dbo].[dv_hub] where hub_name = @HubName
UNION
SELECT 'select top 1000 * from ' + quotename(satellite_database) + '.' + quotename(satellite_schema) + '.' + quotename([ode_config].[dbo].[fn_get_object_name] (satellite_name, 'sat'))
from [ode_config].[dbo].[dv_satellite] where satellite_name =  @SatelliteName
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
--END
