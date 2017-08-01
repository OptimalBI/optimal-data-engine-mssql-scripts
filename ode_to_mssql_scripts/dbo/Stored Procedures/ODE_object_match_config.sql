
CREATE PROCEDURE [dbo].[ODE_object_match_config]
-- To Do - sort out the order of the columns in the Stage Table..
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
	-- system the reference number refers to, Rally
,@HubName VARCHAR(128)				--= 'link_Sale_Match_Test'  --'Customer'--NULL -- to Default the Hub Name to the sat name - good for pure Raw Vault.
	-- For completely Raw Hub Sat combinations, you can leave this column as null. The Script will create a Hub using the same name as the source table.
	-- For Business hubs, specify the name of the Hub of the Ensemble, which you are adding to.
,@SatelliteName VARCHAR(128)		--= 'link_Sale_Match_Test'

,@VaultName VARCHAR(128)            --=  'ode_vault_MAGODE_40'
	--the name of the vault where the Hub and Satellite will be created.
,@ScheduleName VARCHAR(128)			--=  'Full_Load'
	--the schedule the load is to run in. This schedule needs to exist prior to running this script.
,@HubKeyNames	[dbo].[dv_column_list] READONLY
--declare @HubKeyNames table(HubKeyName VARCHAR(128)
--                          ,OrdinalPosition INT IDENTITY (1,1)) 
--insert @HubKeyNames values('dv_source_version_key') 
					     --,('dv_stage_date_time')
						 --,('_match_row')
						 --,('_master_table')

----The name of the unique Key columns. The columns need to exist in your Stage Table, and should be appropriately named for the Hub, which you are building.
-- List the Columns in the order in which you want them to appear in the Hub.
,@StageDatabase VARCHAR(128)		--= 'ode_stage_MAGODE_40'
,@StageSchema VARCHAR(128)			--= 'Stage'

,@MatchingLeftObjectType		VARCHAR(50)  = NULL --'hub','lnk', 'sat', 'stg'
,@MatchingLeftObjectDatabase	VARCHAR(128) = NULL
,@MatchingLeftObjectSchema		VARCHAR(128) = NULL
,@MatchingLeftObjectName		VARCHAR(128) = NULL
,@MatchingTemporalPitLeft		DATETIMEOFFSET(7) = NULL

,@MatchingRightObjectType		VARCHAR(50)  = NULL --'hub','lnk', 'sat', 'stg'
,@MatchingRightObjectDatabase	VARCHAR(128) = NULL
,@MatchingRightObjectSchema		VARCHAR(128) = NULL
,@MatchingRightObjectName		VARCHAR(128) = NULL
,@MatchingTemporalPitRight		DATETIMEOFFSET(7) = NULL

,@ColumnMatching [dbo].[dv_column_matching_list] READONLY
-- This Table is only used for setting up a match - @StageSourceType = 'LeftRightComparison'


--EXECUTE [dv_scheduler].[dv_schedule_insert] 'Full_Load', 'For Testing Purposes', 'Ad Hoc', 0
) AS
BEGIN
SET NOCOUNT ON

select @HubName, * from @HubKeyNames
/********************************************
Defaults:
********************************************/
DECLARE
 @sat_is_columnstore				BIT				= 1
,@sat_is_compressed					BIT				= 0
,@hub_is_compressed					BIT				= 1
	-- Note that Columnstore is only available in SQL Server Enterprise Edition.
,@duplicate_removal_threshold		INT				= 0
,@DevServerName						SYSNAME			= 'Ignore'
	-- You can provide a Server Name here to prevent accidentally creating keys and objects in the wrong environment.
,@BusinessVaultName					VARCHAR(128)	= 'Ignore'
	-- You can provide a name here to cause the Business key to be excluded from the Sat, in a specific Vault.
DECLARE @ExcludeColumns				TABLE (ColumnName VARCHAR(128))
--INSERT @ExcludeColumns				VALUES ('dv_stage_datetime')
	--Insert columns which should never be included in the satellites.
DECLARE @StageTable					VARCHAR(128)			
       ,@StageSourceType			VARCHAR(50)		= 'LeftRightComparison'
       ,@StageLoadType				VARCHAR(50)		= 'Full'
       ,@StagePassLoadTypeToProc	BIT				= 0
	   ,@SourceSystem				VARCHAR(128)	= 'sysgen'
SELECT @StageTable = @SatelliteName + '_' + REPLACE(CAST(newid() AS VARCHAR(36)),'-','')

DECLARE @stage_Load_Date_Time_column		varchar(128)
	   ,@stage_Source_Version_Key_column	varchar(128)
	   ,@stage_match_key_column				varchar(128)
	   ,@stage_master_table_column			varchar(128)

-- List the Columns in the order in which you want them to appear in the Hub.
/********************************************
Begin:
********************************************/

-- Exclude the Hub Key from the Satellite if it is in Business Vault. Otherwise keep it.

select 1 from [$(ConfigDatabase)] .[dbo].[dv_stage_database] sd
inner join [$(ConfigDatabase)] .[dbo].[dv_stage_schema]ss on ss.[stage_database_key] = sd.[stage_database_key]
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
,@source_procedure_name         varchar(128) = NULL
,@pass_load_type_to_proc		BIT = 0
,@column_name					VARCHAR(128)
,@column_type					VARCHAR(50)
,@column_length					INT
,@column_precision				INT
,@column_scale					INT
,@collation_Name				VARCHAR(128)
,@ordinal_position				INT
,@StageDatabaseKey				INT
,@StageSchemaKey				INT
,@SourceSystemKey				INT

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
FROM [$(ConfigDatabase)] .[dv_release].[dv_release_master]
WHERE left(cast([release_number] AS VARCHAR(100)), 8) = @sprintdate
ORDER BY 1 DESC
IF @@rowcount = 0
SET @release_number = cast(@sprintdate + '01' AS INT)
ELSE
SET @release_number = cast(@sprintdate + right('00' + cast(@seqint + 1 AS VARCHAR(100)), 2) AS INT)
SELECT @release_number
SET @Description = 'Load Stage Table: ' + quotename(@StageTable) + ' into ' + quotename(@VaultName)
-- Create the Release:
EXECUTE  @release_key = [$(ConfigDatabase)] .[dv_release].[dv_release_master_insert]  @release_number		= @release_number	-- date of the Sprint Start + ad hoc release number
																,@release_description	= @Description		-- what the release is for
																,@reference_number		= @ReleaseReference
																,@reference_source		= @ReleaseSource
--
/********************************************
Hub:
********************************************/
select 'Configure the Hub:'

if @SatelliteOnly = 'N'
begin
SELECT @abbn = [$(ConfigDatabase)] .[dbo].[fn_get_next_abbreviation]()
EXECUTE @hub_key = [$(ConfigDatabase)] .[dbo].[dv_hub_insert] 
				   @hub_name			= @HubName
				  ,@hub_abbreviation	= @abbn
				  ,@hub_schema			= 'hub'
				  ,@hub_database		= @VaultName
				  ,@is_compressed		= @hub_is_compressed
				  ,@is_retired			= 0
				  ,@release_number		= @release_number
end				  
else
begin
select @hub_key			= [hub_key]
      ,@hub_database	= [hub_database]
from [$(ConfigDatabase)] .[dbo].[dv_hub] 
where [hub_name] = @HubName
if @hub_database <> @VaultName
begin
raiserror( 'The Hub and Satellite have to exist in the same database', 16, 1)
end
end
select 'Hub_key',@hub_key
/********************************************
Satellite:
********************************************/
select 'Configure the Satellite:'

SELECT @abbn = [$(ConfigDatabase)] .[dbo].[fn_get_next_abbreviation]()
EXECUTE @satellite_key = [$(ConfigDatabase)] .[dbo].[dv_satellite_insert] 
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
select 'Get the necessary keys: '
SELECT @StageDatabaseKey	= d.stage_database_key
      ,@StageSchemaKey		= s.stage_schema_key
  FROM [$(ConfigDatabase)].[dbo].[dv_stage_database] d
  inner join [$(ConfigDatabase)].[dbo].[dv_stage_schema] s
  on s.stage_database_key	= d.stage_database_key
where d.stage_database_name = @StageDatabase
  and s.stage_schema_name	= @StageSchema
SELECT @SourceSystemKey = [source_system_key]
FROM [$(ConfigDatabase)].[dbo].[dv_source_system]
where [source_system_name] = @SourceSystem

select 'Create The Stage Table itself:'
EXECUTE @source_table_key	= [$(ConfigDatabase)].[dbo].[dv_source_table_insert] 
   @source_unique_name		= @StageTable
  ,@source_type				= @StageSourceType
  ,@load_type				= @StageLoadType
  ,@system_key				= @SourceSystemKey
  ,@source_table_schema		= NULL
  ,@source_table_name		= @StageTable
  ,@stage_schema_key		= @StageSchemaKey
  ,@stage_table_name		= @StageTable
  ,@is_retired				= 0
  ,@release_number			= @release_number
select ' Add a Current Source Version with a "Version" of'
EXECUTE @source_version_key = [$(ConfigDatabase)].[dbo].[dv_source_version_insert] 
   @source_table_key		= @source_table_key
  ,@source_version			= 1
  ,@source_procedure_name   = @source_procedure_name
  ,@pass_load_type_to_proc	= @pass_load_type_to_proc
  ,@is_current				= 1
  ,@release_number			= @release_number
-- Create the Stage Columns from the Left aspect of the Match:
/********************************************
Left Right Match
********************************************/
BEGIN
select 'hook up the left right columns for matching'
EXECUTE @match_key = [$(ConfigDatabase)] .[dbo].[dv_object_match_insert] 
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
	select @left_hub_key_column_key = hub_key_column_key
	from [$(ConfigDatabase)] .[dbo].[dv_hub] h
	inner join [$(ConfigDatabase)] .[dbo].[dv_hub_key_column] hkc on hkc.hub_key = h.hub_key
	where h.[hub_database]			= @MatchingLeftObjectDatabase
	  and h.[hub_schema]			= @MatchingLeftObjectSchema
	  and h.[hub_name]				= @MatchingLeftObjectName
	  and hkc.[hub_key_column_name] = @left_column_name
	if @@ROWCOUNT <> 1 raiserror( 'Left Matching %s Column %s not found in Config', 16, 1, @MatchingLeftObjectType,@left_column_name)
end
else IF @MatchingLeftObjectType = 'lnk'
begin
	select @left_link_key_column_key = link_key_column_key
	from [$(ConfigDatabase)] .[dbo].[dv_link] l
	inner join [$(ConfigDatabase)] .[dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
	where l.[link_database]			 = @MatchingLeftObjectDatabase
	  and l.[link_schema]			 = @MatchingLeftObjectSchema
	  and l.[link_name]				 = @MatchingLeftObjectName
	  and lkc.[link_key_column_name] = @left_column_name
	if @@ROWCOUNT <> 1 raiserror( 'Left Matching %s Column %s not found in Config', 16, 1, @MatchingLeftObjectType,@left_column_name)
end
else IF @MatchingLeftObjectType = 'sat'
begin 

	select @left_satellite_col_key = satellite_col_key
	from [$(ConfigDatabase)] .[dbo].[dv_satellite] s
	inner join [$(ConfigDatabase)] .[dbo].[dv_satellite_column] sc on sc.satellite_key = s.satellite_key
	where s.[satellite_database]		 = @MatchingLeftObjectDatabase
	  and s.[satellite_schema]			 = @MatchingLeftObjectSchema
	  and s.[satellite_name]			 = @MatchingLeftObjectName
	  and sc.[column_name]				 = @left_column_name
	
	if @@ROWCOUNT <> 1 raiserror( 'Left Matching %s Column %s not found in Config', 16, 1, @MatchingLeftObjectType,@left_column_name)
end
else IF @MatchingLeftObjectType = 'stg'
begin
	select @left_column_key = column_key
	from [$(ConfigDatabase)] .[dbo].[dv_source_table] st
	inner join [$(ConfigDatabase)] .[dbo].[dv_stage_schema] ss on ss.[stage_schema_key] = st.[stage_schema_key]
	inner join [$(ConfigDatabase)] .[dbo].[dv_stage_database] sd on sd.[stage_database_key] = ss.[stage_database_key]
	inner join [$(ConfigDatabase)] .[dbo].[dv_column] c on c.table_key = st.source_table_key
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
	from [$(ConfigDatabase)] .[dbo].[dv_hub] h
	inner join [$(ConfigDatabase)] .[dbo].[dv_hub_key_column] hkc on hkc.hub_key = h.hub_key
	where h.[hub_database]			= @MatchingrightObjectDatabase
	  and h.[hub_schema]			= @MatchingrightObjectSchema
	  and h.[hub_name]				= @MatchingrightObjectName
	  and hkc.[hub_key_column_name] = @right_column_name
	if @@ROWCOUNT <> 1 raiserror( 'right Matching %s Column %s not found in Config', 16, 1, @MatchingrightObjectType,@right_column_name)
end
else IF @MatchingrightObjectType = 'lnk'
begin
	select @right_link_key_column_key = link_key_column_key
	from [$(ConfigDatabase)] .[dbo].[dv_link] l
	inner join [$(ConfigDatabase)] .[dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
	where l.[link_database]			 = @MatchingrightObjectDatabase
	  and l.[link_schema]			 = @MatchingrightObjectSchema
	  and l.[link_name]				 = @MatchingrightObjectName
	  and lkc.[link_key_column_name] = @right_column_name
	if @@ROWCOUNT <> 1 raiserror( 'right Matching %s Column %s not found in Config', 16, 1, @MatchingrightObjectType,@right_column_name)
end
else IF @MatchingrightObjectType = 'sat'
begin
	select @right_satellite_col_key = satellite_col_key
	from [$(ConfigDatabase)] .[dbo].[dv_satellite] s
	inner join [$(ConfigDatabase)] .[dbo].[dv_satellite_column] sc on sc.satellite_key = s.satellite_key
	where s.[satellite_database]		 = @MatchingrightObjectDatabase
	  and s.[satellite_schema]			 = @MatchingrightObjectSchema
	  and s.[satellite_name]			 = @MatchingrightObjectName
	  and sc.[column_name]				 = @right_column_name
	if @@ROWCOUNT <> 1 raiserror( 'right Matching %s Column %s not found in Config', 16, 1, @MatchingrightObjectType,@right_column_name)
end
else IF @MatchingrightObjectType = 'stg'
begin
	select @right_column_key = column_key
	from [$(ConfigDatabase)] .[dbo].[dv_source_table] st
	inner join [$(ConfigDatabase)] .[dbo].[dv_stage_schema] ss on ss.[stage_schema_key] = st.[stage_schema_key]
	inner join [$(ConfigDatabase)] .[dbo].[dv_stage_database] sd on sd.[stage_database_key] = ss.[stage_database_key]
	inner join [$(ConfigDatabase)] .[dbo].[dv_column] c on c.table_key = st.source_table_key
	where sd.[stage_database_name]		 = @MatchingrightObjectDatabase
	  and ss.[stage_schema_name]		 = @MatchingrightObjectSchema
	  and st.[stage_table_name]			 = @MatchingrightObjectName
	  and c.[column_name]				 = @right_column_name
	if @@ROWCOUNT <> 1 raiserror( 'right Matching %s Column %s not found in Config', 16, 1, @MatchingrightObjectType,@right_column_name)
end
else raiserror( 'right Matching %s is not a valid Matching Object', 16, 1, @MatchingrightObjectType)

EXECUTE [$(ConfigDatabase)].[dbo].[dv_column_match_insert] 
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

SELECT 'Create the Source Columns from the Left Leg of the Match:'

DECLARE curStageKey CURSOR FOR 
select column_name	
	  ,column_type
	  ,column_length	
	  ,column_precision	
	  ,column_scale	
	  ,collation_name
	  ,ordinal_position = row_number() over (order by rn, column_name)
FROM
(SELECT --m.col_match_key
       column_name = coalesce(hkc.[hub_key_column_name], lkc.[link_key_column_name], sc.[column_name], c.[column_name])
	  ,column_type = 
	                 case when m.left_hub_key_column_key	is not null then hkc.[hub_key_column_type] 
	                      when m.left_link_key_column_key	is not null then (select [column_type] FROM [$(ConfigDatabase)].[dbo].[dv_default_column] where object_type = 'Hub' and object_column_type = 'Object_Key')
						  when m.left_satellite_col_key		is not null then sc.[column_type]
						  when m.left_column_key			is not null then c.[column_type]
						  else null
						  end
	  ,column_length =
					 case when m.left_hub_key_column_key	is not null then hkc.[hub_key_column_length] 
	                      when m.left_link_key_column_key	is not null then (select [column_length] FROM [$(ConfigDatabase)].[dbo].[dv_default_column] where object_type = 'Hub' and object_column_type = 'Object_Key')
						  when m.left_satellite_col_key		is not null then sc.[column_length]
						  when m.left_column_key			is not null then c.[column_length]
						  else null
						  end
	  ,column_precision =
					 case when m.left_hub_key_column_key	is not null then hkc.[hub_key_column_precision] 
	                      when m.left_link_key_column_key	is not null then (select [column_precision] FROM [$(ConfigDatabase)].[dbo].[dv_default_column] where object_type = 'Hub' and object_column_type = 'Object_Key')
						  when m.left_satellite_col_key		is not null then sc.[column_precision]
						  when m.left_column_key			is not null then c.[column_precision]
						  else null
						  end
	  ,column_scale =
					 case when m.left_hub_key_column_key	is not null then hkc.[hub_key_column_scale] 
	                      when m.left_link_key_column_key	is not null then (select [column_scale] FROM [$(ConfigDatabase)].[dbo].[dv_default_column] where object_type = 'Hub' and object_column_type = 'Object_Key')
						  when m.left_satellite_col_key		is not null then sc.[column_scale]
						  when m.left_column_key			is not null then c.[column_scale]
						  else null
						  end
	  ,collation_name =
					 case when m.left_hub_key_column_key	is not null then hkc.[hub_key_collation_name] 
	                      when m.left_link_key_column_key	is not null then (select [collation_name] FROM [$(ConfigDatabase)].[dbo].[dv_default_column] where object_type = 'Hub' and object_column_type = 'Object_Key')
						  when m.left_satellite_col_key		is not null then sc.[collation_name]
						  when m.left_column_key			is not null then c.[collation_name]
						  else null
						  end
	  ,rn = 100000
  FROM [$(ConfigDatabase)].[dbo].[dv_object_match] o
  inner join [$(ConfigDatabase)].[dbo].[dv_column_match] m on m.[match_key] = o.[match_key]
  left join [$(ConfigDatabase)].[dbo].[dv_hub_key_column] hkc on hkc.hub_key_column_key = m.left_hub_key_column_key
  left join [$(ConfigDatabase)].[dbo].[dv_link_key_column] lkc on lkc.link_key_column_key = m.left_link_key_column_key
  left join [$(ConfigDatabase)].[dbo].[dv_satellite_column] sc on sc.[satellite_col_key] = m.left_satellite_col_key
  left join [$(ConfigDatabase)].[dbo].[dv_column] c on c.[column_key] = m.left_column_key
  where o.match_key = @match_key
  -- get the Stage Technical Columns
  union select [column_name],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[ordinal_position]
		from [$(ConfigDatabase)].[dbo].[dv_default_column]
		where object_column_type = 'Load_Date_Time' and object_type = 'stg'
  union select [column_name],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[ordinal_position]
		from [$(ConfigDatabase)].[dbo].[dv_default_column]
		where object_column_type = 'Source_Version_Key' and object_type = 'stg'

-- get the Match Technical Columns
  union select [column_name],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[ordinal_position] * 100
		from [$(ConfigDatabase)].[dbo].[dv_default_column]
		where object_column_type = 'MatchKeyColumn' and object_type = 'mtc'
  union select [column_name],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[ordinal_position] * 100 
		from [$(ConfigDatabase)].[dbo].[dv_default_column]
		where object_column_type = 'MasterTableColumn' and object_type = 'mtc' 
  ) a

OPEN curStageKey   
FETCH NEXT FROM curStageKey INTO @column_name
								,@column_type
								,@column_length
								,@column_precision
								,@column_scale
								,@collation_Name
								,@ordinal_position

select  @column_name
	   ,@column_type
	   ,@column_length
	   ,@column_precision
	   ,@column_scale
	   ,@collation_Name
	   ,@ordinal_position

WHILE @@FETCH_STATUS = 0   
BEGIN 
EXECUTE [$(ConfigDatabase)].[dbo].[dv_column_insert] 
   @table_key				= @source_table_key
  ,@release_number			= @release_number
  ,@satellite_col_key		= null
  ,@column_name				= @column_name
  ,@column_type				= @column_type
  ,@column_length			= @column_length
  ,@column_precision		= @column_precision
  ,@column_scale			= @column_scale
  ,@Collation_Name			= @collation_Name
  ,@bk_ordinal_position		= 0
  ,@source_ordinal_position = @ordinal_position
  ,@is_source_date			= 0
  ,@is_retired				= 0

FETCH NEXT FROM curStageKey INTO @column_name
								,@column_type
								,@column_length
								,@column_precision
								,@column_scale
								,@collation_Name
								,@ordinal_position
END
CLOSE curStageKey   
DEALLOCATE curStageKey
--
SELECT 'Hook the Source Columns up to the Satellite:'
EXECUTE [$(ConfigDatabase)] .[dv_config].[dv_populate_satellite_columns] 
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
--select 'hello;' ,* from [$(ConfigDBName)] .[dbo].[dv_column] where table_key = @source_table_key
select @HubKeyName, @OrdinalPosition, @source_table_key 
-- Create the Hub Key based on the Source Column:
SELECT @hub_key_column_type			= 'varchar'	--[column_type]
	  ,@hub_key_column_length		= 128		--[column_length]
	  ,@hub_key_column_precision	= 0			--[column_precision]
	  ,@hub_key_column_scale		= 0			--[column_scale]
	  ,@hub_key_Collation_Name	    = null		--[Collation_Name]
      ,@hub_source_column_key		= [column_key]
FROM [$(ConfigDatabase)] .[dbo].[dv_column] c
WHERE [column_key] IN (
SELECT c.[column_key]
FROM [$(ConfigDatabase)] .[dbo].[dv_source_table] st 
inner join [$(ConfigDatabase)] .[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE 1=1
and st.source_table_key = @source_table_key
and c.column_name = @HubKeyName
)


SELECT *
FROM [$(ConfigDatabase)] .[dbo].[dv_column] c
WHERE [column_key] IN (
SELECT c.[column_key]
FROM [$(ConfigDatabase)] .[dbo].[dv_source_table] st 
inner join [$(ConfigDatabase)] .[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE 1=1
and st.source_table_key = @source_table_key
)


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
	
	EXECUTE @hub_key_column_key = [$(ConfigDatabase)] .[dbo].[dv_hub_key_insert] 
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
	from [$(ConfigDatabase)] .[dbo].[dv_hub_key_column]
	where [hub_key] = @hub_key
	and [hub_key_column_name] = @HubKeyName
end
-- hook the Hub Key up to the Source Column which will populate it:
select  hub_key_column_key		 = @hub_key_column_key
	   ,hub_source_column_key	 = @hub_source_column_key
	   ,HubKeyName				 = @HubKeyName

EXECUTE [$(ConfigDatabase)] .[dbo].[dv_hub_column_insert] 
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
update [$(ConfigDatabase)] .[dbo].[dv_column]
set [satellite_col_key] = NULL
where [column_name] IN (
SELECT *
FROM @ExcludeColumns)
and [column_key] in(select c.column_key from [$(ConfigDatabase)] .[dbo].[dv_column] c 
                    inner join [$(ConfigDatabase)] .[dbo].[dv_satellite_column] sc on sc.[satellite_col_key] = c.[satellite_col_key]
					where sc.[satellite_key] = @satellite_key)

-- If you don't want Keys in the satellites:
	DELETE
	FROM [$(ConfigDatabase)] .[dbo].[dv_satellite_column]
	WHERE [satellite_col_key] IN (
		select sc.[satellite_col_key]
		from [$(ConfigDatabase)] .[dbo].[dv_satellite_column] sc
		left join [$(ConfigDatabase)] .[dbo].[dv_column] c	on sc.[satellite_col_key] = c.[satellite_col_key]
		where c.[satellite_col_key] is null
		  and sc.[satellite_key] = @satellite_key
		  )
/********************************************
Scheduler:
********************************************/
-- Add the Source the the required Schedule:
EXECUTE [$(ConfigDatabase)] .[dv_scheduler].[dv_schedule_source_table_insert] 
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
SELECT 'select top 1000 * from ' + quotename(hub_database) + '.' + quotename(hub_schema) + '.' + quotename([$(ConfigDatabase)] .[dbo].[fn_get_object_name] (hub_name, 'hub'))
from [$(ConfigDatabase)] .[dbo].[dv_hub] where hub_name = @HubName
UNION
SELECT 'select top 1000 * from ' + quotename(satellite_database) + '.' + quotename(satellite_schema) + '.' + quotename([$(ConfigDatabase)] .[dbo].[fn_get_object_name] (satellite_name, 'sat'))
from [$(ConfigDatabase)] .[dbo].[dv_satellite] where satellite_name =  @SatelliteName
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