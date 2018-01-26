/*
Calendar configuration and Reporting script
*/



DECLARE @ReleaseKey INT
, @StageDatebaseKey INT
, @StageSchemaKey INT
, @abbn VARCHAR(4)
, @HubKey INT
, @SatelliteKey INT
, @SourceTableKey INT
, @HubKeyColumnKeyCAL INT
, @HubSourceColumnKey1 INT
, @HubKeyColumnKeyHOL1 INT
, @HubKeyColumnKeyHOL2 INT
, @HubSourceColumnKey2 INT
, @LinkCol1 INT
, @LinkCol2 INT
, @LinkCol3 INT
, @LinkKey INT
, @LinkKeyColumnKey1 INT
, @LinkKeyColumnKey2 INT

--Insert new release number. If such release number exists, it will be reused.
IF (SELECT COUNT(*)
FROM [$(ODE_Config)].[dv_release].[dv_release_master]
WHERE [release_number] = 2 ) = 0
EXEC [$(ODE_Config)].[dv_release].[dv_release_master_insert]
@Release_number = 2
,@Release_description = 'Adding Calendar ensemble'

--Insert Sysgen source system if it doesn't exist yet
IF (SELECT COUNT(*)
FROM [$(ODE_Config)].[dbo].[dv_source_system]
WHERE [source_system_name] = 'Sysgen' ) = 0
EXEC [$(ODE_Config)].[dbo].[dv_source_system_insert]
@source_system_name = 'Sysgen'
,@source_database_name = 'Sysgen'
,@package_folder = NULL
,@package_project = NULL
,@project_connection_name = NULL
,@is_retired = 0
,@release_number = 2

--Check if this is a new stage database. If so, add it to config
IF (SELECT COUNT(*)
FROM [$(ODE_Config)].[dbo].[dv_stage_database]
WHERE [stage_database_name] = '$(DatabaseName)') = 0
EXEC [$(ODE_Config)].[dbo].[dv_stage_database_insert]
@stage_database_name = '$(DatabaseName)'
,@stage_connection_name = NULL
,@is_retired = 0
,@release_number = 2

SELECT @StageDatebaseKey = [stage_database_key]
FROM [$(ODE_Config)].[dbo].[dv_stage_database]
WHERE [stage_database_name] = '$(DatabaseName)'

--Check if stage schema exists in stage database. If not, add it to cnfig
IF (SELECT COUNT(*)
FROM [$(ODE_Config)].[dbo].[dv_stage_schema]
WHERE [stage_database_key] = @StageDatebaseKey
AND [stage_schema_name] = 'stage') = 0
EXEC [$(ODE_Config)].[dbo].[dv_stage_schema_insert]
@stage_database_key = @StageDatebaseKey
,@stage_schema_name = 'stage'
,@is_retired = 0
,@release_number = 2

-----------------------------------------------------------
--Calendar ensemble

--Insert Calendar hub
SELECT @abbn = [$(ODE_Config)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @HubKey = [$(ODE_Config)].[dbo].[dv_hub_insert] 
@hub_name = 'Calendar'
,@hub_abbreviation = @abbn
,@hub_schema = 'hub'
,@hub_database = '$(ODE_Vault)'
,@is_compressed = 0
,@is_retired = 0
,@release_number = 2

-- Configure Calendar satellite
SELECT @abbn = [$(ODE_Config)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @SatelliteKey = [$(ODE_Config)].[dbo].[dv_satellite_insert] 
@hub_key = @HubKey
,@link_key = 0
,@link_hub_satellite_flag = 'H'
,@satellite_name = 'Calendar'
,@satellite_abbreviation = @abbn
,@satellite_schema = 'sat'
,@satellite_database = '$(ODE_Vault)'
,@duplicate_removal_threshold = 0
,@is_columnstore = 0
,@is_compressed	= 0
,@is_retired = 0
,@release_number = 2

--Populate source table from stage table
EXECUTE [$(ODE_Config)].[dv_config].[dv_populate_source_table_columns] 
   @vault_stage_database = '$(DatabaseName)'
  ,@vault_stage_schema = 'stage'
  ,@vault_stage_table = 'Calendar'
  ,@vault_source_unique_name = 'Calendar'
  ,@vault_stage_table_load_type = 'Full'
  ,@vault_source_system_name = 'Sysgen'
  ,@vault_source_table_schema = ''
  ,@vault_source_table_name = ''
  ,@vault_release_number = 2
  ,@vault_rerun_column_insert = 0
  ,@is_columnstore = 0
  ,@is_compressed = 0

SELECT @SourceTableKey = source_table_key 
FROM [$(ODE_Config)].[dbo].[dv_source_table] 
WHERE [source_unique_name] = 'Calendar'

 -- Add source version
EXECUTE  [$(ODE_Config)].[dbo].[dv_source_version_insert] 
   @source_table_key = @SourceTableKey
  ,@source_version = 1
  ,@source_type	= 'BespokeProc'
  ,@source_procedure_name = 'usp_Calendar'
  ,@source_filter = NULL
  ,@pass_load_type_to_proc = 0
  ,@is_current = 1
  ,@release_number = 2

--Hook the Source Columns up to the Satellite
EXECUTE [$(ODE_Config)].[dv_config].[dv_populate_satellite_columns] 
   @vault_source_unique_name = 'Calendar'
  ,@vault_satellite_name = 'Calendar'
  ,@vault_release_number = 2
  ,@vault_rerun_satellite_column_insert = 0

-- Hub Key
EXECUTE @HubKeyColumnKeyCAL = [$(ODE_Config)].[dbo].[dv_hub_key_insert] 
 @hub_key = @HubKey
,@hub_key_column_name = 'DateKey'
,@hub_key_column_type = 'date'
,@hub_key_column_length = 3
,@hub_key_column_precision = 10
,@hub_key_column_scale = 0
,@hub_key_Collation_Name = NULL
,@hub_key_ordinal_position = 1
,@release_number = 2

SELECT  @HubSourceColumnKey1 = c.[column_key]
FROM [$(ODE_Config)].[dbo].[dv_source_table] st 
inner join [$(ODE_Config)].[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE st.source_table_key = @SourceTableKey
and c.column_name = 'DateKey'

EXECUTE [$(ODE_Config)].[dbo].[dv_hub_column_insert] 
@hub_key_column_key	= @HubKeyColumnKeyCAL
,@link_key_column_key = NULL
,@column_key = @HubSourceColumnKey1
,@release_number = 2

DELETE FROM [$(ODE_Config)].[dbo].[dv_column]
FROM [$(ODE_Config)].[dbo].[dv_column]
WHERE [table_key] = @SourceTableKey
AND [column_name] = 'dv_stage_date_time'

DELETE FROM [$(ODE_Config)].[dbo].[dv_satellite_column]
FROM [$(ODE_Config)].[dbo].[dv_satellite_column]
WHERE [satellite_key] = @SatelliteKey
AND [column_name] = 'dv_stage_date_time'

---------------------------------------------------
--CalendarFiscal satellite
SELECT @abbn = [$(ODE_Config)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @SatelliteKey = [$(ODE_Config)].[dbo].[dv_satellite_insert] 
@hub_key = @HubKey
,@link_key = 0
,@link_hub_satellite_flag = 'H'
,@satellite_name = 'CalendarFiscal'
,@satellite_abbreviation = @abbn
,@satellite_schema = 'sat'
,@satellite_database = '$(ODE_Vault)'
,@duplicate_removal_threshold = 0
,@is_columnstore = 0
,@is_compressed	= 0
,@is_retired = 0
,@release_number = 2

--Populate source table from stage table
EXECUTE [$(ODE_Config)].[dv_config].[dv_populate_source_table_columns] 
   @vault_stage_database = '$(DatabaseName)'
  ,@vault_stage_schema = 'stage'
  ,@vault_stage_table = 'CalendarFiscal'
  ,@vault_source_unique_name = 'CalendarFiscal'
  ,@vault_stage_table_load_type = 'Full'
  ,@vault_source_system_name = 'Sysgen'
  ,@vault_source_table_schema = ''
  ,@vault_source_table_name = ''
  ,@vault_release_number = 2
  ,@vault_rerun_column_insert = 0
  ,@is_columnstore = 0
  ,@is_compressed = 0

SELECT @SourceTableKey = source_table_key 
FROM [$(ODE_Config)].[dbo].[dv_source_table] 
WHERE [source_unique_name] = 'CalendarFiscal'

 -- Add source version
EXECUTE   [$(ODE_Config)].[dbo].[dv_source_version_insert] 
   @source_table_key = @SourceTableKey
  ,@source_version = 1
  ,@source_type	= 'BespokeProc'
  ,@source_procedure_name = 'usp_CalendarFiscal'
  ,@source_filter = NULL
  ,@pass_load_type_to_proc = 0
  ,@is_current = 1
  ,@release_number = 2

--Hook the Source Columns up to the Satellite
EXECUTE [$(ODE_Config)].[dv_config].[dv_populate_satellite_columns] 
   @vault_source_unique_name = 'CalendarFiscal'
  ,@vault_satellite_name = 'CalendarFiscal'
  ,@vault_release_number = 2
  ,@vault_rerun_satellite_column_insert = 0

SELECT  @HubSourceColumnKey1 = c.[column_key]
FROM [$(ODE_Config)].[dbo].[dv_source_table] st 
inner join [$(ODE_Config)].[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE st.source_table_key = @SourceTableKey
and c.column_name = 'DateKey'

EXECUTE [$(ODE_Config)].[dbo].[dv_hub_column_insert] 
@hub_key_column_key	= @HubKeyColumnKeyCAL
,@link_key_column_key = NULL
,@column_key = @HubSourceColumnKey1
,@release_number = 2

DELETE FROM [$(ODE_Config)].[dbo].[dv_column]
FROM [$(ODE_Config)].[dbo].[dv_column]
WHERE [table_key] = @SourceTableKey
AND [column_name] = 'dv_stage_date_time'

DELETE FROM [$(ODE_Config)].[dbo].[dv_satellite_column]
FROM [$(ODE_Config)].[dbo].[dv_satellite_column]
WHERE [satellite_key] = @SatelliteKey
AND [column_name] = 'dv_stage_date_time'
----------------------------------------------
--Calendar holidays ensemle
--Insert hub
SELECT @abbn = [$(ODE_Config)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @HubKey = [$(ODE_Config)].[dbo].[dv_hub_insert] 
@hub_name = 'CalendarHolidays'
,@hub_abbreviation = @abbn
,@hub_schema = 'hub'
,@hub_database = '$(ODE_Vault)'
,@is_compressed = 0
,@is_retired = 0
,@release_number = 2

-- Configure Calendar holidays satellite
SELECT @abbn = [$(ODE_Config)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @SatelliteKey = [$(ODE_Config)].[dbo].[dv_satellite_insert] 
@hub_key = @HubKey
,@link_key = 0
,@link_hub_satellite_flag = 'H'
,@satellite_name = 'CalendarHolidays'
,@satellite_abbreviation = @abbn
,@satellite_schema = 'sat'
,@satellite_database = '$(ODE_Vault)'
,@duplicate_removal_threshold = 0
,@is_columnstore = 0
,@is_compressed	= 0
,@is_retired = 0
,@release_number = 2

--Populate source table from stage table
EXECUTE [$(ODE_Config)].[dv_config].[dv_populate_source_table_columns] 
   @vault_stage_database = '$(DatabaseName)'
  ,@vault_stage_schema = 'stage'
  ,@vault_stage_table = 'CalendarHolidays'
  ,@vault_source_unique_name = 'CalendarHolidays'
  ,@vault_stage_table_load_type = 'Full'
  ,@vault_source_system_name = 'Sysgen'
  ,@vault_source_table_schema = ''
  ,@vault_source_table_name = ''
  ,@vault_release_number = 2
  ,@vault_rerun_column_insert = 0
  ,@is_columnstore = 0
  ,@is_compressed = 0

SELECT @SourceTableKey = source_table_key 
FROM [$(ODE_Config)].[dbo].[dv_source_table] 
WHERE [source_unique_name] = 'CalendarHolidays'

 -- Add source version
EXECUTE  [$(ODE_Config)].[dbo].[dv_source_version_insert] 
   @source_table_key = @SourceTableKey
  ,@source_version = 1
  ,@source_type	= 'BespokeProc'
  ,@source_procedure_name = 'usp_CalendarHolidays'
  ,@source_filter = NULL
  ,@pass_load_type_to_proc = 0
  ,@is_current = 1
  ,@release_number = 2

--Hook the Source Columns up to the Satellite
EXECUTE [$(ODE_Config)].[dv_config].[dv_populate_satellite_columns] 
   @vault_source_unique_name = 'CalendarHolidays'
  ,@vault_satellite_name = 'CalendarHolidays'
  ,@vault_release_number = 2
  ,@vault_rerun_satellite_column_insert = 0

-- Hub Key
EXECUTE @HubKeyColumnKeyHOL1 = [$(ODE_Config)].[dbo].[dv_hub_key_insert] 
 @hub_key = @HubKey
,@hub_key_column_name = 'DateKey'
,@hub_key_column_type = 'varchar'
,@hub_key_column_length = 128
,@hub_key_column_precision = 0
,@hub_key_column_scale = 0
,@hub_key_Collation_Name = NULL
,@hub_key_ordinal_position = 1
,@release_number = 2

SELECT  @HubSourceColumnKey1 = c.[column_key]
FROM [$(ODE_Config)].[dbo].[dv_source_table] st 
inner join [$(ODE_Config)].[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE st.source_table_key = @SourceTableKey
and c.column_name = 'DateKey'

EXECUTE [$(ODE_Config)].[dbo].[dv_hub_column_insert] 
@hub_key_column_key	= @HubKeyColumnKeyHOL1
,@link_key_column_key = NULL
,@column_key = @HubSourceColumnKey1
,@release_number = 2

EXECUTE @HubKeyColumnKeyHOL2 = [$(ODE_Config)].[dbo].[dv_hub_key_insert] 
 @hub_key = @HubKey
,@hub_key_column_name = 'HolidayName'
,@hub_key_column_type = 'varchar'
,@hub_key_column_length = 128
,@hub_key_column_precision = 00
,@hub_key_column_scale = 0
,@hub_key_Collation_Name = NULL
,@hub_key_ordinal_position = 2
,@release_number = 2

SELECT  @HubSourceColumnKey2 = c.[column_key]
FROM [$(ODE_Config)].[dbo].[dv_source_table] st 
inner join [$(ODE_Config)].[dbo].[dv_column] c	on c.[table_key] = st.[source_table_key]
WHERE st.source_table_key = @SourceTableKey
and c.column_name = 'HolidayName'

EXECUTE [$(ODE_Config)].[dbo].[dv_hub_column_insert] 
@hub_key_column_key	= @HubKeyColumnKeyHOL2
,@link_key_column_key = NULL
,@column_key = @HubSourceColumnKey2
,@release_number = 2

DELETE FROM [$(ODE_Config)].[dbo].[dv_column]
FROM [$(ODE_Config)].[dbo].[dv_column]
WHERE [table_key] = @SourceTableKey
AND [column_name] = 'dv_stage_date_time'

DELETE FROM [$(ODE_Config)].[dbo].[dv_satellite_column]
FROM [$(ODE_Config)].[dbo].[dv_satellite_column]
WHERE [satellite_key] = @SatelliteKey
AND [column_name] = 'dv_stage_date_time'

-------------------------------------------------
-- Link between Calendar and Holiday

SELECT @abbn = [$(ODE_Config)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @LinkKey = [$(ODE_Config)].[dbo].[dv_link_insert] 
@link_name = 'Calendar_Holidays'
,@link_abbreviation = @abbn
,@link_schema = 'lnk'
,@link_database = '$(ODE_Vault)'
,@is_compressed = 0
,@is_retired = 0
,@release_number = 2

SELECT @abbn = [$(ODE_Config)].[dbo].[fn_get_next_abbreviation]()
EXECUTE @SatelliteKey = [$(ODE_Config)].[dbo].[dv_satellite_insert] 
@hub_key = 0
,@link_key = @LinkKey
,@link_hub_satellite_flag = 'L'
,@satellite_name = 'link_Calendar_Holidays'
,@satellite_abbreviation = @abbn
,@satellite_schema = 'sat'
,@satellite_database = '$(ODE_Vault)'
,@duplicate_removal_threshold = 0
,@is_columnstore = 0
,@is_compressed	= 0
,@is_retired = 0
,@release_number = 2

EXECUTE  @LinkKeyColumnKey1 = [$(ODE_Config)].[dbo].[dv_link_key_insert] 
@link_key = @LinkKey
,@link_key_column_name = 'Calendar'
,@release_number = 2

EXECUTE  @LinkKeyColumnKey2 = [$(ODE_Config)].[dbo].[dv_link_key_insert] 
@link_key = @LinkKey
,@link_key_column_name = 'CalendarHolidays'
,@release_number = 2

 EXECUTE [$(ODE_Config)].[dv_config].[dv_populate_source_table_columns] 
   @vault_stage_database = '$(DatabaseName)'
  ,@vault_stage_schema = 'stage'
  ,@vault_stage_table = 'link_Calendar_Holidays'
  ,@vault_source_unique_name = 'link_Calendar_Holidays'
  ,@vault_stage_table_load_type = 'Full'
  ,@vault_source_system_name = 'Sysgen'
  ,@vault_source_table_schema = ''
  ,@vault_source_table_name = ''
  ,@vault_release_number = 2
  ,@vault_rerun_column_insert = 0
  ,@is_columnstore = 0
  ,@is_compressed = 0

SELECT @SourceTableKey = source_table_key 
FROM [$(ODE_Config)].[dbo].[dv_source_table] 
WHERE [source_unique_name] = 'link_Calendar_Holidays'

 -- Add source version
EXECUTE  [$(ODE_Config)].[dbo].[dv_source_version_insert] 
   @source_table_key = @SourceTableKey
  ,@source_version = 1
  ,@source_type	= 'BespokeProc'
  ,@source_procedure_name = 'usp_link_Calendar_Holidays'
  ,@source_filter = NULL
  ,@pass_load_type_to_proc = 0
  ,@is_current = 1
  ,@release_number = 2

--Hook the Source Columns up to the Satellite
EXECUTE [$(ODE_Config)].[dv_config].[dv_populate_satellite_columns] 
   @vault_source_unique_name = 'link_Calendar_Holidays'
  ,@vault_satellite_name = 'link_Calendar_Holidays'
  ,@vault_release_number = 2
  ,@vault_rerun_satellite_column_insert = 0

SELECT @LinkCol1 = column_key
FROM [$(ODE_Config)].[dbo].[dv_column]
WHERE [table_key] = @SourceTableKey
AND [column_name] = 'DateKey'

SELECT @LinkCol2 = column_key
FROM [$(ODE_Config)].[dbo].[dv_column]
WHERE [table_key] = @SourceTableKey
AND [column_name] = 'HolidayDateKey'

SELECT @LinkCol3 = column_key
FROM [$(ODE_Config)].[dbo].[dv_column]
WHERE [table_key] = @SourceTableKey
AND [column_name] = 'HolidayName'

EXECUTE [$(ODE_Config)].[dbo].[dv_hub_column_insert] 
@hub_key_column_key	= @HubKeyColumnKeyCAL
,@link_key_column_key = @LinkKeyColumnKey1
,@column_key = @LinkCol1
,@release_number = 2

EXECUTE [$(ODE_Config)].[dbo].[dv_hub_column_insert] 
@hub_key_column_key	= @HubKeyColumnKeyHOL1
,@link_key_column_key = @LinkKeyColumnKey2
,@column_key = @LinkCol2
,@release_number = 2

EXECUTE [$(ODE_Config)].[dbo].[dv_hub_column_insert] 
@hub_key_column_key	= @HubKeyColumnKeyHOL2
,@link_key_column_key = @LinkKeyColumnKey2
,@column_key = @LinkCol3
,@release_number = 2

DELETE FROM [$(ODE_Config)].[dbo].[dv_column]
FROM [$(ODE_Config)].[dbo].[dv_column]
WHERE [table_key] = @SourceTableKey
AND [column_name] = 'dv_stage_date_time'

DELETE FROM [$(ODE_Config)].[dbo].[dv_satellite_column]
FROM [$(ODE_Config)].[dbo].[dv_satellite_column]
WHERE [satellite_key] = @SatelliteKey
AND [column_name] = 'dv_stage_date_time'

--------------------------------------------------------
--Calendar scheduling
-- Schedule hierarchy
EXECUTE [$(ODE_Config)].[dv_scheduler].[dv_source_table_hierarchy_insert]
@source_unique_name = 'link_Calendar_Holidays'
,@prior_source_unique_name = 'CalendarHolidays'
,@release_number = 2

-- Schedule the whole calendar in separate schedule
EXECUTE [$(ODE_Config)].[dv_scheduler].[dv_schedule_insert] 
@schedule_name = 'Load_Calendar'
,@schedule_description = 'Populates Calendar ensemble'
,@schedule_frequency = 'Manual'
,@release_number = 2

EXECUTE [$(ODE_Config)].[dv_scheduler].[dv_schedule_source_table_insert] 
@schedule_name = 'Load_Calendar'
,@source_unique_name = 'Calendar'
,@source_table_load_type = 'Full'
,@priority = 'Low'
,@queue = 'Agent001'
,@release_number = 2

EXECUTE [$(ODE_Config)].[dv_scheduler].[dv_schedule_source_table_insert] 
@schedule_name = 'Load_Calendar'
,@source_unique_name = 'CalendarFiscal'
,@source_table_load_type = 'Full'
,@priority = 'Low'
,@queue = 'Agent001'
,@release_number = 2

EXECUTE [$(ODE_Config)].[dv_scheduler].[dv_schedule_source_table_insert] 
@schedule_name = 'Load_Calendar'
,@source_unique_name = 'CalendarHolidays'
,@source_table_load_type = 'Full'
,@priority = 'Low'
,@queue = 'Agent001'
,@release_number = 2

EXECUTE [$(ODE_Config)].[dv_scheduler].[dv_schedule_source_table_insert] 
@schedule_name = 'Load_Calendar'
,@source_unique_name = 'link_Calendar_Holidays'
,@source_table_load_type = 'Full'
,@priority = 'Low'
,@queue = 'Agent001'
,@release_number = 2
GO
-------------------------------------------------------------------------------------
-- Create physical tables in the Vault
EXECUTE [$(ODE_Config)].[dbo].[dv_create_hub_table] '$(ODE_Vault)', 'Calendar', 'N'
EXECUTE [$(ODE_Config)].[dbo].[dv_create_hub_table] '$(ODE_Vault)','CalendarHolidays', 'N'
EXECUTE [$(ODE_Config)].[dbo].[dv_create_link_table] '$(ODE_Vault)','Calendar_Holidays', 'N'
EXECUTE [$(ODE_Config)].[dbo].[dv_create_sat_table] '$(ODE_Vault)','Calendar', 'N'
EXECUTE [$(ODE_Config)].[dbo].[dv_create_sat_table] '$(ODE_Vault)','CalendarFiscal', 'N'
EXECUTE [$(ODE_Config)].[dbo].[dv_create_sat_table] '$(ODE_Vault)','CalendarHolidays', 'N'
EXECUTE [$(ODE_Config)].[dbo].[dv_create_sat_table] '$(ODE_Vault)','link_Calendar_Holidays', 'N'

GO

-----------------------------------------------------------------------------------

-- Reporting view. It will be created in the Vault database by default. Move it to your reporting database or remove completely if not needed
USE [$(ODE_Vault)]
GO

CREATE VIEW [Calendar]
AS
WITH 
--------HUBS
  hCalendar			AS (SELECT * FROM [$(ODE_Vault)].[hub].[h_Calendar])
 ,hCalendarHolidays AS (SELECT * FROM [$(ODE_Vault)].[hub].[h_CalendarHolidays])
--------LINKS:
 ,lCalendarHolidays AS  (SELECT l.l_Calendar_Holidays_key, l.h_Calendar_key, l.h_CalendarHolidays_key 
                         FROM [$(ODE_Vault)].[lnk].[l_Calendar_Holidays] l
						 INNER JOIN [$(ODE_Vault)].[sat].[s_link_Calendar_Holidays] s on s.[l_Calendar_Holidays_key] = l.[l_Calendar_Holidays_key]
						 WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0)
--------SATELLITES
 ,sCalendar			AS (SELECT * FROM [$(ODE_Vault)].[sat].[s_Calendar] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)
 ,sFiscal			AS (SELECT * FROM [$(ODE_Vault)].[sat].[s_CalendarFiscal] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)
 ,sCalendarHolidays AS (SELECT * FROM [$(ODE_Vault)].[sat].[s_CalendarHolidays] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)
--------Current Fiscal Year
 ,wCurFiscalYear AS (SELECT sFiscal.FiscalYearCode
                       FROM hCalendar
					   LEFT JOIN sFiscal on sFiscal.h_Calendar_key = hCalendar.h_Calendar_key
					   WHERE hCalendar.DateKey = cast(getdate() as date)
					   )
 ,wLinkedHolidays AS (SELECT lCalendarHolidays.h_Calendar_key
							,sCalendarHolidays.HolidayDate	
							,sCalendarHolidays.HolidayName	
							,sCalendarHolidays.NationalHolidayName	
							,sCalendarHolidays.NationalObservedHolidayName	
							,sCalendarHolidays.RegionalHolidayName	
							,sCalendarHolidays.RegionalObservedHolidayName
					   FROM lCalendarHolidays
					   INNER JOIN sCalendarHolidays on sCalendarHolidays.h_CalendarHolidays_key = lCalendarHolidays.h_CalendarHolidays_key)
 ,wFlatHolidays AS (
select [h_Calendar_key]
 	  ,[NationalHolidays] = STUFF((SELECT N', ' +  [NationalHolidayName]
						FROM wLinkedHolidays AS p2
						WHERE p2.[h_Calendar_key] = p.[h_Calendar_key]
						ORDER BY [NationalHolidayName]
						FOR XML PATH(N'')), 1, 2, N'')
      ,[NationalObservedHolidays] = STUFF((SELECT N', ' +  [NationalObservedHolidayName]
						FROM wLinkedHolidays AS p2
						WHERE p2.[h_Calendar_key] = p.[h_Calendar_key]
						ORDER BY [NationalObservedHolidayName]
						FOR XML PATH(N'')), 1, 2, N'')
	  ,[RegionalHolidays] = STUFF((SELECT N', ' +  [RegionalHolidayName]
						FROM wLinkedHolidays AS p2
						WHERE p2.[h_Calendar_key] = p.[h_Calendar_key]
						ORDER BY [RegionalHolidayName]
						FOR XML PATH(N'')), 1, 2, N'')
      ,[RegionalObservedHolidays] = STUFF((SELECT N', ' +  [RegionalObservedHolidayName]
						FROM wLinkedHolidays AS p2
						WHERE p2.[h_Calendar_key] = p.[h_Calendar_key]
						ORDER BY [regionalObservedHolidayName]
						FOR XML PATH(N'')), 1, 2, N'')
FROM wLinkedHolidays AS p
GROUP BY [h_Calendar_key])
SELECT DateCode = cast(replace(hCalendar.[DateKey], '-', '') as int)
	  ,hCalendar.[DateKey]
      ,sCalendar.[DateFullName]      
      ,sCalendar.[DateLocalisedString]
      ,sCalendar.[DayNumberOfMonth]
      ,sCalendar.[DayNumberOfYear]
      ,sCalendar.[FullDateAlternateKey]
      ,sCalendar.[IsWeekDayCode]
      ,sCalendar.[IsWeekDayDescription]
      ,sCalendar.[MonthLocalisedString]
      ,sCalendar.[MonthName]
      ,sCalendar.[MonthNumberOfYearCode]
      ,sCalendar.[MonthYearCode]
      ,sCalendar.[MonthYearDescription]
      ,sCalendar.[NumberOfDaysInTheMonth]
      ,sCalendar.[QuarterCode]
      ,sCalendar.[QuarterDescription]
      ,sCalendar.[QuarterYearCode]
      ,sCalendar.[WeekDay]
      ,sCalendar.[WeekDayName]
      ,sCalendar.[YearCode]
      ,sCalendar.[YearDescription]
	  ,ABS(DATEDIFF(Year ,getdate(),hCalendar.[DateKey])) as [YearOffset]
	  ,ABS(DATEDIFF(Month,getdate(),hCalendar.[DateKey])) as [MonthOffset]
	  ,ABS(DATEDIFF(Day  ,getdate(),hCalendar.[DateKey])) as [DayOffset]
	  ,sFiscal.[FiscalYearCode]
	  ,sFiscal.[FiscalYearDescription]
	  ,sFiscal.[FiscalQuarterCode]
	  ,sFiscal.[FiscalQuarterDescription]
	  ,sFiscal.[FiscalQuarterYearCode]
	  ,sFiscal.[FiscalMonthCode]
	  ,sFiscal.[FiscalMonthYearCode]
	  ,ABS(sFiscal.[FiscalYearCode] - wCurFiscalYear.[FiscalYearCode]) as [FiscalYearOffset]
	  ,wFlatHolidays.[NationalHolidays]
	  ,wFlatHolidays.[NationalObservedHolidays]
	  ,wFlatHolidays.[RegionalHolidays]
	  ,wFlatHolidays.[RegionalObservedHolidays]
  FROM hCalendar
  LEFT JOIN sCalendar		on sCalendar.[h_Calendar_key]		= hCalendar.[h_Calendar_key]
  LEFT JOIN sFiscal			on sFiscal.[h_Calendar_key]			= hCalendar.[h_Calendar_key]
  LEFT JOIN wFlatHolidays	on wFlatHolidays.[h_Calendar_key]	= hCalendar.[h_Calendar_key]
  CROSS APPLY wCurFiscalYear


GO