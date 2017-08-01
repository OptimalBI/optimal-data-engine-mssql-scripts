


CREATE PROCEDURE [Admin].[ODE_Build_Vault_Object_Create_Statements]
--
(
--********************************************************************************************************************************************************************
		@ReleaseNumber int = -1 --2016080304
       -- Set to -1 to look for Objects for all Releases. If you provide a Release Number (eg. 2016080302), the script will only look for Objects in your chosen Release.
       --
       ,@Rebuild char(3) = 'N'
	   -- Be VERY careful - setting this parameter to "Yes" will generate statements, which may cause data loss.
	   -- Setting @Rebuild to "Yes" and @ReleaseNumber to -1 will generate statements to Rebuild ALL of your Vault Objects!!!.

	   -- "N" will generate statements to Create Objects, which do not already exist in the Vault. 
	   -- "Yes" will generate statements to recreate existing Objects plus create all missing Objects.
       --
--********************************************************************************************************************************************************************
) AS
BEGIN
SET NOCOUNT ON


/********************************************
Begin:
********************************************/
--

--BEGIN TRANSACTION;
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
/*
	This script can be used to generate Create statements for ODE Objects (Hubs, Links and Satellites).
	Generally, this script is used to identify missing objects and build the statements needed to Create them.
	This is useful when developing new objects, or releasing them to new environments (say Production).
	Optionally, this script can be used to Rebuild all or part (by Relesae Number) of your Vault.
	Be very careful when using this option - it will generate scripts to recreate all existing Hubs, Links and Satellites!
	Output of this script is a list SQL statements. 
	Run them to create missing tables in Data Vault.
*/

SET NOCOUNT ON;

-- Working Storage
declare @ReleaseKey int


if @ReleaseNumber != 0 select @ReleaseKey = [release_key] from [ODE_Config].[dv_release].[dv_release_master] where [release_number] = @ReleaseNumber  
                else set @ReleaseKey = 0

if @Rebuild = 'Yes'
begin
    PRINT '-------------------------------------------------------------------------------------------'; 
	PRINT '--You have selected to generate statements which will Rebuild all or part of your Vault';
	PRINT '--This could cause data loss by recreating existing Hubs Links and Satellites.';
	PRINT '--Is this what you require?';
	PRINT '-------------------------------------------------------------------------------------------'; 
	PRINT ' ';
end
PRINT '------------------';
PRINT '--Build Hub Tables';
PRINT '------------------';
DECLARE @SQL NVARCHAR(MAX)
, @SQLOUT NVARCHAR(1000)
, @ParmDefinition NVARCHAR(500);
SET @ParmDefinition = N'@SQLOutVal NVARCHAR(1000) OUTPUT';
DECLARE hub_cursor CURSOR
FOR SELECT 
case when @Rebuild = 'Yes' then 
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_hub_table] '''''+[hub_database]+''''','''''+[hub_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([hub_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([hub_name], 'Hub'
)+''')' +
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_hub_table] '''''+[hub_database]+''''','''''+[hub_name]+''''',''''Y''''''
where exists (select 1 from '+QUOTENAME([hub_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([hub_name], 'Hub'
)+''')' 
else 
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_hub_table] '''''+[hub_database]+''''','''''+[hub_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([hub_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([hub_name], 'Hub'
)+''')'
end
FROM [ODE_Config].[dbo].[dv_hub]
WHERE [hub_key] > 0
and [release_key] = case when @ReleaseKey != 0 then [release_key] else @ReleaseKey end;
OPEN hub_cursor;
FETCH NEXT FROM hub_cursor INTO @SQL;
WHILE @@Fetch_Status = 0
BEGIN
SET @SQLOUT = NULL;
EXEC [sp_executesql]
@SQL
, @ParmDefinition
, @SQLOutVal = @SQLOUT OUTPUT;
IF @SQLOUT IS NOT NULL
PRINT @SQLOUT;
FETCH NEXT FROM hub_cursor INTO @SQL;
END;
CLOSE hub_cursor;
DEALLOCATE hub_cursor;
PRINT '';
PRINT '-------------------';
PRINT '--Build Link Tables';
PRINT '-------------------';
DECLARE link_cursor CURSOR
FOR SELECT 
case when @Rebuild = 'Yes' then 
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_link_table] '''''+[link_database]+''''','''''+[link_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([link_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([link_name], 'Lnk'
)+''')' +
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_link_table] '''''+[link_database]+''''','''''+[link_name]+''''',''''Y''''''
where  exists (select 1 from '+QUOTENAME([link_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([link_name], 'Lnk'
)+''')'
else
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_link_table] '''''+[link_database]+''''','''''+[link_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([link_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([link_name], 'Lnk'
)+''')'
end
FROM
[ODE_Config].[dbo].[dv_link]
WHERE [link_key] > 0
and [release_key] = case when @ReleaseKey != 0 then [release_key] else @ReleaseKey end;;
OPEN link_cursor;
FETCH NEXT FROM link_cursor INTO @SQL;
WHILE @@Fetch_Status = 0
BEGIN
SET @SQLOUT = NULL;
EXEC [sp_executesql]
@SQL
, @ParmDefinition
, @SQLOutVal = @SQLOUT OUTPUT;
IF @SQLOUT IS NOT NULL
PRINT @SQLOUT;
FETCH NEXT FROM link_cursor INTO @SQL;
END;
CLOSE link_cursor;
DEALLOCATE link_cursor;
PRINT '';
PRINT '------------------';
PRINT '--Build Sat Tables';
PRINT '------------------';
DECLARE sat_cursor CURSOR
FOR SELECT 
case when @Rebuild = 'Yes' then
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_sat_table] '''''+[satellite_database]+''''','''''+[satellite_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([satellite_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([satellite_name], 'Sat'
)+''')'+
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_sat_table] '''''+[satellite_database]+''''','''''+[satellite_name]+''''',''''Y''''''
where exists (select 1 from '+QUOTENAME([satellite_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([satellite_name], 'Sat'
)+''')'
else  
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_sat_table] '''''+[satellite_database]+''''','''''+[satellite_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([satellite_database])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([satellite_name], 'Sat'
)+''')'
end
FROM
[ODE_Config].[dbo].[dv_satellite]
WHERE [satellite_key] > 0
and [release_key] = case when @ReleaseKey != 0 then [release_key] else @ReleaseKey end;;
OPEN sat_cursor;
FETCH NEXT FROM sat_cursor INTO @SQL;
WHILE @@Fetch_Status = 0
BEGIN
SET @SQLOUT = NULL;
EXEC [sp_executesql]
@SQL
, @ParmDefinition
, @SQLOutVal = @SQLOUT OUTPUT;
IF @SQLOUT IS NOT NULL
PRINT @SQLOUT;
FETCH NEXT FROM sat_cursor INTO @SQL;
END;
CLOSE sat_cursor;
DEALLOCATE sat_cursor;
PRINT '';
PRINT '------------------';
PRINT '--Build Stage Tables';
PRINT '------------------';
DECLARE stage_cursor CURSOR
FOR SELECT 
case when @Rebuild = 'Yes' then
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_stage_table] '''''+[stage_table_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([stage_database_name])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([stage_table_name], 'Stg'
)+''')'+
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_stage_table] '''''+[stage_table_name]+''''',''''Y''''''
where exists (select 1 from '+QUOTENAME([stage_database_name])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([stage_table_name], 'Stg'
)+''')'
else  
'select @SQLOutVal = ''EXECUTE [ODE_Config].[dbo].[dv_create_stage_table] '''''+[stage_table_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([stage_database_name])+'.[information_schema].[tables] where table_name = ''' + [ODE_Config].[dbo].[fn_get_object_name]
([stage_table_name], 'Stg'
)+''')'
end
FROM
[ODE_Config].[dbo].[dv_source_table] st
INNER JOIN [ODE_Config].[dbo].[dv_stage_schema] ssc ON ssc.stage_schema_key = st.stage_schema_key
INNER JOIN [ODE_Config].[dbo].[dv_stage_database] sd ON sd.stage_database_key = ssc.stage_database_key
WHERE [source_table_key] > 0
and st.[release_key] = case when @ReleaseKey != 0 then st.[release_key] else @ReleaseKey end;;
OPEN stage_cursor;
FETCH NEXT FROM stage_cursor INTO @SQL;
WHILE @@Fetch_Status = 0
BEGIN
SET @SQLOUT = NULL;
EXEC [sp_executesql]
@SQL
, @ParmDefinition
, @SQLOutVal = @SQLOUT OUTPUT;
IF @SQLOUT IS NOT NULL
PRINT @SQLOUT;
FETCH NEXT FROM stage_cursor INTO @SQL;
END;
CLOSE stage_cursor;
DEALLOCATE stage_cursor;
PRINT '';
PRINT '--succeeded';
-- Commit if successful:
--COMMIT;
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