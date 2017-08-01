



CREATE PROCEDURE [Admin].[ODE_Create_Data_Access_Functions_PIT_Single] (
 @vault_name		varchar(128)		
,@satellite_name	varchar(128)
,@func_SQL			nvarchar(max) OUTPUT		
)
AS
BEGIN
/**********************************************************************************************************
Creates a PIT (Point in Time) function for a supplied Satellite.
**********************************************************************************************************/
-- Working Storage
SET NOCOUNT ON
DECLARE @hub_key			INT
       ,@hub_select			NVARCHAR(max)
	   ,@sat_select			NVARCHAR(max)
	   ,@hub_columns		NVARCHAR(max)
	   ,@sat_columns		NVARCHAR(max)
	   ,@tech_columns		NVARCHAR(max)
	   ,@SQL				NVARCHAR(max)
	   ,@sat_alias			NVARCHAR(150)
	   ,@func_schema		VARCHAR(128)
	   ,@func_prefix		VARCHAR(128)
	   ,@func_suffix_pit	VARCHAR(128) 
	   ,@CDC_Action			VARCHAR(128)
	   ,@CDC_StartTime		VARCHAR(128)
	   ,@Version_Start_Date	VARCHAR(128)

	   ,@crlf				CHAR(2)	= CHAR(13) + CHAR(10)

SELECT @func_schema		= CAST([ODE_Config].[dbo].[fn_get_default_value] ('Schema', 'ODE_AccessFunction') as VARCHAR)
SELECT @func_prefix		= CAST([ODE_Config].[dbo].[fn_get_default_value] ('Prefix', 'ODE_AccessFunction') as VARCHAR)
SELECT @func_suffix_pit = CAST([ODE_Config].[dbo].[fn_get_default_value] ('Suffix_pit', 'ODE_AccessFunction') as VARCHAR)

SELECT @CDC_Action	= column_name
FROM [ODE_Config].[dbo].[dv_default_column]
WHERE [object_type] = 'CdcStgODE'	
AND [object_column_type] = 'CDC_Action'

SELECT @CDC_StartTime	= column_name
FROM [ODE_Config].[dbo].[dv_default_column]
WHERE [object_type] = 'CdcStgODE'	
AND [object_column_type] = 'CDC_StartDate'

SELECT @Version_Start_Date = column_name
FROM [ODE_Config].[dbo].[dv_default_column]
WHERE [object_type] = 'Sat'	
AND [object_column_type] = 'Version_Start_Date'

SET @tech_columns = ' ' + QUOTENAME(@CDC_Action) + ' = ''X'''+ @crlf
SET @tech_columns += ',' + QUOTENAME(@CDC_StartTime) + ' = ' + QUOTENAME(@Version_Start_Date) + @crlf
SET @tech_columns += ','

SELECT @hub_key = h.hub_key
FROM [ODE_Config].[dbo].[dv_hub] h 
INNER JOIN [ODE_Config].[dbo].[dv_satellite] s ON s.hub_key = h.hub_key
WHERE s.satellite_database = @vault_name AND satellite_name = @satellite_name

SELECT @hub_select = ' FROM ' + QUOTENAME(@vault_name) + '.' + QUOTENAME(h.hub_schema) + '.' + QUOTENAME([ODE_Config].[dbo].[fn_get_object_name](h.hub_name, 'hub')) + ' [h]'
FROM [ODE_Config].[dbo].[dv_hub] h 
WHERE h.hub_key = @hub_key

SET @hub_columns = ''
SELECT @hub_columns += ',[h].' + QUOTENAME(hkc.hub_key_column_name)
FROM [ODE_Config].[dbo].[dv_hub] h 
INNER JOIN [ODE_Config].[dbo].[dv_hub_key_column]  hkc ON hkc.[hub_key] = h.[hub_key]
WHERE h.hub_key = @hub_key

SET @sat_select = ''
--SET @where = ''
SELECT @sat_select +=' INNER JOIN ' + QUOTENAME(s.satellite_database) + '.' + QUOTENAME(s.satellite_schema)  + '.' + QUOTENAME([ODE_Config].[dbo].[fn_get_object_name](s.satellite_name, 'sat')) + ' ON [h].' + (SELECT column_name FROM [ODE_Config].[dbo].[fn_get_key_definition] (h.hub_name, 'hub')) 
                   + ' = ' + QUOTENAME([ODE_Config].[dbo].[fn_get_object_name](s.satellite_name, 'sat')) + '.' + (SELECT column_name FROM [ODE_Config].[dbo].[fn_get_key_definition] (h.hub_name, 'hub')) + @crlf 
				   + '      AND ' + replace(replace(replace([ODE_Config].[dbo].[fn_get_satellite_pit_statement] ('9999-01-01 00:00:00.0000000 +12:00'), '9999-01-01 00:00:00.0000000 +12:00', 'COALESCE(@pit, sysdatetimeoffset())'), '''', ''), '[dv_', QUOTENAME([ODE_Config].[dbo].[fn_get_object_name](s.satellite_name, 'sat'))+'.[dv_') + @crlf 
     -- ,@where += @crlf + ' AND ' + QUOTENAME([ODE_Config].[dbo].[fn_get_object_name](s.satellite_name, 'sat')) + '.' + (SELECT column_name FROM [ODE_Config].[dbo].[fn_get_key_definition] (h.hub_name, 'hub')) + ' IS NULL ' 
  FROM [ODE_Config].[dbo].[dv_satellite] s
  INNER JOIN [ODE_Config].[dbo].[dv_hub] h ON h.hub_key = s.hub_key 
  WHERE s.satellite_database = @vault_name AND satellite_name = @satellite_name
  --SET @where = ' WHERE NOT(' + right(@where, LEN(@where) - 5) + ')'

  SET @sat_columns = ''
  select @sat_columns += ',' + QUOTENAME([ODE_Config].[dbo].[fn_get_object_name](s.satellite_name, 'sat')) + '.' + QUOTENAME(sc.column_name) + @crlf
  FROM [ODE_Config].[dbo].[dv_satellite] s 
  INNER JOIN [ODE_Config].[dbo].[dv_satellite_column] sc on sc.satellite_key = s.satellite_key
  WHERE s.satellite_database = @vault_name AND satellite_name = @satellite_name
  AND sc.[satellite_col_key] NOT IN(
				SELECT ISNULL(c.[satellite_col_key], 0)
				FROM [ODE_Config].[dbo].[dv_hub] h
				INNER JOIN [ODE_Config].[dbo].[dv_hub_key_column] hkc on hkc.[hub_key] = h.[hub_key]
				INNER JOIN [ODE_Config].[dbo].[dv_hub_column] hc ON hc.[hub_key_column_key] = hkc.[hub_key_column_key]
				INNER JOIN [ODE_Config].[dbo].[dv_column] c ON c.[column_key] = hc.[column_key]
				WHERE s.satellite_database = @vault_name AND satellite_name = @satellite_name) 

--PRINT @sat_columns
SET @SQL = 'CREATE OR ALTER FUNCTION ' + QUOTENAME(@func_schema) + '.' + QUOTENAME(@func_prefix + @satellite_name + @func_suffix_pit) + '(@pit DATETIMEOFFSET(7) = NULL)
RETURNS TABLE 
AS
RETURN 
(SELECT ' + @tech_columns + RIGHT(@hub_columns, LEN(@hub_columns) - 1) + @crlf + @sat_columns + @hub_select + @sat_select + ')'
--PRINT @SQL
SET @func_SQL = @SQL
END