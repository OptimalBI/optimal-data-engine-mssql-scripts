CREATE PROCEDURE [Admin].[ODE_Get_List_Of_Tables_From_Source]
(
@source_system_name varchar(128) -- Should be the same as Linked server name
, @included_schemas varchar(8000) = 'dbo' -- Source tables schema. It is recommended to configure one schema at a time.
)
AS
BEGIN
SET NOCOUNT ON

DECLARE  
  @source_database_name	VARCHAR(128)
, @OPENQUERY			VARCHAR(MAX)
, @connection_name		VARCHAR(128)
, @connection_db_type	VARCHAR(128)
, @SourceTables			[dbo].[dv_table_list]

--Get values from Config
SELECT @source_database_name = source_database_name
, @connection_name = project_connection_name
FROM [$(ConfigDatabase)].[dbo].[dv_source_system]
WHERE source_system_name = @source_system_name

select @connection_db_type = connection_db_type
FROM [$(ConfigDatabase)].dbo.dv_connection
WHERE connection_name = @connection_name

--If the list of schemas provided, refine it to be used in the query
IF CHARINDEX(',', @included_schemas) > 0
SET @included_schemas = REPLACE(@included_schemas,',', ''''',''''')

--Prepare the query for getting a list of all tables from source
SET @OPENQUERY = 'SELECT TABLE_NAME FROM OPENQUERY (' + @source_system_name + ',  ''SELECT TABLE_NAME FROM ' 

IF @connection_db_type = 'MSSQLServer'
SET @OPENQUERY += @source_database_name + '.[INFORMATION_SCHEMA].[TABLES] WHERE TABLE_TYPE = ''''BASE TABLE'''' AND TABLE_SCHEMA IN (''''' + @included_schemas + ''''')' 
ELSE 
SET @OPENQUERY += 'all_tables WHERE OWNER IN (''''' + @included_schemas + ''''')' 

SET @OPENQUERY += ' '')'

INSERT INTO @SourceTables EXEC (@OPENQUERY)

--Show the list of those tables which are not configured yet to the screen
 SELECT ',(' + QUOTENAME(TABLE_NAME, '''') + ')' TableList
  FROM @SourceTables
  WHERE  NOT(
	TABLE_NAME IN (SELECT st.[source_table_nme] COLLATE SQL_Latin1_General_CP1_CS_AS -- ODE Config collation
	FROM [$(ConfigDatabase)].[dbo].[dv_source_table] st
	INNER JOIN [$(ConfigDatabase)].[dbo].[dv_source_system] ss ON ss.source_system_key = st.system_key
	WHERE ss.source_system_name = @source_system_name) 	
)
ORDER BY 1

END