
/*
Query builder
	Helps with building select statements on Data Vault
	Join tables in a proper vault way, i.e. join hubs via links. If no link is found to join hubs, these hubs are not included to the result query
	Query builder doesn't know if it's a three-way (or more) link. List all three (or more) hubs in the input for data consistency.
	If no satellites provided for the hub, all the hub satellites will be added to the query
	Query builder puts all satellite columns and hub key columns to the select statement
*/

/*-------------------------------------------------
Input
---------------------------------------------------*/
USE [ODE_Config]

--List of hubs to be added to the query
DECLARE @Hub_List TABLE (hub_name varchar(128))
INSERT @Hub_List  VALUES ('Customer')
INSERT @Hub_List  VALUES ('Product')
INSERT @Hub_List  VALUES ('Sale')


--List of satellites to be added to the query
DECLARE @Satellite_List TABLE (satellite_name varchar(128))
INSERT @Satellite_List  VALUES ('Customer_Address')
INSERT @Satellite_List  VALUES ('Product')


/*------------------------------------------------------------------
Query builder
--------------------------------------------------------------------*/

SET NOCOUNT ON;
--Identify if all the satellites listed above have a hub. If not, add hub to the list of inputs.
DECLARE @Hub_sorted_List TABLE (hub_name varchar(128), hub_order int IDENTITY(1,1))
INSERT @Hub_sorted_List
SELECT hub_name FROM @Hub_List

INSERT @Hub_sorted_List
SELECT h.hub_name
FROM [dbo].[dv_satellite] s
JOIN [dbo].[dv_hub] h ON s.hub_key = h.hub_key
AND s.link_hub_satellite_flag = 'H'
WHERE s.satellite_name IN (SELECT satellite_name FROM @Satellite_List)
AND s.is_retired = 0
AND h.is_retired = 0
AND h.hub_name NOT IN (SELECT hub_name FROM @Hub_sorted_List)

--------------------------------------------------------------------------------------
--Declare all the variables required for processing

IF (SELECT COUNT(*) FROM @Hub_List) > 0 OR (SELECT COUNT(*) FROM @Satellite_List) > 0
BEGIN

DECLARE @Hub_Link TABLE (hub_key int, hub_name varchar(128), link_key int, link_name varchar(128))
DECLARE @Link_proc TABLE (link_key int, link_name varchar(128), link_database varchar(128), link_schema varchar(128), is_used bit)

--List of hubs for processing
DECLARE @Hub_List_proc TABLE (hub_key int, hub_name varchar(128), hub_order int,  hub_database varchar(128), hub_schema varchar(28), is_used bit)
INSERT @Hub_List_proc
SELECT confH.hub_key, confH.hub_name, h.hub_order, confH.hub_database, confH.hub_schema, 0
FROM @Hub_sorted_List h
JOIN [dbo].[dv_hub] confH
ON h.hub_name = confH.hub_name

DECLARE @crlf char(2) = CHAR(13) + CHAR(10)

--Declare query parts variables
DECLARE @SQL_CTE varchar(max)
SET @SQL_CTE = ';WITH '

DECLARE @SQL_Join varchar(max)
SET @SQL_Join = @crlf + 'FROM '

DECLARE @SQL_Select varchar(max)
DECLARE @Final_SQL varchar(max)

--declare processing variables
DECLARE @proc_link_key	int
,@proc_link_name		varchar(128)
,@proc_link_database	varchar(128)
,@proc_link_schema		varchar(128)
,@proc_sat_name			varchar(128)
,@proc_sat_key			int
,@proc_sat_database		varchar(128)
,@proc_sat_schema		varchar(128)
,@proc_hub_key			int
,@proc_hub_name			varchar(128)
,@proc_hub_database		varchar(128)
,@proc_hub_schema		varchar(128)
,@proc_hub_column		varchar(128)
,@proc_sat_column		varchar(128)

--Get defaults
, @d_hub_prefix			varchar(128)
, @d_hub_schema			varchar(128)
, @d_hub_key			varchar(128)
, @d_lnk_prefix			varchar(128)
, @d_lnk_schema			varchar(128)
, @d_lnk_key			varchar(128)
, @d_sat_prefix			varchar(128)
, @d_sat_schema			varchar(128)
, @d_sat_key			varchar(128)
, @d_Current_Row		varchar(128)
, @d_Tombstone_Ind		varchar(128)


SELECT 
@d_hub_prefix	= CAST([dbo].[fn_get_default_value] ('Prefix','Hub') AS varchar(128))
,@d_hub_schema	= CAST([dbo].[fn_get_default_value] ('Schema','Hub') AS varchar(128))
,@d_hub_key		= CAST([dbo].[fn_get_default_value] ('Suffix','HubSurrogate') AS varchar(128))
,@d_lnk_prefix	= CAST([dbo].[fn_get_default_value] ('Prefix','Lnk') AS varchar(128))
,@d_lnk_schema	= CAST([dbo].[fn_get_default_value] ('Schema','Lnk') AS varchar(128))
,@d_lnk_key		= CAST([dbo].[fn_get_default_value] ('Suffix','LnkSurrogate') AS varchar(128))
,@d_sat_prefix	= CAST([dbo].[fn_get_default_value] ('Prefix','Sat') AS varchar(128))
,@d_sat_schema	= CAST([dbo].[fn_get_default_value] ('Schema','Sat') AS varchar(128))
,@d_sat_key		= CAST([dbo].[fn_get_default_value] ('Suffix','SatSurrogate') AS varchar(128))
SELECT @d_Current_Row	= [column_name]		FROM [dbo].[dv_default_column]	WHERE object_type = 'Sat'	AND object_column_type = 'Current_Row'
SELECT @d_Tombstone_Ind = [column_name]		FROM [dbo].[dv_default_column]	WHERE object_type = 'Sat'	AND object_column_type = 'Tombstone_Indicator'
-----------------------------------------------------------------------------------------------------------------------------------------------------

-- If there's more than one hub in the list, identify the list of links for the listed hubs. Select them to the list
IF (SELECT COUNT(*) FROM @Hub_sorted_List) > 1
BEGIN
	INSERT @Hub_Link
	SELECT DISTINCT h.hub_key, h.hub_name, l.link_key, l.link_name 
	FROM [dbo].[dv_hub] h
	JOIN [dbo].[dv_hub_key_column] hk ON h.hub_key = hk.hub_key
	JOIN [dbo].[dv_hub_column] hc ON hk.hub_key_column_key = hc.hub_key_column_key
	JOIN [dbo].[dv_link_key_column] lc ON hc.link_key_column_key = lc.link_key_column_key
	JOIN [dbo].[dv_link] l ON lc.link_key = l.link_key
	WHERE [hub_name] IN (SELECT hub_name FROM @Hub_sorted_List)
	AND h.is_retired = 0 AND l.is_retired = 0

-- Make a subset from the input lists, only choose those records which belong to the context

--Exclude all the links which are not related to the input hubs
	DELETE FROM @Hub_Link
	WHERE link_key IN (
	SELECT link_key FROM @Hub_Link GROUP BY link_key HAVING COUNT(*) < 2)

--Show notification that not all the hubs could be included in the query
	IF (SELECT COUNT(*) FROM @Hub_List_proc WHERE hub_key NOT IN (SELECT hub_key FROM @Hub_Link)) > 0
	SELECT hub_name AS [Following tables are excluded from the query as could not be linked]
	FROM @Hub_List_proc
	WHERE hub_key NOT IN (SELECT hub_key FROM @Hub_Link)

--Exclude all the hubs which could not be linked (orphans)
	DELETE FROM @Hub_List_proc
	WHERE hub_key NOT IN (SELECT hub_key FROM @Hub_Link)

--Populate links list for processing
	INSERT @Link_proc
	SELECT link_key, link_name, link_database, link_schema, 0 FROM [dbo].[dv_link] l
	WHERE l.link_key IN (SELECT link_key FROM @Hub_Link)

END

---------------------------------------------------------------------------------------------------

--If there's no satellite provided for the hub, populate list with all hub's satellites
DECLARE @Sat_Hub_list TABLE (satellite_key int, satellite_name varchar(128), satellite_database varchar(128), satellite_schema varchar(128), hub_key int, hub_name varchar(128))

--Populate the list of all sats for processing with input sats
INSERT @Sat_Hub_list
SELECT s.satellite_key, s.satellite_name, s.satellite_database, s.satellite_schema, s.hub_key, h.hub_name
FROM [dbo].[dv_satellite] s
JOIN [dbo].[dv_hub] h
ON s.hub_key = h.hub_key
WHERE s.is_retired = 0
AND h.is_retired = 0
AND s.link_hub_satellite_flag = 'H'
AND s.satellite_name IN (SELECT satellite_name FROM @Satellite_List)

--Populate the list of processing sats with all the sats for the input hubs if none of the satellites for hub was clearly defined
INSERT @Sat_Hub_list
SELECT s.satellite_key, s.satellite_name, s.satellite_database, s.satellite_schema, s.hub_key, h.hub_name
FROM [dbo].[dv_satellite] s
JOIN [dbo].[dv_hub] h
ON s.hub_key = h.hub_key
WHERE s.is_retired = 0
AND h.is_retired = 0
AND s.link_hub_satellite_flag = 'H'
AND s.hub_key IN (SELECT hub_key FROM @Hub_List_proc) 
AND s.satellite_name NOT IN (SELECT satellite_name FROM @Sat_Hub_list)
AND h.hub_key NOT IN (SELECT hub_key FROM @Sat_Hub_list)

------------------------------------------------------------------------------------------
----Put first hub to the query. It's special because it's the beginning of the query

SELECT 
@proc_hub_name = hub_name,
@proc_hub_database = t1.hub_database,
@proc_hub_schema = t1.hub_schema,
@proc_hub_key = t1.hub_key
FROM @Hub_List_proc t1 WHERE t1.hub_order = 1

SET @SQL_CTE = @SQL_CTE + 'h' + @proc_hub_name + ' AS (SELECT * FROM [' + @proc_hub_database + '].[' + @proc_hub_schema + '].[' + @d_hub_prefix + @proc_hub_name + '])' 
SET @SQL_Join = @SQL_Join + 'h' + @proc_hub_name

--Flag hub as it is being used in code
UPDATE @Hub_List_proc
SET is_used = 1 WHERE hub_order = 1

------------------------------------------------------------------------------------------------------------------

--If there's more than one hub, add links to CTEs and join parts of the query.
IF (SELECT COUNT(*) FROM @Hub_Link) > 0
BEGIN

	WHILE (SELECT COUNT(*) FROM @Hub_List_proc WHERE is_used = 0) > 0
	BEGIN

		DECLARE curLinks CURSOR
		FOR 
		--cursor picks up next link where 
		--1 one of the hubs is being used
		--2 the link itself is not used
		--3 there's a hub for this link which is not used yet
		SELECT link_key, link_name, link_database, link_schema FROM @Link_proc 
		WHERE is_used = 0
		AND link_key IN (
		SELECT link_key FROM @Hub_Link 
		WHERE link_key IN (SELECT link_key FROM @Hub_List_proc t1 JOIN @Hub_Link t2 ON t1.hub_key = t2.hub_key WHERE is_used = 1)
		AND link_key IN (SELECT link_key FROM @Hub_List_proc t1 JOIN @Hub_Link t2 ON t1.hub_key = t2.hub_key WHERE is_used = 0)
		)

		OPEN curLinks

		FETCH NEXT
		FROM curLinks
		INTO @proc_link_key, @proc_link_name, @proc_link_database, @proc_link_schema

		WHILE @@FETCH_STATUS = 0
		BEGIN

---------------Add link to the query-------------------------------------------------------------------------------------

--Set link satellite variables
			SELECT @proc_sat_key = satellite_key
			, @proc_sat_name = satellite_name
			, @proc_sat_database = satellite_database
			, @proc_sat_schema = satellite_schema
			FROM [dbo].[dv_satellite]
			WHERE is_retired = 0
			AND link_hub_satellite_flag = 'L'
			AND link_key = @proc_link_key

			DECLARE @cur_hub_name varchar(128)

			--Get the hub key name for join
			SET @cur_hub_name = (SELECT TOP 1 t1.hub_name FROM @Hub_List_proc t1
			JOIN @Hub_Link t2 
			ON t1.hub_key = t2.hub_key
			AND t1.is_used = 1
			AND t2.link_key = @proc_link_key)

			SET @SQL_CTE = @SQL_CTE + @crlf + ', l' + @proc_link_name + ' AS (SELECT l.*	FROM [' + @proc_link_database + '].[' + @proc_link_schema + '].[' + @d_lnk_prefix + @proc_link_name + '] l' + @crlf +
							'  JOIN [' + @proc_sat_database + '].[' + @proc_sat_schema + '].[' + @d_sat_prefix + @proc_sat_name + '] s ' + @crlf +
							'  ON l.' + @d_lnk_prefix + @proc_link_name + @d_lnk_key + ' = s.' + @d_lnk_prefix + @proc_link_name + @d_lnk_key + ' WHERE s.' + @d_Current_Row + ' = 1 AND s.' + @d_Tombstone_Ind + ' = 0)'

			SET @SQL_Join = @SQL_Join + @crlf + 'LEFT JOIN l' + @proc_link_name + '		ON l' + @proc_link_name + '.' + @d_hub_prefix + @cur_hub_name + @d_hub_key + ' = h' + @cur_hub_name + '.' + @d_hub_prefix + @cur_hub_name + @d_hub_key

			--flag link as it is being used in code already
			UPDATE @Link_proc
			SET is_used = 1
			WHERE link_key = @proc_link_key

--  ---  ---  ---Add hubs for this link---  ----  --  --  --  --  --  --  ---  --

			--cursor hubs
			DECLARE curHubs CURSOR
			FOR 
			SELECT t1.hub_key, t1.hub_name, t2.hub_database, t2.hub_schema
			FROM @Hub_Link t1
			JOIN @Hub_List_proc t2
			ON t1.hub_key = t2.hub_key
			WHERE t1.link_key = @proc_link_key
			AND t2.is_used = 0

			OPEN curHubs

			FETCH NEXT
			FROM curHubs
			INTO @proc_hub_key, @proc_hub_name, @proc_hub_database, @proc_hub_schema

			WHILE @@FETCH_STATUS = 0
			BEGIN

				SET @SQL_CTE = @SQL_CTE + @crlf + ', h' + @proc_hub_name + ' AS (SELECT * FROM [' + @proc_hub_database + '].[' + @proc_hub_schema + '].[' + @d_hub_prefix + @proc_hub_name + '])'
				SET @SQL_Join = @SQL_Join + @crlf + 'LEFT JOIN h' + @proc_hub_name + '		ON h' + @proc_hub_name + '.' + @d_hub_prefix + @proc_hub_name + @d_hub_key + ' = l' + @proc_link_name + '.' + @d_hub_prefix + @proc_hub_name + @d_hub_key

				UPDATE @Hub_List_proc
				SET is_used = 1
				WHERE hub_key = @proc_hub_key

				FETCH NEXT
				FROM curHubs
				INTO @proc_hub_key, @proc_hub_name, @proc_hub_database, @proc_hub_schema
			END

			CLOSE curHubs
			DEALLOCATE curHubs
--     --    --    --    --    --    --    --      --    --     --     --     --     --     --     --
--close links cursor
		FETCH NEXT
		FROM curLinks
		INTO @proc_link_key, @proc_link_name, @proc_link_database, @proc_link_schema
	END

CLOSE curLinks
DEALLOCATE curLinks
END

END
-----------------------------------------------------------------------------------------------------------
--Add all satellites to the CTEs and Join query


SET @SQL_CTE = @SQL_CTE + @crlf + '--------SATELLITES' + @crlf

--cursor satellites
DECLARE curSats CURSOR
FOR 

SELECT satellite_key, satellite_name, satellite_database, hub_name, satellite_schema
FROM @Sat_Hub_list
WHERE hub_key IS NOT NULL

OPEN curSats

FETCH NEXT
FROM curSats
INTO @proc_sat_key, @proc_sat_name, @proc_sat_database, @proc_hub_name, @proc_sat_schema

WHILE @@FETCH_STATUS = 0
BEGIN


	SET @SQL_CTE = @SQL_CTE  + ', s' + @proc_sat_name + ' AS (SELECT *	FROM [' + @proc_sat_database + '].[' + @proc_sat_schema + '].[' + @d_sat_prefix + @proc_sat_name + '] WHERE '+ @d_Current_Row + ' = 1 AND ' + @d_Tombstone_Ind + ' = 0)' + @crlf

	SET @SQL_Join = @SQL_Join + @crlf + 'LEFT JOIN s' + @proc_sat_name + '		ON s' + @proc_sat_name + '.' + @d_hub_prefix + @proc_hub_name + @d_hub_key + ' = h' + @proc_hub_name + '.' + @d_hub_prefix + @proc_hub_name + @d_hub_key

	FETCH NEXT
	FROM curSats
	INTO @proc_sat_key, @proc_sat_name, @proc_sat_database, @proc_hub_name, @proc_sat_schema
END

CLOSE curSats
DEALLOCATE curSats

----------------------------------------------------------------------

--Add hub keys columns to the select statement
--cursor
DECLARE curHubColumns CURSOR
FOR 
SELECT h.hub_name, hc.hub_key_column_name FROM
[dbo].[dv_hub_key_column] hc
JOIN [dbo].[dv_hub] h
ON hc.hub_key = h.hub_key
WHERE hc.hub_key IN (SELECT hub_key FROM @Hub_List_proc)

OPEN curHubColumns

FETCH NEXT
FROM curHubColumns
INTO @proc_hub_name, @proc_hub_column

WHILE @@FETCH_STATUS = 0
BEGIN

	IF @SQL_Select IS NULL
	SET @SQL_Select = @crlf + 'SELECT h' + @proc_hub_name + '.[' + @proc_hub_column + ']'
	ELSE
	SET @SQL_Select = @SQL_Select + @crlf + ', h' + @proc_hub_name + '.[' + @proc_hub_column + ']'

	FETCH NEXT
	FROM curHubColumns
	INTO @proc_hub_name, @proc_hub_column
END

CLOSE curHubColumns
DEALLOCATE curHubColumns

--------------------------------------------------------------------------------------------------
--Add all satellite columns to the select statement

--cursor satellite columns

DECLARE curSatColumns CURSOR
FOR 
SELECT c1.satellite_key, c3.satellite_name, c1.column_name 
FROM [dbo].[dv_satellite_column] c1
--JOIN [dbo].[dv_column] c2
--ON c1.column_key = c2.column_key
JOIN [dbo].[dv_satellite] c3
ON c1.satellite_key = c3.satellite_key
WHERE c1.satellite_key IN (SELECT satellite_key FROM @Sat_Hub_list)
--AND c2.is_retired = 0


OPEN curSatColumns

FETCH NEXT
FROM curSatColumns
INTO @proc_sat_key, @proc_sat_name, @proc_sat_column

WHILE @@FETCH_STATUS = 0
BEGIN

	SET @SQL_Select = @SQL_Select + @crlf + ', s' + @proc_sat_name + '.[' + @proc_sat_column + ']'

	FETCH NEXT
	FROM curSatColumns
	INTO @proc_sat_key, @proc_sat_name, @proc_sat_column
END

CLOSE curSatColumns
DEALLOCATE curSatColumns

----------------------------------------------------------------

-- Put all parts of the query together and output to the screen

SET @Final_SQL = @SQL_CTE + @SQL_Select + @SQL_Join

PRINT @Final_SQL
END
ELSE
PRINT 'Provide at least one table to build a query!'
------------------------------------------------------------------------------