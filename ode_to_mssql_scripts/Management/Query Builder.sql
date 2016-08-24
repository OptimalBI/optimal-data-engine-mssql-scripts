
/*
Query builder
	Helps with building select statements on Data Vault
	Join tables in a proper vault way, i.e. join hubs via links. If no link is found to join hubs, these hubs are not included to the result query
	Query builder doesn't know if it's a three-way (or more) link. List all three (or more) hubs in the input for data consistency.
	If no satellites provided for the hub, all the hub satellites will be added to the query
*/

/*-------------------------------------------------
Input
---------------------------------------------------*/
USE [ODE_Config]

--List of hubs to be added to the query
DECLARE @Hub_List TABLE (hub_name varchar(128))
INSERT @Hub_List  VALUES ('Person')
INSERT @Hub_List  VALUES ('PersonPhone')
INSERT @Hub_List  VALUES ('PhoneType')


--List of satellites to be added to the query
DECLARE @Satellite_List TABLE (satellite_name varchar(128))
INSERT @Satellite_List  VALUES ('Person_Person')
INSERT @Satellite_List  VALUES ('Person_PersonPhone')


/*------------------------------------------------------------------
Query builder
--------------------------------------------------------------------*/

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
DECLARE @Link_proc TABLE (link_key int, link_name varchar(128), link_database varchar(128), is_used bit)

--List of hubs for processing
DECLARE @Hub_List_proc TABLE (hub_key int, hub_name varchar(128), hub_order int,  hub_database varchar(128), is_used bit)
INSERT @Hub_List_proc
SELECT confH.hub_key, confH.hub_name, h.hub_order, confH.hub_database, 0
FROM @Hub_sorted_List h
JOIN [dbo].[dv_hub] confH
ON h.hub_name = confH.hub_name

--Declare query parts variables
DECLARE @SQL_CTE varchar(max)
SET @SQL_CTE = ';WITH '

DECLARE @SQL_Join varchar(max)
SET @SQL_Join = '

FROM '

DECLARE @SQL_Select varchar(max)
SET @SQL_Select = '

SELECT '

DECLARE @Final_SQL varchar(max)

--declare processing variables
DECLARE @proc_link_key	int
,@proc_link_name		varchar(128)
,@proc_link_database	varchar(128)
,@proc_sat_name			varchar(128)
,@proc_sat_key			int
,@proc_sat_database		varchar(128)
,@proc_hub_key			int
,@proc_hub_name			varchar(128)
,@proc_hub_database		varchar(128)
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
, @d_Tombstone_Indicator varchar(128)


SELECT @d_hub_prefix	= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'Hub'			AND default_subtype = 'Prefix'
SELECT @d_hub_schema	= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'Hub'			AND default_subtype = 'Schema'
SELECT @d_hub_key		= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'HubSurrogate'	AND default_subtype = 'Suffix'
SELECT @d_lnk_prefix	= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'Lnk'			AND default_subtype = 'Prefix'
SELECT @d_lnk_schema	= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'Lnk'			AND default_subtype = 'Schema'
SELECT @d_lnk_key		= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'LnkSurrogate'	AND default_subtype = 'Suffix'
SELECT @d_sat_prefix	= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'Sat'			AND default_subtype = 'Prefix'
SELECT @d_sat_schema	= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'Sat'			AND default_subtype = 'Schema'
SELECT @d_sat_key		= [default_varchar] FROM [dbo].[dv_defaults]		WHERE default_type = 'SatSurrogate'	AND default_subtype = 'Suffix'
SELECT @d_Current_Row	= column_name		FROM [dbo].[dv_default_column]	WHERE object_type = 'Sat'			AND object_column_type = 'Current_Row'
SELECT @d_Tombstone_Indicator = column_name FROM [dbo].[dv_default_column]	WHERE object_type = 'Sat'			AND object_column_type = 'Tombstone_Indicator'
-----------------------------------------------------------------------------------------------------------------------------------------------------

-- If there's more than one hub in the list, identify the list of links for the listed hubs. Select them to the list
IF (SELECT COUNT(*) FROM @Hub_sorted_List) > 1
BEGIN
	INSERT @Hub_Link
	SELECT h.hub_key, h.hub_name, l.link_key, l.link_name FROM [ODE_Config].[dbo].[dv_hub] h
	JOIN [dbo].[dv_hub_link] hl ON h.hub_key = hl.hub_key
	JOIN [dbo].[dv_link] l ON hl.link_key = l.link_key
	WHERE [hub_name] IN (SELECT hub_name FROM @Hub_sorted_List)
	AND h.is_retired = 0 AND l.is_retired = 0

-- make a subset from the list of links, only choose those which are listed twice or more times

--Remove all the rows where links are not related to the input hubs
	DELETE FROM @Hub_Link
	WHERE link_key IN (
	SELECT link_key FROM @Hub_Link GROUP BY link_key HAVING COUNT(*) < 2)

--Remove all the hubs which could not be linked (orphans)
	DELETE FROM @Hub_List_proc
	WHERE hub_key NOT IN (SELECT hub_key FROM @Hub_Link)

--populate links list for processing
	INSERT @Link_proc
	SELECT link_key, link_name, link_database, 0 FROM [dbo].[dv_link] l
	WHERE l.link_key IN (SELECT link_key FROM @Hub_Link)

END
----------------------------------------------------------------------------------------------------
--a list of hub key columns to be added to the select statement.
DECLARE @Hub_column_list TABLE (hub_key int, hub_column_name varchar(128))
INSERT @Hub_column_list
SELECT hub_key, hub_key_column_name FROM
[dbo].[dv_hub_key_column]
WHERE hub_key IN (SELECT hub_key FROM @Hub_List_proc)

---------------------------------------------------------------------------------------------------

--If there's no satellite provided, populate list with all hub's satellites
DECLARE @Sat_Hub_list TABLE (satellite_key int, satellite_name varchar(128), satellite_database varchar(128), hub_key int, hub_name varchar(128))

--pick up hub satellites to the processing list
INSERT @Sat_Hub_list
SELECT s.satellite_key, s.satellite_name, s.satellite_database,
s.hub_key, h.hub_name
FROM [dbo].[dv_satellite] s
JOIN [dbo].[dv_hub] h
ON s.hub_key = h.hub_key
WHERE s.is_retired = 0
AND h.is_retired = 0
AND s.link_hub_satellite_flag = 'H'
--populate a list of all sats for processing with input sats
AND ((s.satellite_name IN (SELECT satellite_name FROM @Satellite_List))
--populate a list of processing sats with all the sats for the input hubs if none of the satellites for hub was clearly defined
OR (s.hub_key IN (SELECT hub_key FROM @Hub_List_proc) 
AND s.satellite_name NOT IN (SELECT satellite_name FROM @Sat_Hub_list)
AND h.hub_key NOT IN (SELECT hub_key FROM @Sat_Hub_list))
)

-------------------------------------------------------------------------------------------

--list of hub satellite columns
DECLARE @Sat_column_list TABLE (sat_key int, sat_name varchar(128), sat_column_name varchar(128))
insert @Sat_column_list
SELECT c1.satellite_key, c3.satellite_name, c2.column_name FROM
[dbo].[dv_satellite_column] c1
JOIN [dbo].[dv_column] c2
on c1.column_key = c2.column_key
JOIN [dbo].[dv_satellite] c3
on c1.satellite_key = c3.satellite_key
WHERE c1.satellite_key IN (SELECT satellite_key FROM @Sat_Hub_list)
AND c2.is_retired = 0

----------------------------------------------------------------------------------------------

--list of the links sats
DECLARE @Sat_Lnk_list TABLE (satellite_key int, satellite_name varchar(128), satellite_database varchar(128), link_key int, link_name varchar(128))

INSERT @Sat_Lnk_list
SELECT s.satellite_key, s.satellite_name, s.satellite_database,
l.link_key, l.link_name FROM [dbo].[dv_satellite] s
JOIN [dbo].[dv_link] l
ON s.link_key = l.link_key
WHERE s.is_retired = 0
AND l.is_retired = 0
AND s.link_hub_satellite_flag = 'L'
AND s.link_key IN (SELECT link_key FROM @Hub_Link)
------------------------------------------------------------------------------------------

----Put first hub to the query. It's special beacuse it's the beginning of the query

SELECT 
@proc_hub_name = hub_name,
@proc_hub_database = t1.hub_database,
@proc_hub_key = t1.hub_key
FROM @Hub_List_proc t1 WHERE t1.hub_order = 1

SET @SQL_CTE = @SQL_CTE + 'h' + @proc_hub_name + ' AS (SELECT *		FROM [' + @proc_hub_database + '].[' + @d_hub_schema + '].[' + @d_hub_prefix + @proc_hub_name + '])'
SET @SQL_Join = @SQL_Join + 'h' + @proc_hub_name

--add first hub key fields to select statement
DECLARE curHubColumns CURSOR
FOR 

SELECT hub_column_name
FROM @Hub_column_list
WHERE hub_key = @proc_hub_key

OPEN curHubColumns

FETCH NEXT
FROM curHubColumns
INTO @proc_hub_column

WHILE @@FETCH_STATUS = 0
BEGIN

SET @SQL_Select = @SQL_Select + '
h' + @proc_hub_name + '.[' + @proc_hub_column + ']'

FETCH NEXT
FROM curHubColumns
INTO @proc_hub_column
END

CLOSE curHubColumns
DEALLOCATE curHubColumns


--Update flag that hub is being used in code
UPDATE @Hub_List_proc
SET is_used = 1 WHERE hub_order = 1

------------------------------------------------------------------------------------------------------------------

--If there's more than one hub, select a link. Add it to CTEs and join parts of the query.
IF (SELECT COUNT(*) FROM @Hub_Link) > 0
BEGIN

	WHILE (SELECT COUNT(*) FROM @Hub_List_proc WHERE is_used = 0) > 0
	BEGIN

		DECLARE curLinks CURSOR
		FOR 
		--cursor picks up next link where 
		--1 one of the hubs is being used
		--2 link itself is not used
		--3 there's a hub for this link which is not used yet
		SELECT link_key, link_name, link_database FROM @Link_proc 
		WHERE is_used = 0
		AND link_key IN (
		SELECT link_key FROM @Hub_Link 
		WHERE link_key IN (SELECT link_key FROM @Hub_List_proc t1 JOIN @Hub_Link t2 on t1.hub_key = t2.hub_key WHERE is_used = 1)
		AND link_key IN (SELECT link_key FROM @Hub_List_proc t1 JOIN @Hub_Link t2 on t1.hub_key = t2.hub_key WHERE is_used = 0)
		)

		OPEN curLinks

		FETCH NEXT
		FROM curLinks
		INTO @proc_link_key, @proc_link_name, @proc_link_database

		WHILE @@FETCH_STATUS = 0
		BEGIN

---------------Add link to the query-------------------------------------------------------------------------------------
--Set link satellite variables
			SELECT @proc_sat_key = satellite_key
			, @proc_sat_name = satellite_name
			, @proc_sat_database = satellite_database
			FROM @Sat_Lnk_list
			WHERE link_key = @proc_link_key

			DECLARE @cur_hub_name varchar(128)

			--Get the hub key name for join
			SET @cur_hub_name = (SELECT TOP 1 t1.hub_name FROM @Hub_List_proc t1
			JOIN @Hub_Link t2 
			ON t1.hub_key = t2.hub_key
			AND t1.is_used = 1
			AND t2.link_key = @proc_link_key)

			SET @SQL_CTE = @SQL_CTE + ' 
, l' + @proc_link_name + ' AS (SELECT l.*	FROM [' + @proc_link_database + '].[' + @d_lnk_schema + '].[' + @d_lnk_prefix + @proc_link_name + '] l
	JOIN [' + @proc_sat_database + '].[' + @d_sat_schema + '].[' + @d_sat_prefix + @proc_sat_name + '] s 
	ON l.' + @d_lnk_prefix + @proc_link_name + @d_lnk_key + ' = s.' + @d_lnk_prefix + @proc_link_name + @d_lnk_key + ' WHERE s.' + @d_Current_Row + ' = 1 AND s.' + @d_Tombstone_Indicator + ' = 0)'

			SET @SQL_Join = @SQL_Join + '
LEFT JOIN l' + @proc_link_name + '		ON l' + @proc_link_name + '.' + @d_hub_prefix + @cur_hub_name + @d_hub_key + ' = h' + @cur_hub_name + '.' + @d_hub_prefix + @cur_hub_name + @d_hub_key

			--update flags that link is being used in code already
			UPDATE @Link_proc
			SET is_used = 1
			WHERE link_key = @proc_link_key

--  ---  ---  ---Add hubs for this link---  ----  --  --  --  --  --  --  ---  --

			--cursor hubs
			DECLARE curSHubs CURSOR
			FOR 
			SELECT t1.hub_key, t1.hub_name, t2.hub_database
			FROM @Hub_Link t1
			JOIN @Hub_List_proc t2
			ON t1.hub_key = t2.hub_key
			WHERE t1.link_key = @proc_link_key
			AND t2.is_used = 0

			OPEN curSHubs

			FETCH NEXT
			FROM curSHubs
			INTO @proc_hub_key, @proc_hub_name, @proc_hub_database

			WHILE @@FETCH_STATUS = 0
			BEGIN

				SET @SQL_CTE = @SQL_CTE + '
, h' + @proc_hub_name + ' AS (SELECT *	FROM [' + @proc_hub_database + '].[' + @d_hub_schema + '].[' + @d_hub_prefix + @proc_hub_name + '])'
				SET @SQL_Join = @SQL_Join + '
LEFT JOIN h' + @proc_hub_name + '		ON h' + @proc_hub_name + '.' + @d_hub_prefix + @proc_hub_name + @d_hub_key + ' = l' + @proc_link_name + '.' + @d_hub_prefix + @proc_hub_name + @d_hub_key

				UPDATE @Hub_List_proc
				SET is_used = 1
				WHERE hub_key = @proc_hub_key

--    --    --    --    --    --    --    --    --    --    --    --    --    --    --
				--add hub key columns to the select statement
				--cursor
				DECLARE curHubColumns CURSOR
				FOR 

				SELECT hub_column_name
				FROM @Hub_column_list
				WHERE hub_key = @proc_hub_key

				OPEN curHubColumns

				FETCH NEXT
				FROM curHubColumns
				INTO @proc_hub_column

				WHILE @@FETCH_STATUS = 0
				BEGIN

					SET @SQL_Select = @SQL_Select + '
, h' + @proc_hub_name + '.[' + @proc_hub_column + ']'

					FETCH NEXT
					FROM curHubColumns
					INTO @proc_hub_column
				END

				CLOSE curHubColumns
				DEALLOCATE curHubColumns
--    ---    ---      ----     ---     --     --    --    --   --    ---    --     --     --
--close hubs cursor
				FETCH NEXT
				FROM curSHubs
				INTO @proc_hub_key, @proc_hub_name, @proc_hub_database
			END

			CLOSE curSHubs
			DEALLOCATE curSHubs
--     --    --    --    --    --    --    --      --    --     --     --     --     --     --     --
--close links cursor
		FETCH NEXT
		FROM curLinks
		INTO @proc_link_key, @proc_link_name, @proc_link_database
	END

CLOSE curLinks
DEALLOCATE curLinks
END

END
-----------------------------------------------------------------------------------------------------------
--Add all satellites to the CTE and Join query


	SET @SQL_CTE = @SQL_CTE + '
--------SATELLITES'

--cursor satellites

DECLARE curSats CURSOR
FOR 

SELECT satellite_key, satellite_name, satellite_database, hub_name
FROM @Sat_Hub_list
WHERE hub_key is NOT null

OPEN curSats

FETCH NEXT
FROM curSats
INTO @proc_sat_key, @proc_sat_name, @proc_sat_database, @proc_hub_name

WHILE @@FETCH_STATUS = 0
BEGIN


	SET @SQL_CTE = @SQL_CTE + '
, s' + @proc_sat_name + ' AS (SELECT *	FROM [' + @proc_sat_database + '].[' + @d_sat_schema + '].[' + @d_sat_prefix + @proc_sat_name + '] WHERE '+ @d_Current_Row + ' = 1 AND ' + @d_Tombstone_Indicator + ' = 0)'

	SET @SQL_Join = @SQL_Join + '
LEFT JOIN s' + @proc_sat_name + '		ON s' + @proc_sat_name + '.' + @d_hub_prefix + @proc_hub_name + @d_hub_key + ' = h' + @proc_hub_name + '.' + @d_hub_prefix + @proc_hub_name + @d_hub_key

	FETCH NEXT
	FROM curSats
	INTO @proc_sat_key, @proc_sat_name, @proc_sat_database, @proc_hub_name
END

CLOSE curSats
DEALLOCATE curSats

--------------------------------------------------------------------------------------------------
--Add all satellite columns to the select statement

--cursor satellite columns

DECLARE curSatColumns CURSOR
FOR 

SELECT sat_key, sat_name, sat_column_name
FROM @Sat_column_list


OPEN curSatColumns

FETCH NEXT
FROM curSatColumns
INTO @proc_sat_key, @proc_sat_name, @proc_sat_column

WHILE @@FETCH_STATUS = 0
BEGIN

					SET @SQL_Select = @SQL_Select + '
, s' + @proc_sat_name + '.[' + @proc_sat_column + ']'

	FETCH NEXT
	FROM curSatColumns
	INTO @proc_sat_key, @proc_sat_name, @proc_sat_column
END

CLOSE curSatColumns
DEALLOCATE curSatColumns

----------------------------------------------------------------

-- Put all parts of the query together and output to the screen

SET @Final_SQL = @SQL_CTE + @SQL_Select + @SQL_Join

SELECT @Final_SQL AS [Generated Vault Select statement]
END
ELSE
SELECT 'Provide at least one table to build a query' AS [Error Message]
------------------------------------------------------------------------------