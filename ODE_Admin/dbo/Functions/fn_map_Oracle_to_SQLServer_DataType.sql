CREATE FUNCTION [dbo].[fn_map_Oracle_to_SQLServer_DataType]
(	
	-- The parameters for this function are the common four that we can obtain
	-- (not all of them will be populated) for a source datatype.
	-- The data types of the parameters should match those that will be returned
	-- from the Oracle all_tab_columns table (obviously converted into SQL server equivalents).
	@dataType VARCHAR(106), 
	@dataLength INT,
	@dataPrecision INT,
	@dataScale INT
)
RETURNS TABLE 
AS
RETURN 
(
	-- The following is a very simplistic approach to mapping the Oracle data types.  This
	-- will probably require re-work in the near future.
	SELECT 
		CASE (@dataType)
			WHEN 'CHAR'		THEN 'NCHAR' 
			WHEN 'DATE'		THEN 'DATETIME2'
				-- At the moment I'm proposing DATETIME2 as it has a better range to match 
				-- source Oracle data, this does need specific 
			WHEN 'FLOAT'	THEN 'VARCHAR'
			WHEN 'LONG'		THEN 'VARCHAR'
			WHEN 'NUMBER'	THEN 'VARCHAR' 
				-- Oracle sometimes doesn't provide Precision and Scale values for this datatype.
				-- This is going to VARCHAR rather than NUMERIC as we discovered that a numeric
				-- datatype without a Precision or Scale won't build a table in PSL/ODE.
			WHEN 'NVARCHAR2' THEN 'NVARCHAR'
			WHEN 'VARCHAR2'	 THEN 'NVARCHAR'
			ELSE 'NVARCHAR'  -- Default Catch all
		END	AS DataType, 

		-- For certain SQL Server Datatypes, we need to specify Size, Precision and Scale for the 
		-- ODE Configuration procedures to correctly generate a database table.
		
		CASE (@dataType)
			WHEN 'FLOAT'		THEN 22
			WHEN 'NUMBER'		THEN 22
			WHEN 'LONG'			THEN 22
			WHEN 'DATE'			THEN 7
			--Double up the length of character datatypes because we land them into Nvarchar 
			--and the way ODE works, we have to put the doubled length into the config to achieve the actual length of the fields
			WHEN 'NVARCHAR2'	THEN @dataLength * 2
			WHEN 'VARCHAR2'		THEN @dataLength * 2
			WHEN 'NCHAR'		THEN @dataLength * 2
			WHEN 'CHAR'			THEN @dataLength * 2
			WHEN 'VARCHAR'		THEN @dataLength * 2
			WHEN 'NVARCHAR'		THEN @dataLength * 2
			ELSE @dataLength
		END AS DataSize, 
		
		CASE (@dataType)
			WHEN 'FLOAT'	THEN 0
			WHEN 'NUMBER'	THEN 0
			WHEN 'LONG'		THEN 0
			WHEN 'DATE'		THEN 23
			ELSE @dataPrecision
		END AS DataPrecision, 

		CASE (@dataType)
			WHEN 'FLOAT'	THEN 0
			WHEN 'NUMBER'	THEN 0
			WHEN 'LONG'		THEN 0
			WHEN 'DATE'		THEN 3
			ELSE @dataScale 
		END AS DataScale
)

GO
