
CREATE PROCEDURE [Admin].[ODE_Get_Metadata_For_Source_Table_List]
(
 @KeyDetectionType				VARCHAR(20)			= 'Primary' --Valid values are Primary, Unique, None
 
,@SourceSystemName              VARCHAR(128) 
,@SourceSchema					VARCHAR(128)			
,@SourceTables					[dbo].[dv_table_list] READONLY			

,@StageDatabase					VARCHAR(128)		--= e.g. 'ODE_Sales_Stage'
,@StageSchema					VARCHAR(128)		--= 'Stage'
,@StageLoadType					VARCHAR(50)         --= 'Full' or 'Delta' or 'MSSQLcdc' or 'ODEcdc'

,@VaultDatabase					VARCHAR(128)        --= e.g. 'ODE_Sales_Vault'

,@PrintCreateStatements			BIT		= 0			--0 means don't output the statements, 1 means output the create statements
,@SprintDate					CHAR(8)				--= '20170116'
,@Description					VARCHAR(256)		-- what the release is for
,@ReleaseReference				VARCHAR(50)
,@ReleaseSource					VARCHAR(50)

) AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @release_key INT = 0;

	--Working Storage
	DECLARE 
	  @seqint						INT
	 ,@sprint_date					CHAR(8)	
	 ,@release_number				INT
	 ,@StageTable					VARCHAR(128)		--= 'link_Sale_Match_Test'
	 ,@TableName					VARCHAR(128)
	 ,@ordinal_position				INT
	 
    -- Insert statements for procedure here
--BEGIN TRANSACTION
	/********************************************
	Release:
	********************************************/
	--'Find the Next Release for the Sprint'
	SELECT @sprint_date =  CASE WHEN ISNULL(@SprintDate, '') = '' 
								THEN convert(char(8), getdate(),112)
								ELSE @SprintDate
						   END
	SELECT TOP 1 @seqint = cast(right(cast([release_number] AS VARCHAR(100)), len(cast([release_number] AS VARCHAR(100))) - 8) AS INT)
	FROM [$(ConfigDatabase)].[dv_release].[dv_release_master]
	WHERE left(cast([release_number] AS VARCHAR(100)), 8) = @sprint_date
	ORDER BY 1 DESC
	IF @@rowcount = 0
	SET @release_number = cast(@sprint_date + '01' AS INT)
	ELSE
	SET @release_number = cast(@sprint_date + right('00' + cast(@seqint + 1 AS VARCHAR(100)), 2) AS INT)
	SELECT @release_number
	SET @Description = 'Load Stage Table: ' + quotename(@StageTable) + ' into ' + quotename(@VaultDatabase)
	-- Create the Release:
	EXECUTE  @release_key = [$(ConfigDatabase)].[dv_release].[dv_release_master_insert]  @release_number		= @release_number	-- date of the Sprint Start + ad hoc release number
																	,@release_description	= @Description		-- what the release is for
																	,@reference_number		= @ReleaseReference
																	,@reference_source		= @ReleaseSource
	
/********************************************
Scheduler:
********************************************/
DECLARE
	 @schedule_full_name			VARCHAR(128) = @SourceSystemName + '_Full' --Sales_Full
	,@schedule_delta_name			VARCHAR(128) = @SourceSystemName + '_Incremental' --Sales_Incremental
	,@schedule_full_desc			VARCHAR(128) = 'Full load of tables from ' + @SourceSystemName
	,@schedule_delta_desc			VARCHAR(128) = 'Incremental load of tables from ' + @SourceSystemName

-- Create full schedule
EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_insert] 
	 @schedule_name				= @schedule_full_name
	,@schedule_description		= @schedule_full_desc
	,@schedule_frequency		= 'Manually' --documentary only
	,@release_number			= @release_number

-- Create incremental schedule
EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_insert] 
	 @schedule_name				= @schedule_delta_name
	,@schedule_description		= @schedule_delta_desc
	,@schedule_frequency		= 'Daily' --documentary only
	,@release_number			= @release_number
  
	
	/* Loop through the Table List */
	DECLARE curTable CURSOR FOR  
	SELECT table_name, max(ordinal_position) as ordinal_position
	FROM @SourceTables
	GROUP BY table_name
	ORDER BY ordinal_position

	OPEN curTable   
	FETCH NEXT FROM curTable INTO @TableName, @ordinal_position  

	WHILE @@FETCH_STATUS = 0   
	BEGIN 

		EXECUTE [Admin].[ODE_Get_Metadata_For_Source_Table_Single]
				 @release_key			= @release_key
				,@KeyDetectionType		= @KeyDetectionType
				,@SourceSystemName		= @SourceSystemName
				,@SourceSchema			= @SourceSchema
				,@SourceTable			= @TableName
				,@StageDatabase			= @StageDatabase
				,@StageSchema			= @StageSchema
				,@StageLoadType			= @StageLoadType
				,@VaultDatabase			= @VaultDatabase
				,@ScheduleFullName		= @schedule_full_name
				,@ScheduleDeltaName		= @schedule_delta_name
				,@PrintCreateStatements = @PrintCreateStatements

	FETCH NEXT FROM curTable INTO @TableName, @ordinal_position    
	END   

	CLOSE curTable   
	DEALLOCATE curTable
--
--COMMIT
END