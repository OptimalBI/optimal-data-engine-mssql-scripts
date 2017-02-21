/*
	When business rules change, it is desirable to be able to audit the different versions of the rule over time.
	One method of doing this is the “Version” the Source Table to represent the new Rule(s).
*/
USE [ODE_Config];
GO
/******************************
Set the Parameters here:
*****************************/
DECLARE 
-- Provide the unique name of the Source as it is in [dbo].[dv_source_table]
  @SourceTableName VARCHAR(128)		= 'Sales__Customer'
	-- Provide the name of the new stored procedure - usually suffixed with the "_Vnnn"
, @NewSourceProcedureName VARCHAR(128) = 'usp_Sales__Customer_V001'
	-- If stored procedure can handle a load type as an input parameter and supports both Delta and Full load, set to 1. Default is 0
, @PassLoadTypeToProc BIT			= 0
	-- If you are building an Incremental Release, provide your Release Number
, @SprintDate CHAR(8)				= '20170131'
	-- Start Date of the current Sprint in Integer yyyymmdd (this depends on having set up a Sprint Release with the key yyymmdd00
	-- e.g. EXECUTE [dv_release].[dv_release_master_insert] 2016080100, 'Test Sprint release', 'US001', 'Jira'
, @ReleaseReference VARCHAR(50)		= 'HR-538'
	-- User Story and/or Task numbers for the Satellite which you are building.
, @ReleaseSource VARCHAR(50)		= 'Jira'
	-- system the reference number refers to, Rally, Jira etc.
/*****************************/

BEGIN TRANSACTION;
BEGIN TRY

 ----Release
 DECLARE  @seqint			INT
 ,@release_key				INT
 ,@release_number			INT
 ,@Description				VARCHAR(256)
 
/********************************************
Release:
********************************************/
--'Find the Next Release for the Sprint'
SELECT TOP 1 @seqint = cast(right(cast([release_number] AS VARCHAR(100)), len(cast([release_number] AS VARCHAR(100))) - 8) AS INT)
FROM [dv_release].[dv_release_master]
WHERE left(cast([release_number] AS VARCHAR(100)), 8) = @SprintDate
ORDER BY 1 DESC
IF @@rowcount = 0
SET @release_number = cast(@SprintDate + '01' AS INT)
ELSE
SET @release_number = cast(@SprintDate + right('00' + cast(@seqint + 1 AS VARCHAR(100)), 2) AS INT)
SELECT @release_number
SET @Description = 'Create new version of source business rule: ' + quotename(@SourceTableName)
-- Create the Release:
EXECUTE  @release_key = [dv_release].[dv_release_master_insert]  @release_number		= @release_number	-- date of the Sprint Start + ad hoc release number
																,@release_description	= @Description		-- what the release is for
																,@reference_number		= @ReleaseReference
																,@reference_source		= @ReleaseSource
-----------------------------------------------------------------
 -- Create the New Source:
 DECLARE 
  @Source_table_key		INT
, @Old_source_version	INT
, @New_source_version	INT

 SELECT @Release_Key
 , @Release_Number;

 -- Get the source table key
 SELECT @Source_table_key = [source_table_key]
 FROM [dbo].[dv_source_table]
 WHERE [source_unique_name] = @SourceTableName;

 -- Get the current source version
 SELECT @Old_source_version = MAX([source_version])
 FROM [dbo].[dv_source_version]
 WHERE [source_table_key] = @Source_table_key

 -- Create a new version
 SET @New_source_version = @Old_source_version + 1

 -- Retire all previous versions of the source table
 UPDATE [dbo].[dv_source_version]
 SET [is_current] = 0
 WHERE [source_table_key] = @Source_table_key

 -- Insert the new version
 EXECUTE [dbo].[dv_source_version_insert]
  @source_table_key = @Source_table_key
, @source_version = @New_source_version
, @source_procedure_name = @NewSourceProcedureName
, @pass_load_type_to_proc = @PassLoadTypeToProc
, @is_current = 1
, @release_number = @Release_Number
 
 PRINT 'succeeded';
 -- Commit if successful:
 COMMIT;
END TRY
BEGIN CATCH
 -- Return any error and Roll Back is there was a problem:
 PRINT 'failed';
 SELECT 'failed'
 , ERROR_NUMBER() AS [errornumber]
 , ERROR_SEVERITY() AS [errorseverity]
 , ERROR_STATE() AS [errorstate]
 , ERROR_PROCEDURE() AS [errorprocedure]
 , ERROR_LINE() AS [errorline]
 , ERROR_MESSAGE() AS [errormessage];
 ROLLBACK;
END CATCH;