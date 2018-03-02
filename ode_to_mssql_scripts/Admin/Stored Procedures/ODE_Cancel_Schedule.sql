CREATE PROCEDURE [Admin].[ODE_Cancel_Schedule]
(
	 @ScheduleName				VARCHAR(256) -- Name of the schedule to be cancelled
	,@pSprintDate				VARCHAR(50) -- Date of the sprint start. It is required for the new release number generation, format is YYYYMMDD, e.g. '20171031'
	,@pReferenceNumber			VARCHAR(50) -- User Story and/or Task numbers, e.g. 'ODE-33'
	,@pReferenceSource			VARCHAR(50) = 'Jira' -- system the reference number refers to, e.g. Rally

) AS
/** Cancel Schedule **/

/*
Steps:
1. Create new release - use dv_release_master_insert sp
2. Update schedule to cancelled - use dv_schedule_update sp
3. Update schedule release number - use dv_change_object_release sp
LOOP:
4. Update schedule source table to cancelled - use [dv_scheduler].[dv_schedule_source_table_update]
5. Update schedule source table release number - use dv_change_object_release sp
END LOOP:
*/
BEGIN
SET NOCOUNT ON
/********************************************
Begin:
********************************************/
-- Defaults:
DECLARE
	 @vReleaseNumber			INT
	,@vReleaseDesc				VARCHAR(256)
	,@vOldReleaseKey			INT
	,@vNewReleaseKey			INT 
	,@ScheduleKey				INT
	,@vScheduleDescription		VARCHAR(256)
	,@vScheduleFrequency		VARCHAR(128)
	,@vScheduleSourceTableKey	INT
	,@vSourceTableKey			INT
	,@vSourceTableLoadType		VARCHAR(50)
	,@vPriority					VARCHAR(50)
	,@vQueue					VARCHAR(50)

select @vReleaseNumber = ISNULL((MAX([dv_release_master].release_number) + 1), CAST((@pSprintDate + '01') AS INT)) FROM [$(ConfigDatabase)].[dv_release].[dv_release_master] WHERE CAST([dv_release_master].release_number as VARCHAR) like (@pSprintDate + '%')

	
SELECT 
	 @ScheduleKey			= [dv_schedule].schedule_key
	,@vScheduleDescription	= [dv_schedule].schedule_description
	,@vScheduleFrequency	= [dv_schedule].schedule_frequency
	,@vOldReleaseKey		= [dv_schedule].release_key
	,@vReleaseDesc			= 'Cancel schedule ' + [dv_schedule].schedule_name
FROM [$(ConfigDatabase)].[dv_scheduler].[dv_schedule] WHERE [dv_schedule].schedule_name = @ScheduleName

/** Create new release for cancelled schedule **/
EXECUTE @vNewReleaseKey = [$(ConfigDatabase)].[dv_release].[dv_release_master_insert] 
   @release_number			= @vReleaseNumber -- 2017103100
  ,@release_description		= @vReleaseDesc -- Cancel schedule TestResult_Full
  ,@reference_number	    = @pReferenceNumber --JJ-1400
  ,@reference_source	    = @pReferenceSource--'Jira'

/** Set schedule to cancelled **/
EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_update] 
	 @schedule_key			= @ScheduleKey
	,@schedule_name			= @ScheduleName
	,@schedule_description	= @vScheduleDescription
	,@schedule_frequency	= @vScheduleFrequency
	,@is_cancelled			= 1

/** Set cancelled schedule to new release **/
EXECUTE [$(ConfigDatabase)].[dv_release].[dv_change_object_release]
	 @vault_config_table	= 'dv_schedule'
	,@vault_config_table_key= @ScheduleKey
	,@vault_old_release		= @vOldReleaseKey
	,@vault_new_release		= @vNewReleaseKey

/** Create cursor to loop through schedule source table records **/
	DECLARE curTable CURSOR FOR  
	SELECT schedule_source_table_key,source_table_key,source_table_load_type,[priority],[queue],release_key
	FROM [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_source_table]
	WHERE schedule_key = @ScheduleKey
	ORDER BY schedule_source_table_key

	OPEN curTable   
	FETCH NEXT FROM curTable INTO @vScheduleSourceTableKey,@vSourceTableKey,@vSourceTableLoadType,@vPriority,@vQueue,@vOldReleaseKey

	WHILE @@FETCH_STATUS = 0   
	BEGIN 

		EXECUTE [$(ConfigDatabase)].[dv_scheduler].[dv_schedule_source_table_update]
				 @schedule_source_table_key = @vScheduleSourceTableKey
				,@schedule_key				= @ScheduleKey
				,@source_table_key			= @vSourceTableKey
				,@source_table_load_type	= @vSourceTableLoadType
				,@priority					= @vPriority
				,@queue						= @vQueue
				,@is_cancelled				= 1
		
		EXECUTE [$(ConfigDatabase)].[dv_release].[dv_change_object_release]
				 @vault_config_table	= 'dv_schedule_source_table'
				,@vault_config_table_key= @vScheduleSourceTableKey
				,@vault_old_release		= @vOldReleaseKey
				,@vault_new_release		= @vNewReleaseKey

	FETCH NEXT FROM curTable INTO @vScheduleSourceTableKey,@vSourceTableKey,@vSourceTableLoadType,@vPriority,@vQueue,@vOldReleaseKey
	END   

	CLOSE curTable   
	DEALLOCATE curTable

END