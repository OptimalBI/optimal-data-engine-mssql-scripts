 
CREATE PROCEDURE [Admin].[ODE_Stop_Running_Schedule]
(
 @RunKey			            INT 
) AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

SELECT COUNT(*) 
FROM [$(ConfigDatabase)].[dv_scheduler].[dv_run]
WHERE @RunKey = run_key 
AND [run_status] IN ('Started', 'Scheduled')

IF @@ROWCOUNT < 1 RAISERROR('Run Key (%i) you have selected is not an active scheduled run',16, 1, @RunKey)
UPDATE [$(ConfigDatabase)].[dv_scheduler].[dv_run]
SET run_status = 'Cancelled'
WHERE run_key = @RunKey;
PRINT 'succeeded';
END