
CREATE PROCEDURE [stage].[usp_link_Calendar_Holidays]
AS
BEGIN

  /*
      Name:                stage.usp_link_Calendar_Holidays
      Purpose:             Links Holidays to the Calendar (there can be multiple holidays on one calendar day).
      Dependencies:        None
      Parameters:          None
      Outputs:             Table [stage].[link_Calendar_Holidays]
      History:
      WHO                  DATE               DESCRIPTION
      --------------       ---------------    -----------------------------------------------------------------------------------------------
      Brian Bradley        21/02/2017         Initial

*/

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = object_id('[stage].[link_Calendar_Holidays]') AND type = 'U') 
DROP TABLE [stage].[link_Calendar_Holidays]

CREATE TABLE [stage].[link_Calendar_Holidays]
(
[dv_stage_date_time] DATETIMEOFFSET(7) NULL
,[HolidayDateKey] DATE NULL
,[HolidayName] NVARCHAR(50) NULL
,[DateKey] DATE NULL
)

INSERT  INTO [stage].[link_Calendar_Holidays]
SELECT DISTINCT 
       sysdatetimeoffset()
	  ,[DateKey] 
      ,[HolidayName]
	  ,[DateKey]
  FROM [$(ODE_Vault)].[sat].[s_CalendarHolidays]
  WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0
END