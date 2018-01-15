
CREATE PROCEDURE [stage].[usp_CalendarHolidays]
AS
BEGIN

  /*
      Name:                stage.usp_CalendarHolidays
      Purpose:             Generate a table of all the NZ public holidays for a range of years.
      Dependencies:        utfnPublicHolidaysForYearRange
      Parameters:          None
      Outputs:             Table [stage].[CalendarHolidays]
      History:
      WHO                  DATE               DESCRIPTION
      --------------       ---------------    -----------------------------------------------------------------------------------------------
      Brian Bradley        21/02/2017         Initial

*/

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = object_id('[stage].[CalendarHolidays]') AND type = 'U') 
DROP TABLE [stage].[CalendarHolidays]

DECLARE @FirstDate					date
       ,@FirstDateInt				int
	   ,@YearsInAdvance				int
	   ,@LastDate					date
	   ,@LastDateInt				int

-- Set the first date of the calendar to start
SET @FirstDate='1900-01-01'
-- Set the number of years in advance of the current year calendar should be populated. Default is 10
SET @YearsInAdvance = 10
SET @LastDate=DATEFROMPARTS(YEAR(GETDATE()) + @YearsInAdvance,12,31)

SELECT @FirstDateInt = cast(convert(char(4),@FirstDate, 112) as int)
      ,@LastDateInt  = cast(convert(char(4),@LastDate, 112) as int)

CREATE TABLE [stage].[CalendarHolidays]
(
[dv_stage_date_time] DATETIMEOFFSET(7) NULL
,[DateKey] DATE NULL
,[HolidayDate] DATE NULL
,[HolidayName] NVARCHAR(50) NULL
,[NationalHolidayName] NVARCHAR(50) NULL
,[NationalObservedHolidayName] NVARCHAR(50) NULL
,[RegionalHolidayName] NVARCHAR(50) NULL
,[RegionalObservedHolidayName] NVARCHAR(50) NULL
)

;WITH wBaseSet AS (
SELECT 
	 [DateKey]		=  convert(date, [HolidayDate])
	,[HolidayDate]	=  convert(date, [HolidayDate])
	,[HolidayName]
	,[NationalHoliday]
FROM [dbo].[utfnPublicHolidaysForYearRange] (@FirstDateInt, @LastDateInt)
UNION
SELECT 
	 [DateKey]		=  convert(date, [ObservedDate])  
	,[HolidayDate]	=  convert(date, [HolidayDate])
	,[HolidayName]
	,[NationalHoliday]
FROM [dbo].[utfnPublicHolidaysForYearRange] (@FirstDateInt, @LastDateInt)
)
,wFullSet AS(
SELECT 
	[DateKey],	
	[HolidayDate],
	[HolidayName],
	[NationalHolidayName]		= case	when NationalHoliday = 'Y'
										then case when HolidayDate = DateKey
							            then HolidayName
										end
							      end,
	NationalObservedHolidayName = case when NationalHoliday = 'Y'
									   then case when HolidayDate <> DateKey
							           then HolidayName
									   end
							      end,
	RegionalHolidayName			= case when NationalHoliday = 'N'
	                                   then case when HolidayDate = DateKey
							           then HolidayName
									   end
							      end,
	RegionalObservedHolidayName = case when NationalHoliday = 'N'
	                                   then case when HolidayDate <> DateKey
							           then HolidayName
									   end
							      end
from wBaseSet
)

INSERT INTO [stage].[CalendarHolidays]
SELECT      sysdatetimeoffset()
		   ,[DateKey]
		   ,[HolidayDate]
           ,[HolidayName]
           ,[NationalHolidayName]
           ,[NationalObservedHolidayName]
           ,[RegionalHolidayName]
           ,[RegionalObservedHolidayName]
FROM wFullSet

END