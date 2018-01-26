
CREATE PROCEDURE [stage].[usp_Calendar]
AS
BEGIN
  /*
      Name:                stage.usp_Calendar
      Purpose:             Calculates the core Calendar Attributes for a set range of dates.
      Dependencies:        None
      Parameters:          None
      Outputs:             Table [stage].[Calendar]
      History:
      WHO                  DATE               DESCRIPTION
      --------------       ---------------    -----------------------------------------------------------------------------------------------
      Brian Bradley        21/02/2017         Initial

*/

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = object_id('[stage].[Calendar]') AND type = 'U') 
DROP TABLE [stage].[Calendar]

DECLARE @CurrentDate				date
	   ,@FirstDate					date
	   ,@YearsInAdvance				int
	   ,@LastDate					date
DECLARE @WeeklyHolidays				table ([WeekDay] int) -- weekday, sunday is 1 and saturday is 7

-- Set the first date of the calendar to start
SET @FirstDate='1900-01-01'
-- Set the number of years in advance of the current year calendar should be populated. Default is 10
SET @YearsInAdvance = 10
-----------------------------------------------------------------------
SET @LastDate = DATEFROMPARTS(YEAR(GETDATE()) + @YearsInAdvance,12,31)
SET @CurrentDate = @FirstDate

-- insert weekly holidays
INSERT into @WeeklyHolidays([WeekDay]) values(1) -- Sunday
INSERT into @WeeklyHolidays([WeekDay]) values(7) -- Saturday

CREATE TABLE [stage].[Calendar]
([dv_stage_date_time] DATETIMEOFFSET(7) NULL
,[DateKey] DATE NULL
,[DateFullName] VARCHAR(50) NULL
,[FullDateAlternateKey] DATE NULL
,[YearCode] INT NULL
,[YearDescription] NVARCHAR(50) NULL
,[QuarterCode] INT NULL
,[QuarterDescription] NVARCHAR(50) NULL
,[QuarterYearCode] INT NULL
,[MonthNumberOfYearCode] INT NULL
,[MonthYearCode] INT NULL
,[MonthName] NVARCHAR(30) NULL
,[MonthYearDescription] NVARCHAR(100) NULL
,[MonthLocalisedString] VARCHAR(50) NULL
,[DateLocalisedString] VARCHAR(50) NULL
,[DayNumberOfMonth] INT NULL
,[NumberOfDaysInTheMonth] INT NULL
,[DayNumberOfYear] INT NULL
,[WeekDay] INT NULL
,[WeekDayName] NVARCHAR(30) NULL
,[IsWeekDayCode] INT NULL
,[IsWeekDayDescription] VARCHAR(10) NULL
)

WHILE(@CurrentDate<=@LastDate)
	BEGIN
	INSERT INTO [stage].[Calendar]
	SELECT
	[dv_stage_datetime]				= sysdatetimeoffset(),
	[DateKey]						= @CurrentDate, 
	[DateFullName]					= convert(varchar(50),@CurrentDate,106),
	[FullDateAlternateKey]			=  @CurrentDate,
	[YearCode]						= datepart(year,@CurrentDate),
	[YearDescription]				= 'CY '+datename(year,@CurrentDate),
	[QuarterCode]					= datepart(QUARTER,@CurrentDate),
	[QuarterDescription]			= 'Q'+datename(QUARTER,@CurrentDate),
	[QuarterYearCode]				= convert(int,datename(year,@CurrentDate)+datename(QUARTER,@CurrentDate)),
	[MonthNumberOfYearCode]			= datepart(month,@CurrentDate),
	[MonthYearCode]					= convert(int,datename(year,@CurrentDate)+right('0'+convert(varchar(2),datepart(month,@CurrentDate)),2)),
	[MonthName]						= datename(month,@CurrentDate),
	[MonthYearDescription]			= datename(month,@CurrentDate)+' '+datename(year,@CurrentDate),
	[MonthLocalisedString]			= substring(convert(varchar(max),@CurrentDate,103),charindex('/',convert(varchar(max),@CurrentDate,103),1)+1,len(convert(varchar(max),@CurrentDate,103))-charindex('/',convert(varchar(max),@CurrentDate,103),1)),
	[DateLocalisedString]			= convert(varchar(max),@CurrentDate,103),
	[DayNumberOfMonth]				= datepart(day,@CurrentDate),
	[NumberOfDaysInTheMonth]		= datepart(day,EOMONTH(@CurrentDate)),
	[DayNumberOfYear]				= datepart(DAYOFYEAR,@CurrentDate),
	[WeekDay]						= datepart(WEEKDAY,@CurrentDate),
	[WeekDayName]					= datename(WEEKDAY,@CurrentDate),
	[IsWeekDayCode]					= case when datepart(WEEKDAY,@CurrentDate) in (select [weekday] from @WeeklyHolidays) then 0 else 1 end ,
	[IsWeekDayDescription]			= case when datepart(WEEKDAY,@CurrentDate) in (select [weekday] from @WeeklyHolidays) then 'Weekend' else 'Weekday' end

	SET @CurrentDate=dateadd(day,1,@CurrentDate)
	END
END