
CREATE PROCEDURE [stage].[usp_CalendarFiscal]
AS
BEGIN
  /*
      Name:               stage.usp_CalendarFiscal
      Purpose:            Calculates the core Calendar Attributes for a set range of dates.
      Dependencies:       None
      Parameters:         None
      Outputs:            Table [stage].[CalendarFiscal]
      History:
      WHO                 DATE               DESCRIPTION
      --------------      ---------------    -----------------------------------------------------------------------------------------------
      Brian Bradley       21/02/2017         Initial

*/

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = object_id('[stage].[CalendarFiscal]') AND type = 'U') 
DROP TABLE [stage].[CalendarFiscal]

DECLARE @CurrentDate				date
	   ,@FirstDate					date
	   ,@YearsInAdvance				int
	   ,@LastDate					date
	   ,@FiscalYearStartMonth		int
DECLARE @WeeklyHolidays				table ([WeekDay] int) -- weekday, sunday is 1 and saturday is 7

-- Set the first date of the calendar to start
SET @FirstDate='1900-01-01'
-- Set the number of years in advance of the current year calendar should be populated. Default is 10
SET @YearsInAdvance = 10
-- Set the month of fiscal year to start. Default is 7 - July
SET @FiscalYearStartMonth = 7
-----------------------------------------------------------------------
SET @LastDate=DATEFROMPARTS(YEAR(GETDATE()) + @YearsInAdvance,12,31)
SET @CurrentDate=@FirstDate

CREATE TABLE [stage].[CalendarFiscal]
([dv_stage_date_time] DATETIMEOFFSET(7) NULL
,[DateKey] DATE NULL
,[FiscalYearCode] INT NULL		
,[FiscalYearDescription] VARCHAR(50) NULL	
,[FiscalQuarterCode] INT NULL		
,[FiscalQuarterDescription] VARCHAR(50) NULL
,[FiscalQuarterYearCode] INT NULL	
,[FiscalMonthCode] INT NULL
,[FiscalMonthYearCode] INT NULL)

WHILE(@CurrentDate<=@LastDate)
	BEGIN
	INSERT INTO [stage].[CalendarFiscal]
	SELECT
	[dv_stage_datetime]				= sysdatetimeoffset(),
	[DateKey]						= @CurrentDate, 
	[FiscalYearCode]				= case when month(@CurrentDate)<@FiscalYearStartMonth 
										   then year(@CurrentDate) 
										   else year(@CurrentDate)+1 
										   end,
	[FiscalYearDescription] 		= 'FY ' + cast(case when month(@CurrentDate)<@FiscalYearStartMonth 
														then year(@CurrentDate) 
														else year(@CurrentDate)+1 
														end as varchar),
	[FiscalQuarterCode]				= ceiling(convert(float,(case when month(@CurrentDate)=13-@FiscalYearStartMonth 
																  then 12 
																  else ((@FiscalYearStartMonth-1)+month(@CurrentDate))%12 
																  end))/3),
	[FiscalQuarterDescription]		= 'FQ ' + cast(ceiling(convert(float,(case when month(@CurrentDate)=13-@FiscalYearStartMonth 
																			   then 12 
																			   else ((@FiscalYearStartMonth-1)+month(@CurrentDate))%12 
																			   end))/3) as varchar),
	[FiscalQuarterYearCode]			= convert(varchar(4),case when month(@CurrentDate)<@FiscalYearStartMonth then year(@CurrentDate) else year(@CurrentDate)+1 end)
									+ convert(varchar(1),ceiling(convert(float,(case when month(@CurrentDate)=13-@FiscalYearStartMonth then 12 else ((@FiscalYearStartMonth-1)+month(@CurrentDate))%12 end))/3)),
	[FiscalMonthCode]				= case when month(@CurrentDate)=13-@FiscalYearStartMonth then 12 else ((@FiscalYearStartMonth-1)+month(@CurrentDate))%12 end,
	[FiscalMonthYearCode]			= convert(varchar(4),case when month(@CurrentDate)<@FiscalYearStartMonth then year(@CurrentDate) else year(@CurrentDate)+1 end)
									+ right('0'+convert(varchar(2),case when month(@CurrentDate)=13-@FiscalYearStartMonth then 12 else ((@FiscalYearStartMonth-1)+month(@CurrentDate))%12 end),2)

	SET @CurrentDate=dateadd(day,1,@CurrentDate)
	END
END