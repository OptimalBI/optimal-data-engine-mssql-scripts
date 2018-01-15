
/*
	Name:			    utfnPublicHolidays
	Purpose:		    Generate a table of all the NZ public holidays for the given year.	
	Dependencies:	    ufnMondayiseHoliday, ufnWeekdayInMonth, ufnEasterSunday
	Parameters:		    @year SMALLINT - number represent year as YYYY
	Outputs:		    TABLE (HolidayDate DATETIME2(7), HolidayDay NVARCHAR(10), HolidayName NVARCHAR(50), NationalHoliday CHAR(1),
						ObservedDate DATETIME2(7), ObservedDay NVARCHAR(10)
	History:
	WHO					DATE			DESCRIPTION
	--------------		---------------	-----------------------------------------------------------------------------------------------
	Jonathan Sellar		8 JULY 2015		Initial
	Brian Bradley		2 MAR  2017     Replaced ufnDatefromParts with the standard SQL function DateFromParts.

*/

CREATE FUNCTION [dbo].[utfnPublicHolidays]
(
	@year int
)
RETURNS @returntable TABLE
(
	HolidayDate DATETIME2(7),
	HolidayDay NVARCHAR(10),
	HolidayName NVARCHAR(50),
	NationalHoliday CHAR(1),
	ObservedDate DATETIME2(7),
	ObservedDay NVARCHAR(10)
)
AS
BEGIN
	
	DECLARE @insertDates TABLE (insertdate DATETIME2(7), datedesc NVARCHAR(50))
	DECLARE @EasterSunday DATETIME2(7)
	DECLARE @LabourDay DATETIME2(7)
	SET @LabourDay = dbo.ufnWeekdayInMonth(DateFromParts(@year,10,1), 2, 4)
	SET @EasterSunday = dbo.ufnEasterSunday(@year)

	--Insert Static Dates
	INSERT @insertDates(insertdate, datedesc)
	VALUES	(DateFromParts(@year, 1,1), 'New Year''s Day'), 
			(DateFromParts(@year, 1,2), 'Day after New Year''s Day'), 
			(DateFromParts(@year, 12,25), 'Christmas Day'), 
			(DateFromParts(@year, 12,26), 'Boxing Day')

	INSERT @returntable
	SELECT 
		insertdate, 
		DATENAME(dw, insertdate), 
		datedesc, 
		'Y', 
		dbo.ufnMondayiseHoliday(insertdate), 
		DATENAME(dw, dbo.ufnMondayiseHoliday(insertdate))
	FROM @insertDates

	DELETE @insertDates

	INSERT @insertDates(insertdate, datedesc)
	VALUES	(DateFromParts(@year, 2,6), 'Waitangi Day'),
			(DateFromParts(@year, 4,25), 'ANZAC Day')
	
	--January 1st 2014 onwards: Mondayised Anzac and Waitangi
	IF @year <= 2013 BEGIN
		INSERT @returntable
		SELECT  
			insertdate, 
			DATENAME(dw, insertdate), 
			datedesc, 
			'Y', 
			insertdate, 
			DATENAME(dw, insertdate)
		FROM @insertDates

	END
	ELSE BEGIN
		INSERT @returntable
		SELECT 
			insertdate, 
			DATENAME(dw, insertdate), 
			datedesc, 
			'Y', 
			dbo.ufnMondayiseHoliday(insertdate), 
			DATENAME(dw, dbo.ufnMondayiseHoliday(insertdate))
		FROM @insertDates

	END

	DELETE @insertDates

	--Feast Days
	INSERT @returntable
	VALUES	
		(DATEADD(day, -2, @EasterSunday),
		'Friday', 
		'Good Friday', 
		'Y', 
		DATEADD(day, -2, @EasterSunday),
		'Friday'),
		(DATEADD(day, 1, @EasterSunday),
		 'Monday', 
		 'Easter Monday', 
		 'Y', 
		 DATEADD(day, 1, @EasterSunday), 
		 'Monday')

	--Moveable Dates
	INSERT @insertDates(insertdate, datedesc)
	VALUES (dbo.ufnWeekdayInMonth(DateFromParts(@year,6,1), 2, 1), 'Queen''s Birthday'),
		   (@LabourDay, 'Labour Day')

	INSERT @returntable
	SELECT	
		insertdate, 
		DATENAME(dw, insertdate),
		datedesc, 
		'Y', 
		insertdate, 
		DATENAME(dw, insertdate)
	FROM @insertDates

	DELETE @insertDates

----------------------------------------------------------------------------------------------------
	--New Zealand Provencial Holidays
	--Provencial Holidays that are xth Monday of Month
	INSERT @insertDates
	VALUES 
		--Taranaki Anniversary Day is the second Monday in March
		(dbo.ufnWeekdayInMonth(DateFromParts(@year,3,1), 2, 2), 'Taranaki Anniversary'),
		
		--South Canterbury Anniversary Day is the fourth Monday in September
		(dbo.ufnWeekdayInMonth(DateFromParts(@year,9,1), 2, 4), 'South Canterbury Anniversary'),

		--Canterbury Show Day celebration is the second Friday after the first Tuesday in November
		(DATEADD(day, 10, dbo.ufnWeekdayInMonth(DateFromParts(@year,11,1), 3, 1)), 'Canterbury Anniversary'),

	--Provencial Holidays relative to Other Holidays 
		--Hawke's Bay Anniversary Day is held on the Friday before Labour Day
		(DATEADD(day, -3, @LabourDay), 'Hawke''s Bay Anniversary'),

		--Marlborough Anniversary is the first Monday after Labour Day (fourth Monday in October)
		(DATEADD(day, 7, @LabourDay), 'Marlborough Anniversary'),

	--Provencial Holidays using closest day to date
		--Wellington Anniversary Day is the Monday that falls closest to 22 January
		(dbo.ufnClosestWeekDayToDate(DateFromParts(@year, 1, 22), 2), 'Wellington Anniversary'),

		--Auckland Anniversary Day holiday usually falls on the Monday closest to 29 January
		(dbo.ufnClosestWeekDayToDate(DateFromParts(@year, 1, 29), 2), 'Auckland Anniversary'),

		--Nelson Anniversary Day holiday usually falls on the Monday closest to 1 Feburary
		(dbo.ufnClosestWeekDayToDate(DateFromParts(@year, 2, 1), 2), 'Nelson Anniversary'),
		
		--Otago Anniversary Day is the Monday that falls closest to 23 March
		(dbo.ufnClosestWeekDayToDate(DateFromParts(@year, 3, 23), 2), 'Otago Anniversary'),

		--Westland Anniversary Day Monday closest to 1 December
		(dbo.ufnClosestWeekDayToDate(DateFromParts(@year, 12, 1), 2), 'Westland Anniversary'),

		--Chatham Islands Anniversary Day is the Monday closest to 30 November
		(dbo.ufnClosestWeekDayToDate(DateFromParts(@year, 11, 30), 2), 'Chatham Islands Anniversary')

	IF @year > 2011 BEGIN
		INSERT @insertDates
		VALUES --Southland Anniversary Day is celebrated on Easter Tuesday since DEC-2011
				(DATEADD(day, 2, @EasterSunday), 'Southland Anniversary')
	END
	ELSE BEGIN
		INSERT @insertDates
		VALUES --Southland Anniversary Day was celebrated Monday closest to 17 January prior to DEC-2011
			(DATEADD(day, 2, @EasterSunday), 'Southland Anniversary')
	END
	
	INSERT @returntable
	SELECT	
		insertdate, 
		DATENAME(dw, insertdate), 
		datedesc, 
		'N', 
		insertdate, 
		DATENAME(dw, insertdate)
	FROM @insertDates 

	RETURN
END