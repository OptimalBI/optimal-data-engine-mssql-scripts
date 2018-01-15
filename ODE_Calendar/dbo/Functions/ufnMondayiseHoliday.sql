
/*
	Name:			    ufnMondayiseHoliday
	Purpose:		    Mondayised: When a public holiday falls on the weekend the day off work is observed the next non-holiday week day.
	Dependencies:	    None
	Parameters:		    @HolidayDate - Date to Mondayise
	Outputs:		    DATETIME2(7) - Mondayised date if required.
	History:
	WHO					DATE			DESCRIPTION
	--------------		---------------	-----------------------------------------------------------------------------------------------
	Jonathan Sellar		8 JULY 2015		Initial

*/

CREATE FUNCTION [dbo].[ufnMondayiseHoliday]
(
	@HolidayDate DATETIME2(7)
)
RETURNS DATETIME2(7)
AS
BEGIN
	
	DECLARE @SaturdayOffset SMALLINT
	DECLARE @SundayOffset SMALLINT
	SET @SaturdayOffset = DATEPART(dw, '2015-07-04') --Known Saturday
	SET @SundayOffset = DATEPART(dw, '2015-07-05') --Known Sunday

	DECLARE @MondayisedHoliday DATETIME2(7)
	SET @MondayisedHoliday = @HolidayDate
	
	IF DATEPART(dw, @HolidayDate) = @SaturdayOffset SET @MondayisedHoliday = DATEADD(D, 2, @HolidayDate)
	IF DATEPART(dw, @HolidayDate) = @SundayOffset SET @MondayisedHoliday = DATEADD(D, 1, @HolidayDate)

	/* Check for double holidays - New Years and Christmas */
	IF (DATEPART(D, @HolidayDate) = 1 AND DATEPART(M, @HolidayDate) = 1 AND DATEPART(D, @MondayisedHoliday) = 2) SET @MondayisedHoliday = DATEADD(D, 1, @MondayisedHoliday)
	IF (DATEPART(D, @HolidayDate) = 25 AND DATEPART(M, @HolidayDate) = 12 AND DATEPART(D, @MondayisedHoliday) = 26) SET @MondayisedHoliday = DATEADD(D, 1, @MondayisedHoliday)


	RETURN 	@MondayisedHoliday

END