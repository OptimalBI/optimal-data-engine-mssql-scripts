
/*
	Name:			   ufnWeekdayInMonth
	Purpose:		   Calculate the nth occurrence of a day within a given month.
	Dependencies:	   None
	Parameters:		   @date DATETIME2(7) - Date from the month to find day in
					   @dayOfWeek SMALLINT - number represents the day of the week
					   @nthWeekdayInMonth SMALLINT - number of occurrence
	Outputs:		   DATETIME2(7) - date of the calculated day.
	History:
	WHO				   DATE			DESCRIPTION
	--------------	   ---------------	-----------------------------------------------------------------------------------------------
	Jonathan Sellar	   8 JULY 2015		Initial

*/

-- =============================================
-- Description: Gets the nth occurrence of a given weekday in the month containing the specified date.
-- For @dayOfWeek, 1 = Sunday, 2 = Monday, 3 = Tuesday, 4 = Wednesday, 5 = Thursday, 6 = Friday, 7 = Saturday
-- =============================================

CREATE FUNCTION [dbo].[ufnWeekdayInMonth]
(
    @date DATETIME2(7),
    @dayOfWeek SMALLINT,
    @nthWeekdayInMonth SMALLINT
)
RETURNS DATETIME2(7)
AS
BEGIN
    DECLARE @beginMonth DATETIME2(7)
    DECLARE @offSet SMALLINT
    DECLARE @firstWeekdayOfMonth DATETIME2(7)
    DECLARE @result DATETIME2(7)

    SET @beginMonth = DATEADD(DAY, -DATEPART(DAY, @date) + 1, @date)
    SET @offSet = @dayOfWeek - DATEPART(dw, @beginMonth)

    IF (@offSet < 0)
    BEGIN
        SET @firstWeekdayOfMonth = DATEADD(d, 7 + @offSet, @beginMonth)
    END
    ELSE
    BEGIN
        SET @firstWeekdayOfMonth = DATEADD(d, @offSet, @beginMonth)
    END

    SET @result = DATEADD(WEEK, @nthWeekdayInMonth - 1, @firstWeekdayOfMonth)

    IF (NOT(MONTH(@beginMonth) = MONTH(@result)))
    BEGIN
        SET @result = NULL
    END

    RETURN @result
END