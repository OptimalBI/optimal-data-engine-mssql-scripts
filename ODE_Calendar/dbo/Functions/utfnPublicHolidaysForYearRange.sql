
/*
	Name:			utfnPublicHolidaysForYearRange
	Purpose:		Generate a table of all the NZ public holidays for a range of years.	
	SQL Version:	any
	Dependencies:	utfnPublicHolidays	
	Parameters:		@StartYear SMALLINT - number representing first year of range as YYYY
					@EndYear SMALLINT - number representing last year of range as YYYY
	Outputs:		TABLE (HolidayDate DATETIME2(7), HolidayDay NVARCHAR(10), HolidayName NVARCHAR(50), NationalHoliday CHAR(1),
						ObservedDate DATETIME2(7), ObservedDay NVARCHAR(10), , PublicHolidayRegion NVARCHAR(50))
	History:
	WHO					DATE			DESCRIPTION
	--------------		---------------	-----------------------------------------------------------------------------------------------
	Jonathan Sellar		8 JULY 2015		Initial

*/

CREATE FUNCTION [dbo].[utfnPublicHolidaysForYearRange]
(
	@StartYear SMALLINT, 
	@EndYear SMALLINT
)
	RETURNS @resulttable TABLE
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

	DECLARE @year SMALLINT
	SET @year = @StartYear

	WHILE @YEAR <= @EndYear BEGIN

		INSERT INTO @resulttable
		SELECT HolidayDate, HolidayDay, HolidayName, NationalHoliday, ObservedDate, ObservedDay
		FROM [dbo].[utfnPublicHolidays](@YEAR)
		ORDER BY HolidayDate

		SET @YEAR = @YEAR + 1
	END

	RETURN

END