

/*
	Name:			ufnEasterSunday	
	Purpose:		Calculate the date of Easter Sunday for a give year.	
	Dependencies:	None	
	Parameters:		@year SMALLINT - Year as YYYY to calculate the date for.	
	Outputs:		DATETIME2(7) -  Date of Easter Sunday for given year.	
	Source:			https://gallery.technet.microsoft.com/scriptcenter/Calculate-Date-of-Eastern-36c624f9	
	History:
	WHO					DATE			DESCRIPTION
	--------------		---------------	-----------------------------------------------------------------------------------------------
	Jonathan Sellar		8 JULY 2015		Initial

*/
-- Calculate Date of Easter Sunday 
-- Function to Calculate the Date of Easter Sunday for a given year. 
-- Parameter: @year = Year to calculate the easter sunday for.  
--            The value must be between 1751 and 9999. 
-- Returns: The calculate easter sunday as a DateTime value. 
--          If the passed @year isn't valid then it returns NULL. 


CREATE FUNCTION [dbo].[ufnEasterSunday]
(
	@year SMALLINT
)
RETURNS DATETIME2(7) 
AS
BEGIN

    DECLARE @a SMALLINT, @b SMALLINT, @c SMALLINT, @d SMALLINT; 
    DECLARE @e SMALLINT, @o SMALLINT, @N SMALLINT, @M SMALLINT; 
    DECLARE @H1 SMALLINT, @H2 SMALLINT; 
  
    -- Validate @year parameter. 
    IF @year < 1753 OR @year > 9999 OR @year IS NULL 
        RETURN NULL; 
 
    -- Calculate easter sunday with Gauß algorithm. 
    SET @a  = @year % 19; 
    SET @b  = @year % 4; 
    SET @c  = @year % 7 
    SET @H1 = @year / 100; 
    SET @H2 = @year / 400; 
    SET @N = 4 + @H1 - @H2; 
    SET @M = 15 + @H1 - @H2 - ((8 * @H1 + 13) / 25); 
    SET @d = (19 * @a + @M) % 30; 
    SET @e = (2 * @b + 4 * @c + 6 * @d + @N) % 7; 
    SET @o = 22 + @d + @e; 
  
    -- Exceptions from the base rule. 
    IF @o = 57 
        SET @o = 50; 
    IF (@d = 28) AND (@e = 6) AND (@a > 10)  
        SET @o = 49; 
     
    RETURN(DATEADD(d, @o - 1, CONVERT(DATETIME2(7), CONVERT(CHAR(4), @year) + '0301', 112))); 

END