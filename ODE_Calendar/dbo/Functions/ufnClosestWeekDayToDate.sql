
  /*
      Name:                ufn_ClosestWeekDayToDate
      Purpose:             Calculate the closest specified day of the week to given date.
      Dependencies:        None
      Parameters:          @date DATETIME2(7) - Date to calculate from
                           @dayOfWeek SMALLINT - number represents the day of the week
      Outputs:             DATETIME2(7) - date of the calculated day closest to date
      History:
      WHO                  DATE                 DESCRIPTION
      --------------       ---------------      -----------------------------------------------------------------------------------------------
      Jonathan Sellar      8/7/2015             Initial

*/

CREATE FUNCTION [dbo].[ufnClosestWeekDayToDate]
(
   @date DATETIME2(7),
   @dayOfWeek SMALLINT
)
RETURNS DATETIME2(7)
AS
BEGIN
     
      DECLARE @result DATETIME2(7)
      DECLARE @offset SMALLINT

      SET @offSet = @dayOfWeek - DATEPART(dw, @date)

      IF @offSet > -4 SET @result = DATEADD(day, @offset, @date)
      ELSE IF @offSet = -5 SET @result =  DATEADD(day, (-@offset)-3, @date)
      ELSE SET @result = DATEADD(day, (-@offset)-1, @date)

      RETURN @result

END