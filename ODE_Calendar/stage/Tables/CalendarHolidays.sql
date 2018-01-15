CREATE TABLE [stage].[CalendarHolidays] (
    [dv_stage_date_time]          DATETIMEOFFSET (7) NULL,
    [DateKey]                     DATE               NULL,
    [HolidayDate]                 DATE               NULL,
    [HolidayName]                 NVARCHAR (50)      NULL,
    [NationalHolidayName]         NVARCHAR (50)      NULL,
    [NationalObservedHolidayName] NVARCHAR (50)      NULL,
    [RegionalHolidayName]         NVARCHAR (50)      NULL,
    [RegionalObservedHolidayName] NVARCHAR (50)      NULL
);

