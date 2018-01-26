CREATE TABLE [stage].[CalendarFiscal] (
    [dv_stage_date_time]       DATETIMEOFFSET (7) NULL,
    [DateKey]                  DATE               NULL,
    [FiscalYearCode]           INT                NULL,
    [FiscalYearDescription]    VARCHAR (50)       NULL,
    [FiscalQuarterCode]        INT                NULL,
    [FiscalQuarterDescription] VARCHAR (50)       NULL,
    [FiscalQuarterYearCode]    INT                NULL,
    [FiscalMonthCode]          INT                NULL,
    [FiscalMonthYearCode]      INT                NULL
);

