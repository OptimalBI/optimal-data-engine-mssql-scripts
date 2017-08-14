CREATE TABLE [dbo].[dv_task_state] (
    [task_state_key]         INT                IDENTITY (1, 1) NOT NULL,
    [source_table_key]       INT                NULL,
    [source_unique_name]     VARCHAR (128)      NULL,
    [object_key]             INT                NULL,
    [object_type]            VARCHAR (50)       NULL,
    [object_name]            VARCHAR (128)      NULL,
    [procedure_name]         VARCHAR (128)      NULL,
    [high_water_date]        DATETIMEOFFSET (7) NULL,
    [source_high_water_lsn]  BINARY (10)        NULL,
    [source_high_water_date] VARCHAR (50)       NULL,
    [task_start_datetime]    DATETIMEOFFSET (7) NULL,
    [task_end_datetime]      DATETIMEOFFSET (7) NULL,
    [rows_inserted]          INT                NULL,
    [rows_updated]           INT                NULL,
    [rows_deleted]           INT                NULL,
    [session_id]             INT                NULL,
    [run_key]                INT                NULL,
    [updated_by]             VARCHAR (128)      DEFAULT (suser_name()) NULL,
    [update_date_time]       DATETIMEOFFSET (7) DEFAULT (sysdatetimeoffset()) NULL,
    PRIMARY KEY CLUSTERED ([task_state_key] ASC)
);

