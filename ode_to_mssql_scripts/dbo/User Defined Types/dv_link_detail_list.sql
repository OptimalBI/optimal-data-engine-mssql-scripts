CREATE TYPE [dbo].[dv_link_detail_list] AS TABLE (
    [link_key_name]   VARCHAR (128) NULL,
    [hub_name]        VARCHAR (128) NULL,
    [hub_column_name] VARCHAR (128) NULL,
    [column_name]     VARCHAR (128) NULL,
    [OrdinalPosition] INT           IDENTITY (1, 1) NOT NULL);

