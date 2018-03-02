CREATE TYPE [dbo].[dv_column_type] AS TABLE(
	[column_name] [varchar](128) NOT NULL,
	[column_type] [varchar](50) NOT NULL,
	[column_length] [int] NULL,
	[column_precision] [int] NULL,
	[column_scale] [int] NULL,
	[collation_name] [sysname] NULL,
	[bk_ordinal_position] [int] NOT NULL,
	[source_ordinal_position] [int] NOT NULL,
	[satellite_ordinal_position] [int] NOT NULL,
	[abbreviation] [varchar](50) NULL,
	[object_type] [varchar](50) NULL
)
GO