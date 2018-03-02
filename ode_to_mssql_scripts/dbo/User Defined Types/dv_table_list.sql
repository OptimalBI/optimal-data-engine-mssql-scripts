CREATE TYPE [dbo].[dv_table_list] AS TABLE(
	[table_name] [nvarchar](128) NULL,
	[ordinal_position] [int] IDENTITY(1,1) NOT NULL,
	PRIMARY KEY CLUSTERED 
(
	[ordinal_position] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO