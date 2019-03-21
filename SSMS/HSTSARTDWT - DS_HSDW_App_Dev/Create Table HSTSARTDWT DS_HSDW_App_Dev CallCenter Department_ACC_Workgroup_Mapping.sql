USE [DS_HSDW_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF EXISTS (SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA='CallCenter'
    AND TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME='Department_ACC_Workgroup_Mapping')
   DROP TABLE CallCenter.Department_ACC_Workgroup_Mapping
GO

CREATE TABLE [CallCenter].[Department_ACC_Workgroup_Mapping](
	[sk_Department_ACC_Workgroup_Mapping] [INT] IDENTITY(1,1) NOT NULL,
	[Workgroup] [VARCHAR](150) NULL,
	[EpicDepartment] [NUMERIC](18,0) NULL,
	[ServiceLine] [VARCHAR](254) NULL,
	[Load_Dte] DATETIME NOT NULL,
	[ETL_guid] [VARCHAR](38) NOT NULL,
 CONSTRAINT [pk_Department_ACC_Workgroup_Mapping] PRIMARY KEY CLUSTERED 
(
	[sk_Department_ACC_Workgroup_Mapping] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


