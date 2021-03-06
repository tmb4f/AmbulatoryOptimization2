USE [DS_HSDM_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [Rptg].[CLARITY_SER_OT_PROV_TYPE](
	[PROV_ID] [VARCHAR](18) NULL,
	[CONTACT_DATE_REAL] [FLOAT] NULL,
	[CONTACT_DATE] [DATETIME] NULL,
	[EFF_TO_DATE] [DATETIME] NULL,
	[PROV_TYPE_OT_C] [VARCHAR](66) NULL,
	[PROV_TYPE_OT_NAME] [VARCHAR](254) NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


