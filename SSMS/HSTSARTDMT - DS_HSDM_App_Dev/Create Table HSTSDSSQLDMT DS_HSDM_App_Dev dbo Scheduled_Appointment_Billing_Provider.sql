USE [DS_HSDM_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE dbo.Scheduled_Appointment_Billing_Provider

--EXEC dbo.usp_TruncateTable @schema = 'dbo',
--                           @Table = 'Scheduled_Appointment_Billing_Provider'

CREATE TABLE [dbo].[Scheduled_Appointment_Billing_Provider](
	[PROV_ID] [VARCHAR](18) NULL,
	[APPT_DT] [DATETIME] NULL,
	--[PROV_ATTR_ID] [VARCHAR](18) NULL,
	--[BILL_PROV_YN] CHAR(1) NULL
	[BILL_PROV_YN] INTEGER NULL
) ON [PRIMARY]
GO


