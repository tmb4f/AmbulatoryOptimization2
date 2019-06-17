USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_ScheduledAppointment_LastRefreshDateTime]
AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_ScheduledAppointment_LastRefreshDateTime
--WHO : Tom Burgan
--WHEN: 6/14/19
--WHY : Report dataset for Last Refresh Date Time parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_ScheduledAppointment_LastRefreshDateTime]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/14/2019 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------

SELECT TOP 1 Load_Dtm
FROM Stage.Scheduled_Appointment
ORDER BY Load_Dtm DESC

GO


