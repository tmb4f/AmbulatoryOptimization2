USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_ServiceLine]
    (
     @StartDate SMALLDATETIME = NULL,
     @EndDate SMALLDATETIME = NULL
    )
AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_ServiceLine
--WHO : Tom Burgan
--WHEN: 6/13/19
--WHY : Report dataset for Service Line parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_ServiceLine]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/13/2019 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------
DECLARE @locStartDate SMALLDATETIME,
        @locEndDate SMALLDATETIME

SET @locStartDate = CAST(CAST(@StartDate AS DATE) AS SMALLDATETIME) + CAST(CAST('00:00:00' AS TIME) AS SMALLDATETIME)
SET @locEndDate   = CAST(DATEADD(MINUTE,-1,CAST((DATEADD(DAY,1,CAST(@EndDate AS DATE))) AS SMALLDATETIME)) AS SMALLDATETIME)

SELECT     0 AS SERVICE_LINE_ID, '(All)' AS SERVICE_LINE
UNION
SELECT DISTINCT
	COALESCE(mdm.service_line_id, mdm.opnl_service_id) SERVICE_LINE_ID,
	COALESCE(mdm.service_line, mdm.opnl_service_name) SERVICE_LINE
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
ON appts.DEPARTMENT_ID = mdm.epic_department_id
WHERE (appts.APPT_DTTM BETWEEN @locStartDate AND @locEndDate)
AND (COALESCE(mdm.service_line_id, mdm.opnl_service_id) IS NOT NULL)
AND (COALESCE(mdm.service_line, mdm.opnl_service_name) <> 'Unknown')
ORDER BY SERVICE_LINE

GO


