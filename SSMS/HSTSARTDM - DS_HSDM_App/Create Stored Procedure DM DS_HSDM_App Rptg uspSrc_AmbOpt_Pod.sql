USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_Pod]
    (
     @StartDate SMALLDATETIME = NULL,
     @EndDate SMALLDATETIME = NULL
    )
AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_Pod
--WHO : Tom Burgan
--WHEN: 6/13/19
--WHY : Report dataset for Pod parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_Pod]
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

SELECT     0 AS pod_id, '(All)' AS pod_name 
UNION
SELECT DISTINCT
	appts.RPT_GRP_SIX AS pod_id,
	mdmloc.PFA_POD AS pod_name
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN
(
    SELECT DISTINCT
		EPIC_DEPARTMENT_ID,
	    PFA_POD
	FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
) AS mdmloc
ON appts.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID
WHERE (appts.APPT_DTTM BETWEEN @locStartDate AND @locEndDate)
AND (appts.RPT_GRP_SIX IS NOT NULL AND mdmloc.PFA_POD IS NOT NULL)
AND NOT (appts.RPT_GRP_SIX = '12' AND mdmloc.PFA_POD = 'Women''s and Children''s')
ORDER BY pod_name

GO


