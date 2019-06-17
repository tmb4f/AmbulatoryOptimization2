USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_Department]
    (
     @StartDate SMALLDATETIME = NULL,
     @EndDate SMALLDATETIME = NULL,
     @in_servLine VARCHAR(MAX),
	 @in_pods VARCHAR(MAX),
	 @in_hubs VARCHAR(MAX)
    )
AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_Department
--WHO : Tom Burgan
--WHEN: 6/13/19
--WHY : Report dataset for Epic department parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_Department]
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

DECLARE @tab_servLine TABLE
(
    Service_Line_Id int
);
INSERT INTO @tab_servLine
SELECT Param
FROM ETL.fn_ParmParse(@in_servLine, ',');
DECLARE @tab_pods TABLE
(
    pod_id VARCHAR(66)
);
INSERT INTO @tab_pods
SELECT Param
FROM ETL.fn_ParmParse(@in_pods, ',');
DECLARE @tab_hubs TABLE
(
    hub_id VARCHAR(66)
);
INSERT INTO @tab_hubs
SELECT Param
FROM ETL.fn_ParmParse(@in_hubs, ',');
SELECT     0 AS DEPARTMENT_ID, '(All)' AS DEPARTMENT_NAME 
UNION	  
SELECT DISTINCT
	appts.DEPARTMENT_ID DEPARTMENT_ID,
	mdm.epic_department_name_external DEPARTMENT_NAME
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
ON appts.DEPARTMENT_ID = mdm.epic_department_id
LEFT OUTER JOIN
(
    SELECT DISTINCT
		EPIC_DEPARTMENT_ID,
	    HUB
	FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
) AS mdmloc
ON appts.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID
WHERE (appts.APPT_DTTM BETWEEN @locStartDate AND @locEndDate)
      AND
      (
          @in_servLine = 0
          OR (COALESCE(mdm.service_line_id, mdm.opnl_service_id) IN (SELECT Service_Line_Id FROM @tab_servLine))
			 
      )
      AND
      (
          @in_pods = 0
          OR (appts.RPT_GRP_SIX IN (SELECT pod_id FROM @tab_pods))
      )
      AND
      (
          @in_hubs = 0
          OR (appts.RPT_GRP_SEVEN IN (SELECT hub_id FROM @tab_hubs))
      )
      AND appts.DEPARTMENT_ID IS NOT NULL AND mdm.epic_department_name_external IS NOT NULL
ORDER by DEPARTMENT_NAME

GO


