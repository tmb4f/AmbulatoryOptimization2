USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME,
        @EndDate SMALLDATETIME

--SET @StartDate = NULL
--SET @EndDate = NULL
--SET @StartDate = '2/3/2019 00:00 AM'
--SET @EndDate = '2/9/2019 11:59 PM'
--SET @StartDate = '5/27/2019 00:00 AM'
--SET @EndDate = '6/3/2019 11:59 PM'
SET @StartDate = '7/1/2018 00:00 AM'
SET @EndDate = '6/12/2019 00:00 AM'

--CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_Hub]
--    (
--     @StartDate SMALLDATETIME = NULL,
--     @EndDate SMALLDATETIME = NULL
--    )
--AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_Hub
--WHO : Tom Burgan
--WHEN: 6/13/19
--WHY : Report dataset for Hub parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_Hub]
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

SELECT     0 AS hub_id, '(All)' AS hub_name 
UNION
SELECT DISTINCT
	appts.RPT_GRP_SEVEN AS hub_id,
	mdmloc.HUB AS hub_name
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN
(
    SELECT DISTINCT
		EPIC_DEPARTMENT_ID,
	    HUB
	FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
) AS mdmloc
ON appts.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID
WHERE (appts.APPT_DTTM BETWEEN @locStartDate AND @locEndDate)
AND (appts.RPT_GRP_SEVEN IS NOT NULL AND mdmloc.HUB IS NOT NULL) 
ORDER BY hub_name;

GO


