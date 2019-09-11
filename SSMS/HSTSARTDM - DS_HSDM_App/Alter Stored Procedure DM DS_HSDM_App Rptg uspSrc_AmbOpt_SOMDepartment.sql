USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_SOMDepartment]
    (
     @StartDate SMALLDATETIME = NULL,
     @EndDate SMALLDATETIME = NULL
    )
AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_SOMDepartment
--WHO : Tom Burgan
--WHEN: 6/13/19
--WHY : Report dataset for SOM Department parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--              DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_SOMDepartment]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/13/2019 - TMB - create stored procedure
--		   07/01/2019 - TMB - change logic for setting SOM hierarchy values
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------
DECLARE @locStartDate SMALLDATETIME,
        @locEndDate SMALLDATETIME

SET @locStartDate = CAST(CAST(@StartDate AS DATE) AS SMALLDATETIME) + CAST(CAST('00:00:00' AS TIME) AS SMALLDATETIME)
SET @locEndDate   = CAST(DATEADD(MINUTE,-1,CAST((DATEADD(DAY,1,CAST(@EndDate AS DATE))) AS SMALLDATETIME)) AS SMALLDATETIME)

SELECT     0 AS som_department_id, '(All)' AS som_department_name 
UNION	  
SELECT DISTINCT
	physcn.SOM_department_id AS som_department_id,
	physcn.SOM_department AS som_department_name
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
ON ser.PROV_ID = appts.PROV_ID
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
ON physcn.sk_Dim_Physcn = ser.sk_Dim_Physcn
WHERE (appts.APPT_DTTM BETWEEN @locStartDate AND @locEndDate)
      AND physcn.SOM_department_id IS NOT NULL
	  AND physcn.SOM_department IS NOT NULL
ORDER BY som_department_name

GO


