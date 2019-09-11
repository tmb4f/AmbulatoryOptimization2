USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_SOMDivision]
    (
     @StartDate SMALLDATETIME = NULL,
     @EndDate SMALLDATETIME = NULL,
     @in_somdeps VARCHAR(MAX)
    )
AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_SOMDivision
--WHO : Tom Burgan
--WHEN: 6/13/19
--WHY : Report dataset for Service Line paerameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--              DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_SOMDivision]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/13/2019 - TMB - create stored procedure
--		   07/01/2019 - TMB - change logic for setting SOM hierarchy values
--         07/09/2019 - TMB - edit logic to restore column som_division_id as key for SOM Division
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------
DECLARE @locStartDate SMALLDATETIME,
        @locEndDate SMALLDATETIME

SET @locStartDate = CAST(CAST(@StartDate AS DATE) AS SMALLDATETIME) + CAST(CAST('00:00:00' AS TIME) AS SMALLDATETIME)
SET @locEndDate   = CAST(DATEADD(MINUTE,-1,CAST((DATEADD(DAY,1,CAST(@EndDate AS DATE))) AS SMALLDATETIME)) AS SMALLDATETIME)

DECLARE @tab_somdeps TABLE
(
    som_department_id INT
);
INSERT INTO @tab_somdeps
(
    som_department_id
)
SELECT Param
FROM ETL.fn_ParmParse(@in_somdeps, ',');
SELECT     0 AS som_division_id, '(All)' AS som_division_name
UNION	  
SELECT DISTINCT
	physcn.SOM_division_id AS som_division_id,
	physcn.SOM_division_name AS som_division_name
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
ON ser.PROV_ID = appts.PROV_ID
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
ON physcn.sk_Dim_Physcn = ser.sk_Dim_Physcn
WHERE (appts.APPT_DTTM BETWEEN @locStartDate AND @locEndDate)
      AND
      (
          @in_somdeps = 0
          OR (physcn.som_department_id IN (SELECT som_department_id FROM @tab_somdeps))
      )
	  AND  physcn.SOM_division_id IS NOT NULL AND physcn.SOM_division_name IS NOT NULL
ORDER BY som_division_name

GO


