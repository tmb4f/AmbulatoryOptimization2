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

--CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_SOMDepartment]
--    (
--     @StartDate SMALLDATETIME = NULL,
--     @EndDate SMALLDATETIME = NULL
--    )
--AS  
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
--              DS_HSDM_App.Rptg.vwRef_Crosswalk_HSEntity_Prov
--              DS_HSDM_App.Rptg.vwRef_SOM_Hierarchy
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_SOMDepartment]
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

SELECT     0 AS som_department_id, '(All)' AS som_department_name 
UNION	  
SELECT DISTINCT
	uwd.SOM_department_id AS som_department_id,
	uwd.SOM_department AS som_department_name
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
ON ser.PROV_ID = appts.PROV_ID
LEFT OUTER JOIN
(
	SELECT DISTINCT
		wd.sk_Dim_Physcn,
		wd.SOM_department_id,
		wd.SOM_department
	FROM
	(
		SELECT
			cwlk.sk_Dim_Physcn,
			som.SOM_department_id,
			som.SOM_department,
			ROW_NUMBER() OVER (PARTITION BY cwlk.sk_Dim_Physcn ORDER BY som.SOM_Group_ID ASC) AS [SOMSeq]
		FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
		LEFT OUTER JOIN
		(
			SELECT DISTINCT
				SOM_Group_ID,
				SOM_department_id,
				SOM_department,
				SOM_division_5
			FROM Rptg.vwRef_SOM_Hierarchy
		) AS som
	    ON cwlk.wd_Dept_Code = som.SOM_division_5
        WHERE cwlk.wd_Is_Primary_Job = 1
        AND cwlk.wd_Is_Position_Active = 1
	) AS wd
	WHERE wd.SOMSeq = 1
) AS uwd
ON uwd.sk_Dim_Physcn = ser.sk_Dim_Physcn
WHERE (appts.APPT_DTTM BETWEEN @locStartDate AND @locEndDate)
      AND uwd.SOM_department_id IS NOT NULL
	  AND uwd.SOM_department IS NOT NULL
ORDER BY som_department_name;

GO


