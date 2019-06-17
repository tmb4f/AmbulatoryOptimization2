USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME,
        @EndDate SMALLDATETIME,
	    @in_somdeps VARCHAR(MAX)

--SET @StartDate = NULL
--SET @EndDate = NULL
--SET @StartDate = '2/3/2019 00:00 AM'
--SET @EndDate = '2/9/2019 11:59 PM'
--SET @StartDate = '5/27/2019 00:00 AM'
--SET @EndDate = '6/3/2019 11:59 PM'
SET @StartDate = '7/1/2018 00:00 AM'
SET @EndDate = '6/12/2019 00:00 AM'

DECLARE @SOMDepartment TABLE (SOMDepartmentId VARCHAR(100))

INSERT INTO @SOMDepartment
(
    SOMDepartmentId
)
VALUES
--('0'),--(All)
--('57'),--MD-INMD Internal Medicine
--('98'),--MD-NERS Neurological Surgery
--('139'),--MD-OBGY Ob & Gyn
--('163'),--MD-ORTP Orthopaedic Surgery
--('194'),--MD-OTLY Otolaryngology
--('29'),--MD-PBHS Public Health Sciences
--('214'),--MD-PEDT Pediatrics
--('261'),--MD-PSCH Psychiatric Medicine
--('267'),--MD-RADL Radiology
--('292'),--MD-SURG Surgery
--('305'),--MD-UROL Urology
--('0') --(All)
('57')--,--MD-INMD Internal Medicine
--('292')--,--MD-SURG Surgery
--('47')--,--MD-ANES Anesthesiology
;

SELECT @in_somdeps = COALESCE(@in_somdeps+',' ,'') + CAST(SOMDepartmentId AS VARCHAR(MAX))
FROM @SOMDepartment

--SELECT @in_somdeps

--CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_SOMDivision]
--    (
--     @StartDate SMALLDATETIME = NULL,
--     @EndDate SMALLDATETIME = NULL,
--     @in_somdeps VARCHAR(MAX)
--    )
--AS  
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
--              DS_HSDM_App.Rptg.vwRef_Crosswalk_HSEntity_Prov
--              DS_HSDM_App.Rptg.vwRef_SOM_Hierarchy
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_SOMDivision]
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

DECLARE @tab_somdeps TABLE
(
    som_department_id int
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
	uwd.SOM_division_id AS som_division_id,
	uwd.SOM_division_name AS som_division_name
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
ON ser.PROV_ID = appts.PROV_ID
LEFT OUTER JOIN
(
	SELECT DISTINCT
		wd.sk_Dim_Physcn,
		wd.SOM_department_id,
		wd.SOM_department,
		wd.SOM_division_id,
		wd.SOM_division_name
	FROM
	(
		SELECT
			cwlk.sk_Dim_Physcn,
			som.SOM_department_id,
			som.SOM_department,
			som.SOM_division_id,
		    som.SOM_division_name,
			ROW_NUMBER() OVER (PARTITION BY cwlk.sk_Dim_Physcn ORDER BY som.SOM_Group_ID ASC) AS [SOMSeq]
		FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
		LEFT OUTER JOIN
		(
			SELECT DISTINCT
				SOM_Group_ID,
				SOM_department_id,
				SOM_department,
				SOM_division_id,
				SOM_division_name,
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
      AND
      (
          @in_somdeps = 0
          OR (uwd.som_department_id IN (SELECT som_department_id FROM @tab_somdeps))
      )
	  AND  uwd.SOM_division_id IS NOT NULL AND uwd.SOM_division_name IS NOT NULL
ORDER BY som_division_name;

GO


