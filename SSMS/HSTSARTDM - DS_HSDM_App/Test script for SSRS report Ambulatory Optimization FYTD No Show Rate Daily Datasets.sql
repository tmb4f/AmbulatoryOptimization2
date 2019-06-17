USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME,
        @EndDate SMALLDATETIME,
        @in_servLine VARCHAR(MAX),
	    @in_pods VARCHAR(MAX),
	    @in_hubs VARCHAR(MAX),
	    @in_somdeps VARCHAR(MAX)

--SET @StartDate = NULL
--SET @EndDate = NULL
--SET @StartDate = '2/3/2019 00:00 AM'
--SET @EndDate = '2/9/2019 11:59 PM'
--SET @StartDate = '5/27/2019 00:00 AM'
--SET @EndDate = '6/3/2019 11:59 PM'
SET @StartDate = '7/1/2018 00:00 AM'
SET @EndDate = '6/11/2019 11:59 PM'

DECLARE @ServiceLine TABLE (ServiceLineId SMALLINT)

INSERT INTO @ServiceLine
(
    ServiceLineId
)
VALUES
--(1),--Digestive Health
--(2),--Heart and Vascular
--(3),--Medical Subspecialties
--(4),--Musculoskeletal
--(5),--Neurosciences and Behavioral Health
--(6),--Oncology
--(7),--Ophthalmology
--(8),--Primary Care
--(9),--Surgical Subspecialties
--(10),--Transplant
--(11) --Womens and Childrens
(0)  --(All)
--(1) --Digestive Health
--(1),--Digestive Health
--(2) --Heart and Vascular

SELECT @in_servLine = COALESCE(@in_servLine+',' ,'') + CAST(ServiceLineId AS VARCHAR(MAX))
FROM @ServiceLine

--SELECT @in_servLine

DECLARE @Pod TABLE (PodId VARCHAR(100))

INSERT INTO @Pod
(
    PodId
)
VALUES
--('1'),--Cancer
--('2'),--Musculoskeletal
--('3'),--Primary Care
--('4'),--Surgical Procedural Specialties
--('5'),--Transplant
--('6'),--Medical Specialties
--('7'),--Radiology
--('8'),--Heart and Vascular Center
--('9'),--Neurosciences and Psychiatry
--('10'),--Women's and Children's
--('12'),--CPG
--('13'),--UVA Community Cancer POD
--('14'),--Digestive Health
--('15'),--Ophthalmology
--('16') --Community Medicine
('0') --(All)
--('1') --Cancer
--('1'), --Cancer
--('10') --Women's and Children's
--('14')--,--Digestive Health
;

SELECT @in_pods = COALESCE(@in_pods+',' ,'') + CAST(PodId AS VARCHAR(MAX))
FROM @Pod

--SELECT @in_pods

DECLARE @Hub TABLE (HubId VARCHAR(66))

INSERT INTO @Hub
(
    HubId
)
VALUES
--('2'),--Non-Hub registration
--('3'),--ECCC
--('4'),--415 Fontaine
--('5'),--Battle
--('6'),--JPA
--('7'),--Northridge
--('8'),--PCC
--('9'),--West Complex
--('10') --500 Fontaine
('0') --(All)
--('3'),--ECCC
--('10') --500 Fontaine
;

SELECT @in_hubs = COALESCE(@in_hubs+',' ,'') + CAST(HubId AS VARCHAR(MAX))
FROM @Hub

--SELECT @in_hubs

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
('0') --(All)
--('57')--,--MD-INMD Internal Medicine
--('292')--,--MD-SURG Surgery
--('47')--,--MD-ANES Anesthesiology
;

SELECT @in_somdeps = COALESCE(@in_somdeps+',' ,'') + CAST(SOMDepartmentId AS VARCHAR(MAX))
FROM @SOMDepartment

--SELECT @in_somdeps
/*
SELECT     0 AS SERVICE_LINE_ID, '(All)' AS SERVICE_LINE
UNION
SELECT DISTINCT
	COALESCE(mdm.service_line_id, mdm.opnl_service_id) SERVICE_LINE_ID,
	COALESCE(mdm.service_line, mdm.opnl_service_name) SERVICE_LINE
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
ON appts.DEPARTMENT_ID = mdm.epic_department_id
WHERE (CAST(CAST(appts.APPT_DTTM AS DATE) AS SMALLDATETIME) BETWEEN @StartDate AND @EndDate) AND (COALESCE(mdm.service_line_id, mdm.opnl_service_id) IS NOT NULL) AND (COALESCE(mdm.service_line, mdm.opnl_service_name) <> 'Unknown')
ORDER BY SERVICE_LINE;
*/
/*
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
WHERE (CAST(CAST(appts.APPT_DTTM AS DATE) AS SMALLDATETIME) BETWEEN @StartDate AND @EndDate) AND (appts.RPT_GRP_SIX IS NOT NULL AND mdmloc.PFA_POD IS NOT NULL) AND NOT (appts.RPT_GRP_SIX = '12' AND mdmloc.PFA_POD = 'Women''s and Children''s')
ORDER BY pod_name;
*/
/*
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
WHERE (CAST(CAST(appts.APPT_DTTM AS DATE) AS SMALLDATETIME) BETWEEN @StartDate AND @EndDate) AND (appts.RPT_GRP_SEVEN IS NOT NULL AND mdmloc.HUB IS NOT NULL) 
ORDER BY hub_name;
*/
/*
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
WHERE (CAST(CAST(appts.APPT_DTTM AS DATE) AS SMALLDATETIME) BETWEEN @StartDate AND @EndDate)
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
ORDER by DEPARTMENT_NAME;
*/
/*
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
WHERE (CAST(CAST(appts.APPT_DTTM AS DATE) AS SMALLDATETIME) BETWEEN @StartDate AND @EndDate)
      AND uwd.SOM_department_id IS NOT NULL AND uwd.SOM_department IS NOT NULL
ORDER BY som_department_name;
*/
/*
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
WHERE (CAST(CAST(appts.APPT_DTTM AS DATE) AS SMALLDATETIME) BETWEEN @StartDate AND @EndDate)
      AND
      (
          @in_somdeps = 0
          OR (uwd.som_department_id IN (SELECT som_department_id FROM @tab_somdeps))
      )
	  AND  uwd.SOM_division_id IS NOT NULL AND uwd.SOM_division_name IS NOT NULL
ORDER BY som_division_name;
*/
--LastRefreshDateTime
SELECT TOP 1 Load_Dtm
FROM Stage.Scheduled_Appointment
ORDER BY Load_Dtm DESC;
--FYStartDate
SELECT CAST(CASE WHEN MONTH(DATEADD(DAY, - 1, CAST(month_begin_date AS DATE))) = 6 THEN CONVERT(DATE, '7/1/' + CONVERT(VARCHAR(4),Fyear_num-2),101) ELSE CONVERT(DATE, '7/1/' + CONVERT(VARCHAR(4),Fyear_num-1),101) END AS SMALLDATETIME) AS FYStartDate
	FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte
	WHERE (CONVERT(VARCHAR(10), GETDATE(), 101) = CONVERT(VARCHAR(10), ddte.day_date, 101));
--YesterdayDate
SELECT CAST(CAST(DATEADD(MINUTE,-1,CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME)) AS DATE) AS SMALLDATETIME) AS YesterdayDate;
GO
