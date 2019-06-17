USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME,
        @EndDate SMALLDATETIME--,
     --   @in_servLine VARCHAR(MAX),
	    --@in_pods VARCHAR(MAX),
	    --@in_hubs VARCHAR(MAX),
	    --@in_somdeps VARCHAR(MAX)

--SET @StartDate = NULL
--SET @EndDate = NULL
--SET @StartDate = '2/3/2019 00:00 AM'
--SET @EndDate = '2/9/2019 11:59 PM'
--SET @StartDate = '5/27/2019 00:00 AM'
--SET @EndDate = '6/3/2019 11:59 PM'
SET @StartDate = '7/1/2018 00:00 AM'
SET @EndDate = '6/12/2019 00:00 AM'
/*
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
*/
--ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Daily]
--    (
--     @StartDate SMALLDATETIME = NULL,
--     @EndDate SMALLDATETIME = NULL,
--     @in_servLine VARCHAR(MAX),
--     @in_deps VARCHAR(MAX),
--	 @in_pods VARCHAR(MAX),
--	 @in_hubs VARCHAR(MAX),
--     @in_somdeps VARCHAR(MAX),
--	 @in_somdivs VARCHAR(MAX)
--    )
--AS
--CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_ServiceLine]
--    (
--     @StartDate SMALLDATETIME = NULL,
--     @EndDate SMALLDATETIME = NULL
--    )
--AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_ServiceLine
--WHO : Tom Burgan
--WHEN: 6/13/19
--WHY : Report dataset for Service Line paerameter.
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

--DECLARE @tab_servLine TABLE
--(
--    Service_Line_Id int
--);
--INSERT INTO @tab_servLine
--SELECT Param
--FROM ETL.fn_ParmParse(@in_servLine, ',');
--DECLARE @tab_pods TABLE
--(
--    pod_id VARCHAR(66)
--);
--INSERT INTO @tab_pods
--SELECT Param
--FROM ETL.fn_ParmParse(@in_pods, ',');
--DECLARE @tab_hubs TABLE
--(
--    hub_id VARCHAR(66)
--);
--INSERT INTO @tab_hubs
--SELECT Param
--FROM ETL.fn_ParmParse(@in_hubs, ',');
--DECLARE @tab_deps TABLE
--(
--    epic_department_id NUMERIC(18,0)
--);
--INSERT INTO @tab_deps
--SELECT Param
--FROM ETL.fn_ParmParse(@in_deps, ',');
--DECLARE @tab_somdeps TABLE
--(
--    som_department_id int
--);
--INSERT INTO @tab_somdeps
--(
--    som_department_id
--)
--SELECT Param
--FROM ETL.fn_ParmParse(@in_somdeps, ',');
--DECLARE @tab_somdivs TABLE
--(
--    som_division_id int
--);
--INSERT INTO @tab_somdivs
--(
--    som_division_id
--)
--SELECT Param
--FROM ETL.fn_ParmParse(@in_somdivs, ',');

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
ORDER BY SERVICE_LINE;

--WHERE ((evnts.appt_event_Canceled = 0)
--       OR ((evnts.appt_event_No_Show = 1)
--	       OR (evnts.appt_event_Canceled_Late = 1)
--		   OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 45)
--		  )
--	  )
--	  AND date_dim.day_date >= @locStartDate
--      AND date_dim.day_date < @locEndDate
--      AND
--      (
--          @in_servLine = 0
--          OR (COALESCE(evnts.service_line_id, evnts.opnl_service_id) IN (SELECT Service_Line_Id FROM @tab_servLine))
			 
--      )
--      AND
--      (
--          @in_pods = 0
--          OR (evnts.pod_id IN (SELECT pod_id FROM @tab_pods))
--      )
--      AND
--      (
--          @in_hubs = 0
--          OR (evnts.hub_id IN (SELECT hub_id FROM @tab_hubs))
--      )
--      AND
--      (
--          @in_deps = 0
--          OR (evnts.epic_department_id IN (SELECT epic_department_id FROM @tab_deps))
--      )
--      AND
--      (
--          @in_somdeps = 0
--          OR (evnts.som_department_id IN (SELECT som_department_id FROM @tab_somdeps))
--      )
--      AND
--      (
--          @in_somdivs = 0
--          OR (evnts.som_division_id IN (SELECT som_division_id FROM @tab_somdivs))
--      )

--ORDER BY date_dim.day_date;

GO


