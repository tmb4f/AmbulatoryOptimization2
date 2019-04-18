USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME,
        @EndDate SMALLDATETIME,
        @in_servLine VARCHAR(MAX),
        @in_deps VARCHAR(MAX),
        @in_depid VARCHAR(MAX),
	    @in_pods VARCHAR(MAX),
	    @in_podid VARCHAR(MAX),
	    @in_hubs VARCHAR(MAX),
	    @in_hubid VARCHAR(MAX),
	    @in_somdeps VARCHAR(MAX),
	    @in_somdepid VARCHAR(MAX),
	    @in_somdivs VARCHAR(MAX),
	    @in_somdivid VARCHAR(MAX)

SET @StartDate = '2/1/2019 00:00 AM'
SET @EndDate = '2/28/2019 11:59 PM'
--SET @StartDate = '7/1/2018 00:00 AM'
--SET @EndDate = '2/28/2019 11:59 PM'

SET NOCOUNT ON

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

--DECLARE @ServiceLine TABLE (ServiceLineName VARCHAR(150))

--INSERT INTO @ServiceLine
--(
--    ServiceLineName
--)
--VALUES
----('Digestive Health'),
----('Heart and Vascular'),
----('Medical Subspecialties'),
----('Musculoskeletal'),
----('Neurosciences and Behavioral Health'),
----('Oncology'),
----('Ophthalmology'),
----('Primary Care'),
----('Surgical Subspecialties'),
----('Transplant'),
----('Womens and Childrens')
----('Medical Subspecialties')
--('Digestive Health')
----('Womens and Childrens')
--;

SELECT @in_servLine = COALESCE(@in_servLine+',' ,'') + CAST(ServiceLineId AS VARCHAR(MAX))
FROM @ServiceLine

--SELECT @in_servLine

DECLARE @Department TABLE (DepartmentId NUMERIC(18,0))

INSERT INTO @Department
(
    DepartmentId
)
VALUES
-- (10210006)
--,(10210040)
--,(10210041)
--,(10211006)
--,(10214011)
--,(10214014)
--,(10217003)
--,(10239017)
--,(10239018)
--,(10239019)
--,(10239020)
--,(10241001)
--,(10242007)
--,(10242049)
--,(10243003)
--,(10244004)
--,(10348014)
--,(10354006)
--,(10354013)
--,(10354014)
--,(10354015)
--,(10354016)
--,(10354017)
--,(10354024)
--,(10354034)
--,(10354042)
--,(10354044)
--,(10354052)
--,(10354055)
 --(10214011)
 --(10210006)
 --(10280004) -- AUBL PEDIATRICS
 --(10341002) -- CVPE UVA RHEU INF PNTP
 --(10228008) -- NRDG MAMMOGRAPHY
 --(10381003) -- UVEC RAD CT
 --(10354032) -- UVBB PHYSICAL THER FL4
 --(10242018) -- UVPC PULMONARY
 --(10243003) -- UVHE DIGESTIVE HEALTH
 --(10239003) -- UVMS NEPHROLOGY
 --(10354015) -- UVBB PEDS ONCOLOGY CL
(0)  -- (All)
;

SELECT @in_deps = COALESCE(@in_deps+',' ,'') + CAST(DepartmentId AS VARCHAR(MAX))
FROM @Department

--SELECT @in_deps

SELECT @in_depid = COALESCE(@in_depid+',' ,'') + CAST(DepartmentId AS VARCHAR(MAX))
FROM @Department

--SELECT @in_depid

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
;

--DECLARE @Pod TABLE (PodName VARCHAR(100))

--INSERT INTO @Pod
--(
--    PodName
--)
--VALUES
----('Cancer'),
----('Musculoskeletal'),
----('Primary Care'),
----('Surgical Procedural Specialties'),
----('Transplant'),
----('Medical Specialties'),
----('Radiology'),
----('Heart and Vascular Center'),
----('Neurosciences and Psychiatry'),
----('Women''s and Children''s'),
----('CPG'),
----('UVA Community Cancer POD'),
----('Digestive Health'),
----('Ophthalmology'),
----('Community Medicine')
----('Medical Specialties')
--('Digestive Health')
--;

SELECT @in_pods = COALESCE(@in_pods+',' ,'') + CAST(PodId AS VARCHAR(MAX))
FROM @Pod

--SELECT @in_pods

/*
DECLARE @tab_pods TABLE
(
    pod_id VARCHAR(MAX)
);
INSERT INTO @tab_pods
SELECT Param
FROM ETL.fn_ParmParse(@in_pods, ',');

SELECT * FROM @tab_pods

SELECT DISTINCT pod_id, pod_name FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] 
where (pod_id IS NOT NULL)
      AND
	  (pod_name IS NOT NULL)
	  AND
      (
          '0' IN
          (
              SELECT pod_id FROM @tab_pods
          )
          OR pod_id IN
             (
                 SELECT pod_id FROM @tab_pods
             )
      )
order by pod_name
*/

SELECT @in_podid = COALESCE(@in_podid+',' ,'') + CAST(PodId AS VARCHAR(MAX))
FROM @Pod

--SELECT @in_podid

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
;

SELECT @in_hubs = COALESCE(@in_hubs+',' ,'') + CAST(HubId AS VARCHAR(MAX))
FROM @Hub

--SELECT @in_hubs

SELECT @in_hubid = COALESCE(@in_hubid+',' ,'') + CAST(HubId AS VARCHAR(MAX))
FROM @Hub

--SELECT @in_hubid

DECLARE @SOMDepartment TABLE (SOMDepartmentId VARCHAR(100))

INSERT INTO @SOMDepartment
(
    SOMDepartmentId
)
VALUES
--('0'),--(All)
('57')--,--MD-INMD Internal Medicine
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
;

SELECT @in_somdeps = COALESCE(@in_somdeps+',' ,'') + CAST(SOMDepartmentId AS VARCHAR(MAX))
FROM @SOMDepartment

--SELECT @in_somdeps

SELECT @in_somdepid = COALESCE(@in_somdepid+',' ,'') + CAST(SOMDepartmentId AS VARCHAR(MAX))
FROM @SOMDepartment

--SELECT @in_somdepid

DECLARE @SOMDivision TABLE (SOMDivisionId VARCHAR(100))

INSERT INTO @SOMDivision
(
    SOMDivisionId
)
VALUES
('0')--,--(All)
--('60'),--MD-INMD Allergy
--('66'),--MD-INMD CV Medicine
--('68'),--MD-INMD Endocrinology
--('72'),--MD-INMD Gastroenterology
--('80'),--MD-INMD Hem/Onc
--('76'),--MD-INMD Hospital Medicine
--('84'),--MD-INMD Infectious Dis
--('86'),--MD-INMD Nephrology
--('88'),--MD-INMD Pulmonary
--('90'),--MD-INMD Rheumatology
--('99'),--MD-NERS Admin
--('109'),--MD-NERS CV Disease
--('111'),--MD-NERS Deg Spinal Dis
--('113'),--MD-NERS Gamma Knife
--('119'),--MD-NERS Multiple Neuralgia
--('121'),--MD-NERS Neuro-Onc
--('127'),--MD-NERS Pediatric Pituitary
--('129'),--MD-NERS Radiosurgery
--('142'),--MD-OBGY Gyn Oncology
--('154'),--MD-OBGY Gyn Specialties
--('144'),--MD-OBGY Maternal Fetal Med
--('148'),--MD-OBGY Midlife Health
--('150'),--MD-OBGY Northridge
--('140'),--MD-OBGY Ob & Gyn, Admin
--('146'),--MD-OBGY Reprod Endo/Infertility
--('166'),--MD-ORTP Adult Reconst
--('176'),--MD-ORTP Foot/Ankle
--('190'),--MD-ORTP Hand Surgery
--('164'),--MD-ORTP Ortho Surg, Admin
--('182'),--MD-ORTP Pediatric Ortho
--('186'),--MD-ORTP Spine
--('188'),--MD-ORTP Sports Med
--('192'),--MD-ORTP Trauma
--('195'),--MD-OTLY Oto, Admin
--('30'),--MD-PBHS Public Health Sciences Admin
--('235'),--MD-PEDT Adolescent Medicine
--('221'),--MD-PEDT Cardiology
--('223'),--MD-PEDT Critical Care
--('225'),--MD-PEDT Developmental
--('227'),--MD-PEDT Endocrinology
--('237'),--MD-PEDT Gastroenterology
--('239'),--MD-PEDT General Pediatrics
--('241'),--MD-PEDT Genetics
--('243'),--MD-PEDT Hematology
--('247'),--MD-PEDT Infectious Diseases
--('249'),--MD-PEDT Neonatology
--('251'),--MD-PEDT Nephrology
--('217'),--MD-PEDT Pediatrics, Admin
--('255'),--MD-PEDT Pulmonary
--('262'),--MD-PSCH Psychiatry and NB Sciences
--('272'),--MD-RADL Angio/Interv
--('276'),--MD-RADL Breast Imaging
--('282'),--MD-RADL Neuroradiology
--('274'),--MD-RADL Non-Invasive Cardio
--('284'),--MD-RADL Nuclear Medicine
--('268'),--MD-RADL Radiology, Admin
--('278'),--MD-RADL Thoracoabdominal
--('293'),--MD-SURG Surgery, Admin
--('310'),--MD-UROL Urology, General
;

SELECT @in_somdivs = COALESCE(@in_somdivs+',' ,'') + CAST(SOMDivisionId AS VARCHAR(MAX))
FROM @SOMDivision

--SELECT @in_somdivs

SELECT @in_somdivid = COALESCE(@in_somdivid+',' ,'') + CAST(SOMDivisionId AS VARCHAR(MAX))
FROM @SOMDivision

--SELECT @in_somdivid
/*
/*SOMDepartment*/
SELECT     '0' AS som_department_id, '(All)' AS som_department_name 
UNION	  
SELECT DISTINCT w_som_department_id AS som_department_id, w_som_department_name AS som_department_name FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] WHERE w_som_department_id IS NOT NULL AND w_som_department_name IS NOT NULL order BY som_department_name
/*SOMDepartmentID*/
DECLARE @tab_somdeps TABLE
(
    som_department_id VARCHAR(MAX)
);
INSERT INTO @tab_somdeps
SELECT Param
FROM ETL.fn_ParmParse(@in_somdeps, ',');
SELECT DISTINCT w_som_department_id AS som_department_id, w_som_department_name AS som_department_name FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] 
where (w_som_department_id IS NOT NULL)
      AND
	  (w_som_department_name IS NOT NULL)
	  AND
      (
          '0' IN
          (
              SELECT som_department_id FROM @tab_somdeps
          )
          OR som_department_id IN
             (
                 SELECT som_department_id FROM @tab_somdeps
             )
      )
order by w_som_department_name;

/*SOMDivision*/
SELECT     '0' AS som_division_id, '(All)' AS som_division_name 
UNION	  
SELECT DISTINCT w_som_division_id AS som_division_id, w_som_division_name AS som_division_name FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] WHERE w_som_division_id IS NOT NULL AND w_som_division_name IS NOT NULL order BY som_division_name
/*SOMDivisionID*/
DECLARE @tab_somdivs TABLE
(
    som_division_id VARCHAR(MAX)
);
INSERT INTO @tab_somdivs
SELECT Param
FROM ETL.fn_ParmParse(@in_somdivs, ',');
SELECT DISTINCT w_som_division_id AS som_division_id, w_som_division_name AS som_division_name FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] 
where (w_som_division_id IS NOT NULL)
      AND
	  (w_som_division_name IS NOT NULL)
	  AND
      (
          '0' IN
          (
              SELECT som_division_id FROM @tab_somdivs
          )
          OR som_division_id IN
             (
                 SELECT som_division_id FROM @tab_somdivs
             )
      )
order by w_som_division_name;
*/
-- =========================================================
-- Author:		Tom Burgan
-- Create date: 03/25/2019
-- Description:	No Show Rate Data Portal SSRS export script
-- =========================================================
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         03/25/2019 - Tom		-- create stored procedure
--         03/27/2019 - Tom     -- use w_service_line_id, w_opnl_service_id for service line parameter
--         04/05/2019 - TMB     -- add APPT_MADE_DTTM, BUSINESS_UNIT, Prov_Typ, and new wrapper columns to extract
--         04/18/2019 - TMB     -- add logic for SOM Department and SOM Division report parameters
--************************************************************************************************************************
--ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Download]
--    @StartDate SMALLDATETIME,
--    @EndDate SMALLDATETIME,
--    @in_servLine VARCHAR(MAX),
--    @in_deps VARCHAR(MAX),
--    @in_depid VARCHAR(MAX),
--	@in_pods VARCHAR(MAX),
--	@in_podid VARCHAR(MAX),
--	@in_hubs VARCHAR(MAX),
--	@in_hubid VARCHAR(MAX),
--    @in_somdeps VARCHAR(MAX),
--	@in_somdepid VARCHAR(MAX),
--	@in_somdivs VARCHAR(MAX),
--	@in_somdivid VARCHAR(MAX)
--AS
DECLARE @tab_servLine TABLE
(
    Service_Line_Id VARCHAR(MAX)
);
INSERT INTO @tab_servLine
SELECT Param
FROM ETL.fn_ParmParse(@in_servLine, ',');
DECLARE @tab_pods TABLE
(
    pod_id VARCHAR(MAX)
);
INSERT INTO @tab_pods
SELECT Param
FROM ETL.fn_ParmParse(@in_pods, ',');
DECLARE @tab_podid TABLE
(
    pod_id VARCHAR(MAX)
);
INSERT INTO @tab_podid
SELECT Param
FROM ETL.fn_ParmParse(@in_podid, ',');
DECLARE @tab_hubs TABLE
(
    hub_id VARCHAR(MAX)
);
INSERT INTO @tab_hubs
SELECT Param
FROM ETL.fn_ParmParse(@in_hubs, ',');
DECLARE @tab_hubid TABLE
(
    hub_id VARCHAR(MAX)
);
INSERT INTO @tab_hubid
SELECT Param
FROM ETL.fn_ParmParse(@in_hubid, ',');
DECLARE @tab_deps TABLE
(
    epic_department_id VARCHAR(MAX)
);
INSERT INTO @tab_deps
SELECT Param
FROM ETL.fn_ParmParse(@in_deps, ',');
DECLARE @tab_depid TABLE
(
    epic_department_id VARCHAR(MAX)
);
INSERT INTO @tab_depid
SELECT Param
FROM ETL.fn_ParmParse(@in_depid, ',');
DECLARE @tab_somdeps TABLE
(
    som_department_id VARCHAR(MAX)
);
INSERT INTO @tab_somdeps
(
    som_department_id
)
SELECT Param
FROM ETL.fn_ParmParse(@in_somdeps, ',');
DECLARE @tab_somdepid TABLE
(
    som_department_id VARCHAR(MAX)
);
INSERT INTO @tab_somdepid
SELECT Param
FROM ETL.fn_ParmParse(@in_somdepid, ',');
DECLARE @tab_somdivs TABLE
(
    som_division_id VARCHAR(MAX)
);
INSERT INTO @tab_somdivs
(
    som_division_id
)
SELECT Param
FROM ETL.fn_ParmParse(@in_somdivs, ',');
DECLARE @tab_somdivid TABLE
(
    som_division_id VARCHAR(MAX)
);
INSERT INTO @tab_somdivid
SELECT Param
FROM ETL.fn_ParmParse(@in_somdivid, ',');
SELECT
       event_date,
       event_type, -- 'Appointment'
	   event_category, -- NULL
       event_count, -- 1
       fyear_num,
       Load_Dtm,
       hs_area_name,
       COALESCE(w_service_line_id, w_opnl_service_id, w_corp_service_line_id) service_line_id,
       COALESCE(w_service_line_name, w_opnl_service_name, w_corp_service_line_name) Service_Line,
	   w_pod_id,
	   w_pod_name,
	   w_hub_id,
	   w_hub_name,
       w_department_id,
       w_department_name,
       w_department_name_external,
       peds,
       transplant,
       person_id,
       provider_id,
	   enc.PAT_ENC_CSN_ID,
	   acct.AcctNbr_int,
	   CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appt],
	   CASE WHEN (appt_event_No_Show = 1 OR appt_event_Canceled_Late = 1) THEN 1 ELSE 0 END AS [No Show],
	   APPT_STATUS_FLAG,
	   APPT_DTTM,
	   CANCEL_REASON_C,
	   CANCEL_REASON_NAME,
	   CANCEL_INITIATOR,
	   CANCEL_LEAD_HOURS,
	   Cancel_Lead_Days,
	   APPT_CANC_DTTM,
	   APPT_MADE_DATE,
	   appt_event_No_Show,
	   appt_event_Canceled_Late,
	   appt_event_Scheduled,
	   appt_event_Provider_Canceled,
	   appt_event_Completed,
	   appt_event_Arrived,
	   PHONE_REM_STAT_NAME,
	   APPT_MADE_DTTM,
	   BUSINESS_UNIT,
	   Prov_Typ,
	   w_rev_location_id,
	   w_rev_location,
	   w_som_department_id,
	   w_som_department_name,
	   w_financial_division_id,
	   w_financial_division_name,
	   w_financial_sub_division_id,
	   w_financial_sub_division_name,
	   w_som_division_id,
	   w_som_division_name
FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt enc
ON enc.sk_Fact_Pt_Enc_Clrt = tabrptg.sk_Fact_Pt_Enc_Clrt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct_Aggr acct
ON acct.sk_Fact_Pt_Acct = tabrptg.sk_Fact_Pt_Acct
WHERE 1 = 1
      AND event_date >= @StartDate
      AND event_date <= @EndDate
	  AND ((event_count = 1)
           AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
      AND
      (
          0 IN
          (
              SELECT epic_department_id FROM @tab_deps
          )
          OR epic_department_id IN
             (
                 SELECT epic_department_id FROM @tab_deps
             )
      )
      AND
      (
          0 IN
          (
              SELECT epic_department_id FROM @tab_depid
          )
          OR epic_department_id IN
             (
                 SELECT epic_department_id FROM @tab_depid
             )
      )
      AND
      (
          0 IN
          (
              SELECT pod_id FROM @tab_pods
          )
          OR pod_id IN
             (
                 SELECT pod_id FROM @tab_pods
             )
      )
      AND
      (
          0 IN
          (
              SELECT pod_id FROM @tab_podid
          )
          OR pod_id IN
             (
                 SELECT pod_id FROM @tab_podid
             )
      )
      AND
      (
          0 IN
          (
              SELECT hub_id FROM @tab_hubs
          )
          OR hub_id IN
             (
                 SELECT hub_id FROM @tab_hubs
             )
      )
      AND
      (
          0 IN
          (
              SELECT hub_id FROM @tab_hubid
          )
          OR hub_id IN
             (
                 SELECT hub_id FROM @tab_hubid
             )
      )
      AND
      (
          0 IN
          (
              SELECT Service_Line_Id FROM @tab_servLine
          )
          OR COALESCE(w_service_line_id, w_opnl_service_id) IN
             (
                 SELECT Service_Line_Id FROM @tab_servLine
             )
      )
      AND
      (
          0 IN
          (
              SELECT som_department_id FROM @tab_somdeps
          )
          OR som_department_id IN
             (
                 SELECT som_department_id FROM @tab_somdeps
             )
      )
      AND
      (
          0 IN
          (
              SELECT som_department_id FROM @tab_somdepid
          )
          OR som_department_id IN
             (
                 SELECT som_department_id FROM @tab_somdepid
             )
      )
      AND
      (
          0 IN
          (
              SELECT som_division_id FROM @tab_somdivs
          )
          OR som_division_id IN
             (
                 SELECT som_division_id FROM @tab_somdivs
             )
      )
      AND
      (
          0 IN
          (
              SELECT som_division_id FROM @tab_somdivid
          )
          OR som_division_id IN
             (
                 SELECT som_division_id FROM @tab_somdivid
             )
      )--;

ORDER BY [No Show] DESC
GO


