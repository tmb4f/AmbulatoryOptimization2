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
	    @in_hubid VARCHAR(MAX)

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
--(0)  --(All)
(1) --Digestive Health
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

-- =========================================================
-- Author:		Tom Burgan
-- Create date: 04/17/2019
-- Description:	Appointment Slot Utilization Rate Data Portal SSRS export script
-- =========================================================
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         04/17/2019 - Tom		-- create stored procedure
--************************************************************************************************************************
--CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_AppointmentSlotUtilizationRate_SSRS_Download]
--    @StartDate SMALLDATETIME,
--    @EndDate SMALLDATETIME,
--    @in_servLine VARCHAR(MAX),
--    @in_deps VARCHAR(MAX),
--    @in_depid VARCHAR(MAX),
--	@in_pods VARCHAR(MAX),
--	@in_podid VARCHAR(MAX),
--	@in_hubs VARCHAR(MAX),
--	@in_hubid VARCHAR(MAX)
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

SELECT
       event_date,
       event_type, -- 'Slot Utilization'
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
	   Regular_Openings,
	   Overbook_Openings,
	   Openings_Booked,
	   Regular_Openings_Available,
	   Regular_Openings_Unavailable,
	   Overbook_Openings_Available,
	   Overbook_Openings_Unavailable,
	   Regular_Openings_Booked,
	   Overbook_Openings_Booked,
	   Regular_Outside_Template_Booked,
	   Overbook_Outside_Template_Booked,
	   Regular_Openings_Available_Booked,
	   Overbook_Openings_Available_Booked,
	   Regular_Openings_Unavailable_Booked,
	   Overbook_Openings_Unavailable_Booked,
	   Regular_Outside_Template_Available_Booked,
	   Overbook_Outside_Template_Available_Booked,
	   Regular_Outside_Template_Unavailable_Booked,
	   Overbook_Outside_Template_Unavailable_Booked,
	   BUSINESS_UNIT,
	   STAFF_RESOURCE,
	   PROV_TYPE,
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
FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_SlotUtilization_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt enc
ON enc.sk_Fact_Pt_Enc_Clrt = tabrptg.sk_Fact_Pt_Enc_Clrt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct_Aggr acct
ON acct.sk_Fact_Pt_Acct = tabrptg.sk_Fact_Pt_Acct
WHERE 1 = 1
      AND event_date >= @StartDate
      AND event_date <= @EndDate
	  AND (event_count = 1)
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
      );

GO


