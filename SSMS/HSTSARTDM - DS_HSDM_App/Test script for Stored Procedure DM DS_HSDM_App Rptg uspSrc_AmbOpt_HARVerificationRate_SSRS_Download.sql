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

--SET @StartDate = '2/1/2019 00:00 AM'
--SET @StartDate = '2/28/2019 00:00 AM'
--SET @StartDate = '7/1/2018 00:00 AM'
--SET @EndDate = '2/28/2019 11:59 PM'
--SET @StartDate = '4/2/2019 00:00 AM'
--SET @EndDate = '5/1/2019 11:59 PM'
SET @StartDate = '4/2/2019 00:00 AM'
SET @EndDate = '5/1/2019 00:00 AM'

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
;

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
('0')  -- (All)
--(10210001) -- ECCC HEM ONC EAST
--,(10210002) -- ECCC HEM ONC WEST
--,(10210004) -- ECCC INFUSION CENTER
--,(10210013) -- ECCC RAD ONC CLINIC
--,(10210014) -- ECCC HEM ONC WOMENS
--,(10210028) -- ECCC MED DHC EAST CL
--,(10210035) -- ECCC GYN ONC WOMENS
--,(10210036) -- ECCC CARD DEVICE CL
--,(10210054) -- ECCC NEPHROLOGY WEST
--,(10211003) -- F415 RHEUMATOLOGY
--,(10211006) -- F415 ENDOCRINE
--,(10211012) -- F415 ALLERGY OTO
--,(10211013) -- F415 MED PITUITARY
--,(10211021) -- F415 NEUROSURGERY
--,(10212016) -- F500 CARDIOLOGY
--,(10212019) -- F500 ENDOCRINOLOGY
--,(10217002) -- JPA SLEEP LAB
--,(10217003) -- JPA UNIV MED ASSOCS
--,(10220001) -- LOID MEDICAL ASSOC
--,(10222003) -- LBSC KIDNEY TRANSPLANT
--,(10228003) -- NRDG MIDLIFE
--,(10228006) -- NRDG ECHO LAB
--,(10228007) -- NRDG ALLERGY
--,(10230004) -- ORUL UNIV PHYSICIANS
--,(10230005) -- ORUL MED NEPHROLOGY
--,(10230009) -- ORUL MED CARD
--,(10239003) -- UVMS NEPHROLOGY
--,(10239007) -- UVMS RAD MAMMOGRAPHY
--,(10239017) -- UVMS TRANSPLANT LIVER
--,(10239018) -- UVMS TRANSPLANT LUNG
--,(10239019) -- UVMS TRANSPLANT KIDNEY
--,(10239020) -- UVMS SURG TRANSPLANT
--,(10239023) -- UVMS MED GI CL NEPH
--,(10239024) -- UVMS ID TRANSPLANT
--,(10239026) -- UVMS ENDO TRANSPLANT
--,(10242005) -- UVPC DERMATOLOGY
--,(10242006) -- UVPC OB-GYN
--,(10242007) -- UVPC NEUROLOGY
--,(10242008) -- UVPC CARDIOLOGY
--,(10242018) -- UVPC PULMONARY
--,(10242019) -- UVPC PULM FUNC LAB
--,(10242047) -- ZZZZ SURG CARDTHORACIC
--,(10242050) -- UVPC ALLERGY PULM
--,(10243003) -- UVHE DIGESTIVE HEALTH
--,(10243009) -- UVHE ECG
--,(10243013) -- UVHE ELECTROPHYSIOLOGY
--,(10243020) -- UVHE GI MOTILITY
--,(10243097) -- UVHE MED CARD CL
--,(10243098) -- UVHE MED MONITORS
--,(10243114) -- UVHE ALLERGY DHC
--,(10244016) -- UVWC INFECTIOUS DIS
--,(10244023) -- UVWC MED GI CL
--,(10244024) -- UVWC ENDOCRN BARIATRIC
--,(10244028) -- UVWC GASTRO ID
--,(10244029) -- UVWC OBGYN ID
--,(10246004) -- WALL MED DH FAM MED
--,(10246005) -- WALL ENDO MED FAMILY
--,(10250001) -- UVBR NEUROSURGERY
--,(10256002) -- WSRS INFECTIOUS DIS
--,(10257001) -- CPNM MADISON PRIMARY C
--,(10257004) -- CPNM ENDOCRINE
--,(10275003) -- CPBR ALLERGY
--,(10277003) -- LGGH KIDNEY TRANSPLANT
--,(10280001) -- AUBL UVA SPTY CARE
--,(10280008) -- AUBL UVA CANC INF AUG
--,(10295005) -- CPSE CH CANC CTR
--,(10299001) -- CVSR NEPHROLOGY
--,(10299002) -- CVSR ENDOCRINE
--,(10306001) -- CVPJ UVA CANC CTR PTP
--,(10306003) -- CVPJ UVA CANC INF PTP
--,(10341001) -- CVPE UVA RHEU PANTOPS
--,(10341009) -- CVPE MED NEPHROLOGY
--,(10348003) -- ZCSC CARDIOLOGY
--,(10348005) -- ZCSC ENDOCRINE
--,(10348007) -- ZCSC NEPHROLOGY
--,(10348011) -- ZCSC PULMONARY
--,(10348039) -- ZCSC ALLERGY
--,(10353001) -- AUPN SP CARE NEPH
--,(10353003) -- AUPN SP CARE ENDO
--,(10353004) -- AUPN SP CARE PULM
--,(10353005) -- AUPN SP CARE RHEU
--,(10354007) -- UVBB PULM FUNC TESTING
--,(10354014) -- UVBB PEDS GASTRO CL
--,(10354018) -- UVBB PEDS CARD CL FL 6
--,(10354025) -- UVBB PEDS RESP MED FL6
--,(10354035) -- UVBB MATERNAL FETAL CL
--,(10354047) -- UVBB MED CARD CONGEN
--,(10354052) -- UVBB PEDS GI MOT
--,(10354074) -- UVBB PEDS RESP MED FL5
--,(10369001) -- CPSA CARDIOLOGY
--,(10374001) -- ROFR KIDNEY TRANSPLANT
--,(10377001) -- PAMS PRIMARY CARE
--,(10378001) -- WCVD KIDNEY TRANSPLANT
--,(10379900) -- UVML ENDOBRONC PROC
--,(10387001) -- CVSM PRIMARY CARE
--,(10390005) -- CPBE ENDOCRINE
--,(10399001) -- CVSN ENDOCRINE
-- (10210042) -- ECCC DERMATOLOGY WEST
--,(10210035) -- ECCC GYN ONC WOMENS
--,(10210001) -- ECCC HEM ONC EAST
--,(10210002) -- ECCC HEM ONC WEST
--,(10210014) -- ECCC HEM ONC WOMENS
--,(10210004) -- ECCC INFUSION CENTER
--,(10210028) -- ECCC MED DHC EAST CL
--,(10210030) -- ECCC NEURO WEST
--,(10210032) -- ECCC NSURG WEST
--,(10210026) -- ECCC OTO CL EAST
--,(10210027) -- ECCC OTO SPEECH EAST
--,(10210006) -- ECCC PALLIATIVE CLINIC
--,(10210057) -- ECCC PHYSMED&REHB WMS
--,(10210010) -- ECCC POSITIVE IMAGE
--,(10210017) -- ECCC PSY CL 1FL
--,(10210007) -- ECCC RAD CT
--,(10210045) -- ECCC RAD E&M CL WEST
--,(10210060) -- ECCC RAD E&M CL WMS
--,(10210018) -- ECCC RAD NUC MED CL
--,(10210008) -- ECCC RAD NUCLEAR
--,(10210013) -- ECCC RAD ONC CLINIC
--,(10210005) -- ECCC RAD ONC THERAPY
--,(10210009) -- ECCC RAD PET
--,(10210015) -- ECCC RONC PROCEDURE RM
--,(10210061) -- ECCC STRESS LAB
--,(10210016) -- ECCC SUPPORT SVCS 1FL
--,(10210022) -- ECCC SURG ONC EAST
--,(10210029) -- ECCC SURG ONC WEST
--,(10210052) -- ECCC UROLOGY WEST
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
--('1') --Cancer
--('1'), --Cancer
--('10') --Women's and Children's
;

SELECT @in_pods = COALESCE(@in_pods+',' ,'') + CAST(PodId AS VARCHAR(MAX))
FROM @Pod

--SELECT @in_pods

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
--('3'),--ECCC
--('10') --500 Fontaine
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
--('57'),--MD-INMD Internal Medicine
--('292')--,--MD-SURG Surgery
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
--('310')--,--MD-UROL Urology, General
--('80'),--MD-INMD Hem/Onc
--('76')--,--MD-INMD Hospital Medicine
;

SELECT @in_somdivs = COALESCE(@in_somdivs+',' ,'') + CAST(SOMDivisionId AS VARCHAR(MAX))
FROM @SOMDivision

--SELECT @in_somdivs

SELECT @in_somdivid = COALESCE(@in_somdivid+',' ,'') + CAST(SOMDivisionId AS VARCHAR(MAX))
FROM @SOMDivision


-- =========================================================
-- Author:		Tom Burgan
-- Create date: 04/12/2019
-- Description:	HAR Verification Rate Data Portal SSRS export script
-- =========================================================
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         04/12/2019 - Tom		-- create stored procedure
--         04/16/2019 - Tom     -- edit logic for filtering by event_date
--         05/01/2019 - TMB     -- add logic for SOM Department and SOM Division report parameters
--************************************************************************************************************************
--ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_HARVerificationRate_SSRS_Download]
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

SET NOCOUNT ON;

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME,
        @locdt INT;

SET @locstartdate = @StartDate;
SET @locenddate   = DATEADD(mi,-1,DATEADD(dd,1,@EndDate)); -- TabRptg table event_date value has date and time of appointment
SET @locdt = 5; -- Default days between HAR Verification date and appointment date for scorecard

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
       event_type, -- 'HAR_Verification'
	   event_category, -- NULL
       event_count, -- NULL
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
	   1 AS [HAR Encounter],
	   CAST(CASE WHEN [HAR_Verif_Status_C] IN ('1','6','8','12','13') AND [HAR_Verification_DATEDIFF] >= @locdt THEN 1 ELSE 0 END AS INT) AS [HAR Verification],
	   [Ins Verification],
	   [E-Verified],
	   [RTE Enabled],
	   [RTE Override case Rate],
	   MYCHART_STATUS_NAME,
	   APPT_STATUS_C,
	   enc.Appt_Dtm AS APPT_DTTM,
	   CANCEL_REASON_C,
	   CANCEL_INITIATOR,
	   CANCEL_LEAD_HOURS,
	   Cancel_Lead_Days,
	   APPT_CANCEL_DATE,
	   appt_event_Canceled,
	   appt_event_Canceled_Late,
	   appt_event_Provider_Canceled,
	   HAR_Verified_Date,
	   INS_Verified_Date,
	   HAR_Verif_Status,
	   HAR_Verif_Status_C,
	   PAYOR_ID,
	   PAYOR_NAME,
	   PLAN_ID,
	   BENEFIT_PLAN_NAME,
	   ACCT_FIN_CLASS_C,
	   HSP_ACCOUNT_ID,
	   COVERAGE_ID,
	   Appt_Entry_User,
	   HAR_Verif_User,
	   HAR_Verification_DATEDIFF,
	   APPT_MADE_DTTM,
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
FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_HARVerification_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt enc
ON enc.sk_Fact_Pt_Enc_Clrt = tabrptg.sk_Fact_Pt_Enc_Clrt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct_Aggr acct
ON acct.sk_Fact_Pt_Acct = tabrptg.sk_Fact_Pt_Acct
WHERE 1 = 1
      AND event_date >= @locstartdate
      AND event_date <= @locenddate
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

ORDER BY [HAR Verification] DESC
       , event_date

GO


