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
	    @in_pods VARCHAR(MAX),
	    @in_hubs VARCHAR(MAX),
	    @in_somdeps VARCHAR(MAX),
	    @in_somdivs VARCHAR(MAX)

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
--(0)  --(All)
(1) --Digestive Health
--(1),--Digestive Health
--(2) --Heart and Vascular

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
*/
DECLARE @SOMDivision TABLE (SOMDivisionId int)

INSERT INTO @SOMDivision
(
    SOMDivisionId
)
VALUES
(0)--,--(All)
--(14),--40445 MD-MICR Microbiology
--(22),--40450 MD-MPHY Mole Phys & Biophysics
--(30),--40415 MD-PBHS Public Health Sciences Admin
--(48),--40700 MD-ANES Anesthesiology
--(50),--40705 MD-DENT Dentistry
--(52),--40710 MD-DERM Dermatology
--(54),--40715 MD-EMED Emergency Medicine
--(56),--40720 MD-FMED Family Medicine
--(58),--40725 MD-INMD Int Med, Admin
--(60),--40730 MD-INMD Allergy
--(66),--40735 MD-INMD CV Medicine
--(68),--40745 MD-INMD Endocrinology
--(72),--40755 MD-INMD Gastroenterology
--(74),--40760 MD-INMD Gen, Geri, Pall, Hosp
--(76),--40761 MD-INMD Hospital Medicine
--(80),--40770 MD-INMD Hem/Onc
--(82),--40771 MD-INMD Community Oncology
--(84),--40775 MD-INMD Infectious Dis
--(86),--40780 MD-INMD Nephrology
--(88),--40785 MD-INMD Pulmonary
--(90),--40790 MD-INMD Rheumatology
--(98),--40746 MD-INMD Advanced Diabetes Mgt
--(101),--40800 MD-NERS Admin
--(111),--40820 MD-NERS CV Disease
--(113),--40830 MD-NERS Deg Spinal Dis
--(115),--40835 MD-NERS Gamma Knife
--(119),--40816 MD-NERS Minimally Invasive Spine
--(121),--40840 MD-NERS Multiple Neuralgia
--(123),--40825 MD-NERS Neuro-Onc
--(127),--40810 MD-NERS Pediatric
--(129),--40849 MD-NERS Pediatric Pituitary
--(131),--40806 MD-NERS Radiosurgery
--(138),--40850 MD-NEUR Neurology
--(142),--40860 MD-OBGY Ob & Gyn, Admin
--(144),--40865 MD-OBGY Gyn Oncology
--(146),--40870 MD-OBGY Maternal Fetal Med
--(148),--40875 MD-OBGY Reprod Endo/Infertility
--(150),--40880 MD-OBGY Midlife Health
--(152),--40885 MD-OBGY Northridge
--(154),--40890 MD-OBGY Primary Care Center
--(156),--40895 MD-OBGY Gyn Specialties
--(158),--40897 MD-OBGY Midwifery
--(163),--40900 MD-OPHT Ophthalmology
--(166),--40910 MD-ORTP Ortho Surg, Admin
--(168),--40915 MD-ORTP Adult Reconst
--(178),--40930 MD-ORTP Foot/Ankle
--(184),--40940 MD-ORTP Pediatric Ortho
--(188),--40950 MD-ORTP Spine
--(190),--40955 MD-ORTP Sports Med
--(192),--40960 MD-ORTP Hand Surgery
--(194),--40961 MD-ORTP Trauma
--(197),--40970 MD-OTLY Oto, Admin
--(201),--40980 MD-OTLY Audiology
--(208),--41005 MD-PATH Surgical Path
--(210),--41010 MD-PATH Clinical Pathology
--(212),--41015 MD-PATH Neuropathology
--(214),--41017 MD-PATH Research
--(219),--41025 MD-PEDT Pediatrics, Admin
--(223),--41035 MD-PEDT Cardiology
--(225),--41040 MD-PEDT Critical Care
--(227),--41045 MD-PEDT Developmental
--(229),--41050 MD-PEDT Endocrinology
--(233),--41056 MD-PEDT Bariatrics
--(237),--41058 MD-PEDT Adolescent Medicine
--(239),--41060 MD-PEDT Gastroenterology
--(241),--41065 MD-PEDT General Pediatrics
--(243),--41070 MD-PEDT Genetics
--(245),--41075 MD-PEDT Hematology
--(249),--41085 MD-PEDT Infectious Diseases
--(251),--41090 MD-PEDT Neonatology
--(253),--41095 MD-PEDT Nephrology
--(257),--41105 MD-PEDT Pulmonary
--(260),--41130 MD-PHMR Phys Med & Rehab
--(262),--41140 MD-PLSR Plastic Surgery
--(264),--41120 MD-PSCH Psychiatry and NB Sciences
--(270),--41160 MD-RADL Radiology, Admin
--(272),--41161 MD-RADL Community Division
--(274),--41165 MD-RADL Angio/Interv
--(276),--41166 MD-RADL Non-Invasive Cardio
--(278),--41170 MD-RADL Breast Imaging
--(280),--41175 MD-RADL Thoracoabdominal
--(282),--41180 MD-RADL Musculoskeletal
--(284),--41185 MD-RADL Neuroradiology
--(286),--41186 MD-RADL Interventional Neuroradiology (INR)
--(288),--41190 MD-RADL Nuclear Medicine
--(290),--41195 MD-RADL Pediatric Rad
--(295),--41150 MD-RONC Radiation Oncology
--(297),--41210 MD-SURG Surgery, Admin
--(310),--41250 MD-UROL Urology, Admin
--(314),--41255 MD-UROL Urology, General
--(327),--40480 MD-CDBT Ctr for Diabetes Tech
--(331),--40530 MD-CPHG Ctr for Public Health Genomics
--(373),--40204 MD-DMED School of Medicine Adm
--(435),--40230 MD-DMED Curriculum
--(435),--40250 MD-DMED Clin Performance Dev
--(435),--40265 MD-DMED Med Ed Chief of Staff
;

SELECT @in_somdivs = COALESCE(@in_somdivs+',' ,'') + CAST(SOMDivisionId AS VARCHAR(MAX))
FROM @SOMDivision

--SELECT @in_somdivs


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
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_NoShowRate_SSRS_Daily
--WHO : Tom Burgan
--WHEN: 6/3/19
--WHY : Report scheduled appointment No Show Rate from Cadence.
-- 
--	Metric Calculations
--
--		Note: "SUM" can be interpreted as "SUM(event_count) WHERE ...."
--
-- No Show Rate
--				(SUM(appt_event_No_Show = 1) + SUM(appt_event_Canceled_Late = 1))
--              /
--              (SUM(appt_event_Canceled = 0) + SUM(appt_event_Canceled_Late = 1) + SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--				DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--				DS_HSDW_Prod.Rptg.vwDim_Patient
--				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--				DS_HSDW_Prod.Rptg.vwDim_Physcn
--				DS_HSDW_Prod.Rptg.vwRef_Service_Line
--				DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye
--				DS_HSDM_App.Stage.AmbOpt_Excluded_Department
--				DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined
--				DS_HSDW_Prod.Rptg.vwDim_Date
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Daily]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/03/2019 - TMB - create stored procedure
--         06/06/2019 - TMB - remove parameters/arguments logic
--         06/12/2019 - TMB - edit logic: StartDate and EndDate arguments may not have time values
--		   07/01/2019 - TMB - change logic for setting SOM hierarchy values: som_division_id (INT) => som_division_name_id (VARCHAR(150))
--         07/02/2019 - TMB - restore column som_division_id
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------
DECLARE @locStartDate SMALLDATETIME,
        @locEndDate SMALLDATETIME

SET @locStartDate = CAST(CAST(@StartDate AS DATE) AS SMALLDATETIME) + CAST(CAST('00:00:00' AS TIME) AS SMALLDATETIME)
SET @locEndDate   = CAST(DATEADD(MINUTE,-1,CAST((DATEADD(DAY,1,CAST(@EndDate AS DATE))) AS SMALLDATETIME)) AS SMALLDATETIME)

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
DECLARE @tab_deps TABLE
(
    epic_department_id NUMERIC(18,0)
);
INSERT INTO @tab_deps
SELECT Param
FROM ETL.fn_ParmParse(@in_deps, ',');
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
DECLARE @tab_somdivs TABLE
(
    som_division_id int
);
INSERT INTO @tab_somdivs
(
    som_division_id
)
SELECT Param
FROM ETL.fn_ParmParse(@in_somdivs, ',');

SELECT 
       date_dim.day_date AS event_date,
       evnts.epic_department_id,
       evnts.epic_department_name,
       evnts.epic_department_name_external,
       date_dim.fmonth_num,
       date_dim.Fyear_num,
       date_dim.FYear_name,
       CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period,
       evnts.sk_Dim_Pt,
       evnts.sk_Fact_Pt_Acct,
       evnts.sk_Fact_Pt_Enc_Clrt,
       evnts.person_birth_date,
       evnts.person_gender,
       evnts.person_id,
       evnts.person_name,
       evnts.provider_id,
       evnts.provider_name,
       evnts.service_line_id,
       evnts.service_line,
       evnts.sub_service_line_id,
       evnts.sub_service_line,
       evnts.opnl_service_id,
       evnts.opnl_service_name,
       evnts.corp_service_line_id,
       evnts.corp_service_line,
       evnts.hs_area_id,
       evnts.hs_area_name,
       evnts.pod_id,
       evnts.pod_name,
       evnts.hub_id,
       evnts.hub_name,
       evnts.prov_service_line_id,
       evnts.prov_service_line,
       evnts.prov_hs_area_id,
       evnts.prov_hs_area_name,
       evnts.APPT_STATUS_FLAG,
       evnts.APPT_STATUS_C,
       evnts.CANCEL_REASON_C,
       evnts.MRN_int,
       evnts.CONTACT_DATE,
       evnts.APPT_DT,
       evnts.PAT_ENC_CSN_ID,
       evnts.PRC_ID,
       evnts.PRC_NAME,
       evnts.sk_Dim_Physcn,
       evnts.UVaID,
       CASE WHEN evnts.VIS_NEW_TO_SYS_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SYS_YN,
       CASE WHEN evnts.VIS_NEW_TO_DEP_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_DEP_YN,
       CASE WHEN evnts.VIS_NEW_TO_PROV_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_PROV_YN,
       CASE WHEN evnts.VIS_NEW_TO_SPEC_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SPEC_YN,
       CASE WHEN evnts.VIS_NEW_TO_SERV_AREA_YN = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SERV_AREA_YN,
       CASE WHEN evnts.VIS_NEW_TO_LOC_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_LOC_YN,
       evnts.APPT_MADE_DATE,
       evnts.ENTRY_DATE,
       evnts.CHECKIN_DTTM,
       evnts.CHECKOUT_DTTM,
       evnts.VISIT_END_DTTM,
       evnts.CYCLE_TIME_MINUTES,
	   CASE WHEN evnts.appt_event_Canceled = 0 OR evnts.appt_event_Canceled_Late = 1 OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment],
	   CASE WHEN (evnts.appt_event_No_Show = 1 OR evnts.appt_event_Canceled_Late = 1) THEN 1 ELSE 0 END AS [No Show],
	   evnts.appt_event_No_Show,
	   evnts.appt_event_Canceled_Late,
       evnts.appt_event_Scheduled,
	   evnts.appt_event_Provider_Canceled,
	   evnts.appt_event_Completed,
	   evnts.appt_event_Arrived,
	   evnts.appt_event_New_to_Specialty,
       evnts.Appointment_Lag_Days,
       evnts.CYCLE_TIME_MINUTES_Adjusted,
	   evnts.APPT_DTTM,
	   evnts.CANCEL_INITIATOR,
	   evnts.CANCEL_REASON_NAME,
	   evnts.CANCEL_LEAD_HOURS,
	   evnts.APPT_CANC_DTTM,
	   evnts.Entry_UVaID,
	   evnts.Canc_UVaID,
	   evnts.PHONE_REM_STAT_NAME,
	   evnts.Cancel_Lead_Days,
	   evnts.Load_Dtm,
	   evnts.som_group_id,
	   evnts.som_group_name,
	   evnts.rev_location_id,
	   evnts.rev_location,
	   evnts.financial_division_id,
	   evnts.financial_division_name,
	   evnts.financial_sub_division_id,
	   evnts.financial_sub_division_name,
	   evnts.som_department_id,
	   evnts.som_department_name,
	   evnts.som_division_id,
	   evnts.som_division_name_id,
	   evnts.som_division_name,
	   evnts.som_division_5,
	   evnts.som_hs_area_id,
	   evnts.som_hs_area_name

FROM

    (
        SELECT DISTINCT
            main.epic_pod AS pod_id,
            main.mdmloc_pod AS pod_name,
            main.epic_hub AS hub_id,
            main.mdmloc_hub AS hub_name,
            main.epic_department_id,
            main.epic_department_name,
            main.epic_department_name_external,
            main.sk_Dim_Pt,
            main.sk_Fact_Pt_Acct,
            main.sk_Fact_Pt_Enc_Clrt,
            main.person_birth_date,
            main.person_gender,
            main.person_id,
            main.person_name,
            main.provider_id,
            main.provider_name,
            main.service_line_id,
            main.service_line,
            main.prov_service_line_id,
            main.prov_service_line,
            main.sub_service_line_id,
            main.sub_service_line,
            main.opnl_service_id,
            main.opnl_service_name,
            main.corp_service_line_id,
            main.corp_service_line,
            main.hs_area_id,
            main.hs_area_name,
            main.prov_hs_area_id,
            main.prov_hs_area_name,
            main.APPT_STATUS_FLAG,
            main.APPT_STATUS_C,
			main.CANCEL_INITIATOR,
            main.CANCEL_REASON_C,
            main.MRN_int,
            main.CONTACT_DATE,
            main.APPT_DT,
            main.PAT_ENC_CSN_ID,
            main.PRC_ID,
            main.PRC_NAME,
            main.sk_Dim_Physcn,
            main.UVaID,
            main.VIS_NEW_TO_SYS_YN,
            main.VIS_NEW_TO_DEP_YN,
            main.VIS_NEW_TO_PROV_YN,
            main.VIS_NEW_TO_SPEC_YN,
            main.VIS_NEW_TO_SERV_AREA_YN,
            main.VIS_NEW_TO_LOC_YN,
            main.APPT_MADE_DATE,
            main.ENTRY_DATE,
            main.CHECKIN_DTTM,
            main.CHECKOUT_DTTM,
            main.VISIT_END_DTTM,
            main.CYCLE_TIME_MINUTES,
                                                 -- Appt Status Flags
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'No Show' ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_No_Show,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled Late' ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Canceled_Late,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled' ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Scheduled' ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Scheduled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C = 3)
                    AND (main.CANCEL_INITIATOR = 'PROVIDER')
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Provider_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C IN ( 2 ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Completed,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C IN ( 6 ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Arrived,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.VIS_NEW_TO_SPEC_YN = 'Y')
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_New_to_Specialty,
                                                 -- Calculated columns
-- Assumes that there is always a referral creation date (CHANGE_DATE) documented when a referral entry date (ENTRY_DATE) is documented
            CASE
                WHEN (main.APPT_STATUS_FLAG IS NOT NULL) THEN
                    DATEDIFF(   dd,
                                CASE
                                    WHEN main.ENTRY_DATE IS NULL THEN
                                        main.APPT_MADE_DATE
									WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
									    main.APPT_MADE_DATE
									WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
									    main.ENTRY_DATE
                                    ELSE
                                        main.CHANGE_DATE
                                END,
                                main.APPT_DT
                            )
                ELSE
                    CAST(NULL AS INT)
            END AS Appointment_Lag_Days,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.CYCLE_TIME_MINUTES >= 960)
                ) THEN
                    960 -- Operations has defined 960 minutes (16 hours) as the ceiling for the calculation to use for any times longer than 16 hours
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.CYCLE_TIME_MINUTES < 960)
                ) THEN
                    main.CYCLE_TIME_MINUTES
                ELSE
                    CAST(NULL AS INT)
            END AS CYCLE_TIME_MINUTES_Adjusted,

			main.APPT_DTTM,
			main.CANCEL_REASON_NAME,
			main.CANCEL_LEAD_HOURS,
			main.APPT_CANC_DTTM,
			main.Entry_UVaID,
			main.Canc_UVaID,
			main.PHONE_REM_STAT_NAME,
			main.CHANGE_DATE,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled','Canceled Late' ))
                ) THEN
                    DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT)
                ELSE
                    CAST(NULL AS INT)
            END AS Cancel_Lead_Days,
			main.APPT_MADE_DTTM,
		    main.som_group_id,
			main.som_group_name,
			main.rev_location_id,
			main.rev_location,
			main.financial_division_id,
			main.financial_division_name,
			main.financial_sub_division_id,
			main.financial_sub_division_name,
			main.som_department_id,
			main.som_department_name,
			main.som_division_id,
	        main.som_division_name_id,
			main.som_division_name,
			main.som_division_5,
			main.som_hs_area_id,
			main.som_hs_area_name,
			main.Load_Dtm

        FROM
        ( --main
            SELECT appts.RPT_GRP_THIRTY AS epic_service_line,
                   mdmloc.SERVICE_LINE AS mdmloc_service_line,
                   appts.RPT_GRP_SIX AS epic_pod,
                   mdmloc.PFA_POD AS mdmloc_pod,
                   appts.RPT_GRP_SEVEN AS epic_hub,
                   mdmloc.HUB AS mdmloc_hub,
                   appts.DEPARTMENT_ID AS epic_department_id,
                   mdm.epic_department_name AS epic_department_name,
                   mdm.epic_department_name_external AS epic_department_name_external,
                   appts.sk_Dim_Pt,
                   appts.sk_Fact_Pt_Acct,
                   appts.sk_Fact_Pt_Enc_Clrt,
                   pat.BirthDate AS person_birth_date,
                   pat.Sex AS person_gender,
                   CAST(appts.IDENTITY_ID AS INT) AS person_id,
                   pat.Name AS person_name,
                   CAST(NULL AS INT) AS practice_group_id,
                   CAST(NULL AS VARCHAR(150)) AS practice_group_name,
                   appts.PROV_ID AS provider_id,
                   appts.PROV_NAME AS provider_name,
                   -- MDM
                   mdm.service_line_id,
                   mdm.service_line,
                   physsvc.Service_Line_ID AS prov_service_line_id,
                   physsvc.Service_Line AS prov_service_line,
                   mdm.sub_service_line_id,
                   mdm.sub_service_line,
                   mdm.opnl_service_id,
                   mdm.opnl_service_name,
                   mdm.corp_service_line_id,
                   mdm.corp_service_line,
                   mdm.hs_area_id,
                   mdm.hs_area_name,
                   physsvc.hs_area_id AS prov_hs_area_id,
                   physsvc.hs_area_name AS prov_hs_area_name,
                   --Select
                   appts.APPT_STATUS_FLAG,
                   appts.APPT_STATUS_C,
				   appts.CANCEL_INITIATOR,
                   appts.CANCEL_REASON_C,
				   CAST(appts.IDENTITY_ID AS INTEGER) AS MRN_int,
                   appts.CONTACT_DATE,
                   appts.APPT_DT,
                   appts.PAT_ENC_CSN_ID,
                   appts.PRC_ID,
                   appts.PRC_NAME,
                   ser.sk_Dim_Physcn,
                   doc.UVaID,
                   appts.VIS_NEW_TO_SYS_YN,
                   appts.VIS_NEW_TO_DEP_YN,
                   appts.VIS_NEW_TO_PROV_YN,
                   appts.VIS_NEW_TO_SPEC_YN,
                   appts.VIS_NEW_TO_SERV_AREA_YN,
                   appts.VIS_NEW_TO_LOC_YN,
                   appts.APPT_MADE_DATE,
                   appts.ENTRY_DATE,
                   appts.CHECKIN_DTTM,
                   appts.CHECKOUT_DTTM,
                   appts.VISIT_END_DTTM,
                   appts.CYCLE_TIME_MINUTES,
				   appts.APPT_DTTM,
				   appts.CANCEL_REASON_NAME,
				   appts.CANCEL_LEAD_HOURS,
				   appts.APPT_CANC_DTTM,
				   entryemp.EMPlye_Systm_Login AS Entry_UVaID,
				   cancemp.EMPlye_Systm_Login AS Canc_UVaID,
				   appts.PHONE_REM_STAT_NAME,
				   appts.CHANGE_DATE,
				   appts.APPT_MADE_DTTM,
				   -- Loc
				   mdmloc.LOC_ID AS rev_location_id,
				   mdmloc.REV_LOC_NAME AS rev_location,
				   -- SOM			   
				   physcn.Clrt_Financial_Division AS financial_division_id,
				   physcn.Clrt_Financial_Division_Name AS financial_division_name,
				   physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id,
				   physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name,
				   physcn.SOM_Group_ID AS som_group_id,
				   physcn.SOM_group AS som_group_name,
				   physcn.SOM_department_id AS som_department_id,
				   physcn.SOM_department AS	som_department_name,
				   physcn.SOM_division_id AS som_division_id,
				   physcn.SOM_division_name_id AS som_division_name_id,
				   physcn.SOM_division_name AS som_division_name,
				   physcn.SOM_division_5 AS	som_division_5,
				   physcn.som_hs_area_id AS	som_hs_area_id,
				   physcn.som_hs_area_name AS som_hs_area_name,
				   appts.Load_Dtm

            FROM Stage.Scheduled_Appointment AS appts
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
                    ON ser.PROV_ID = appts.PROV_ID
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
                    ON pat.sk_Dim_Pt = appts.sk_Dim_Pt
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                    ON appts.DEPARTMENT_ID = mdm.epic_department_id
                LEFT OUTER JOIN
                (
                    SELECT DISTINCT
                        EPIC_DEPARTMENT_ID,
                        SERVICE_LINE,
                        PFA_POD,
                        HUB,
						BUSINESS_UNIT,
						LOC_ID,
						REV_LOC_NAME
                    FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
                ) AS mdmloc
                    ON appts.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID
                LEFT JOIN
                (
                    SELECT sk_Dim_Physcn,
                           UVaID,
                           Service_Line
                    FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
                    WHERE current_flag = 1
                ) AS doc
                    ON ser.sk_Dim_Physcn = doc.sk_Dim_Physcn
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc
                    ON physsvc.Physician_Roster_Name = CASE
                                                           WHEN (ser.sk_Dim_Physcn > 0) THEN
                                                               doc.Service_Line
                                                           ELSE
                                                               'No Value Specified'
                                                       END
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye entryemp
				    ON entryemp.EMPlye_Usr_ID = appts.APPT_ENTRY_USER_ID
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye cancemp
				    ON cancemp.EMPlye_Usr_ID = appts.APPT_CANC_USER_ID

                -- -------------------------------------
                -- Excluded departments--
                -- -------------------------------------
                LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
				    ON excl.DEPARTMENT_ID = appts.DEPARTMENT_ID

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = doc.sk_Dim_Physcn

            WHERE (appts.APPT_DT >= @locStartDate
              AND appts.APPT_DT < @locEndDate)
			AND excl.DEPARTMENT_ID IS NULL

        ) AS main
    ) evnts
INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date date_dim
    ON (date_dim.day_date = CAST(evnts.APPT_DT AS SMALLDATETIME))

WHERE ((evnts.appt_event_Canceled = 0)
       OR ((evnts.appt_event_No_Show = 1)
	       OR (evnts.appt_event_Canceled_Late = 1)
		   OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 45)
		  )
	  )
	  AND date_dim.day_date >= @locStartDate
      AND date_dim.day_date < @locEndDate
      AND
      (
          @in_servLine = 0
          OR (COALESCE(evnts.service_line_id, evnts.opnl_service_id) IN (SELECT Service_Line_Id FROM @tab_servLine))
			 
      )
      AND
      (
          @in_pods = 0
          OR (evnts.pod_id IN (SELECT pod_id FROM @tab_pods))
      )
      AND
      (
          @in_hubs = 0
          OR (evnts.hub_id IN (SELECT hub_id FROM @tab_hubs))
      )
      AND
      (
          @in_deps = 0
          OR (evnts.epic_department_id IN (SELECT epic_department_id FROM @tab_deps))
      )
      AND
      (
          @in_somdeps = 0
          OR (evnts.som_department_id IN (SELECT som_department_id FROM @tab_somdeps))
      )
      AND
      (
          @in_somdivs = 0
          OR (evnts.som_division_id IN (SELECT som_division_id FROM @tab_somdivs))
      )

ORDER BY date_dim.day_date;

GO


