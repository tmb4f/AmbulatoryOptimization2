USE DS_HSDM_App

DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate SMALLDATETIME = NULL

SET @startdate = '7/1/2019 00:00 AM'
SET @enddate = '9/30/2019 11:59 PM'

SET NOCOUNT ON
 
	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

IF OBJECT_ID('tempdb..#events_list ') IS NOT NULL
DROP TABLE #events_list

DECLARE @Pod TABLE (PodName VARCHAR(100))

INSERT INTO @Pod
(
    PodName
)
VALUES
--('Cancer'),
--('Musculoskeletal'),
--('Primary Care'),
--('Surgical Procedural Specialties'),
--('Transplant'),
--('Medical Specialties'),
--('Radiology'),
--('Heart and Vascular Center'),
--('Neurosciences and Psychiatry'),
--('Women''s and Children''s'),
--('CPG'),
--('UVA Community Cancer POD'),
--('Digestive Health'),
--('Ophthalmology'),
--('Community Medicine')
--('Medical Specialties')
('Transplant')
--('Digestive Health')
;

DECLARE @ServiceLine TABLE (ServiceLineName VARCHAR(150))

INSERT INTO @ServiceLine
(
    ServiceLineName
)
VALUES
--('Digestive Health'),
--('Heart and Vascular'),
--('Medical Subspecialties'),
--('Musculoskeletal'),
--('Neurosciences and Behavioral Health'),
--('Oncology'),
--('Ophthalmology'),
--('Primary Care'),
--('Surgical Subspecialties'),
--('Transplant'),
--('Womens and Childrens')
--('Medical Subspecialties')
('Digestive Health')
--('Womens and Childrens')
;

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
 (10243003) -- UVHE DIGESTIVE HEALTH
 --(10239003) -- UVMS NEPHROLOGY
 --(10354015) -- UVBB PEDS ONCOLOGY CL
;

DECLARE @StaffResource TABLE (Resource_Type VARCHAR(8))

INSERT INTO @StaffResource
(
    Resource_Type
)
VALUES
-- ('Person')
--,('Resource)
 ('Person')
 --('Resource)
;

DECLARE @ProviderType TABLE (Provider_Type VARCHAR(40))

INSERT INTO @ProviderType
(
    Provider_Type
)
VALUES
--('Anesthesiologist') -- Person
--,('Audiologist') -- Person
--,('Case Manager') -- Person
--,('Clinical Social Worker') -- Person
--,('Community Provider') -- Person
--,('Counselor') -- Person
--,('Dentist') -- Person
--,('Doctor of Philosophy') -- Person
--,('Fellow') -- Person
--,('Financial Counselor') -- Person
--,('Genetic Counselor') -- Person
--,('Health Educator') -- Person
--,('Hygienist') -- Person
--,('Licensed Clinical Social Worker') -- Person
--,('Licensed Nurse') -- Person
--,('Medical Assistant') -- Person
--,('Medical Student') -- Person
--,('Nurse Practitioner') -- Person
--,('Occupational Therapist') -- Person
--,('Optometrist') -- Person
--,('P&O Practitioner') -- Person
--,('Pharmacist') -- Person
--,('Physical Therapist') -- Person
--,('Physical Therapy Assistant') -- Person
--,('Physician') -- Person
--,('Physician Assistant') -- Person
--,('Psychiatrist') -- Person
--,('Psychologist') -- Person
--,('RD Intern') -- Person
--,('Registered Dietitian') -- Person
--,('Registered Nurse') -- Person
--,('Resident') -- Person
--,('Scribe') -- Person
--,('Speech and Language Pathologist') -- Person
--,('Technician') -- Person
--,('Unknown') -- Person
--,('Nutritionist') -- Resource
--,('Pharmacist') -- Resource
--,('Registered Dietitian') -- Resource
--,('Registered Nurse') -- Resource
--,('Resident') -- Resource
--,('Resource') -- Resource
--,('Social Worker') -- Resource
--,('Unknown') -- Resource
--,('Financial Counselor') -- Unknown
--,('Nutritionist') -- Unknown
('Physician') -- Person
,('Physician Assistant') -- Person
,('Fellow') -- Person
,('Nurse Practitioner') -- Person
;

DECLARE @Provider TABLE (ProviderId VARCHAR(18))

INSERT INTO @Provider
(
    ProviderId
)
VALUES
 --('28813') -- FISHER, JOSEPH D
 --('1300563') -- ARTH INF
 --('41806') -- NORTHRIDGE DEXA
 --('1301100') -- CT6
 --('82262') -- CT APPOINTMENT ERC
 --('40758') -- PAYNE, PATRICIA
 --('73571') -- LEEDS, JOSEPH THOMAS
 --,('29303') -- KALANTARI, KAMBIZ
 --('73725') -- ROSS, BUERLEIN
 --('41013') -- MANN, JAMES A
 ('85744') -- CORBETT, SUSAN
;

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

--SELECT 
--       [provider_id]
--      ,[provider_name]
--      ,[PAT_ENC_CSN_ID]
--	  --,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
--	  --,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
--	  ,[sk_Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
--      ,[event_type]
--      ,[event_count]
--      ,[event_date]
--      ,[event_id]
--      ,[event_category]
--      ,[epic_department_id]
--      ,[epic_department_name]
--      ,[epic_department_name_external]
--      ,[fmonth_num]
--      ,[fyear_num]
--      ,[fyear_name]
--      ,[report_period]
--      ,[report_date]
--      ,[peds]
--      ,[transplant]
--      ,[sk_Dim_Pt]
--      ,[sk_Fact_Pt_Acct]
--      ,[sk_Fact_Pt_Enc_Clrt]
--      ,[person_birth_date]
--      ,[person_gender]
--      ,[person_id]
--      ,[person_name]
--      ,[practice_group_id]
--      ,[practice_group_name]
--      --,[provider_id]
--      --,[provider_name]
--      ,[service_line_id]
--      ,[service_line]
--      ,[sub_service_line_id]
--      ,[sub_service_line]
--      ,[opnl_service_id]
--      ,[opnl_service_name]
--      ,[corp_service_line_id]
--      ,[corp_service_line_name]
--      ,[hs_area_id]
--      ,[hs_area_name]
--      ,[pod_id]
--      ,[pod_name]
--      ,[hub_id]
--      ,[hub_name]
--      ,[w_department_id]
--      ,[w_department_name]
--      ,[w_department_name_external]
--      ,[w_practice_group_id]
--      ,[w_practice_group_name]
--      ,[w_service_line_id]
--      ,[w_service_line_name]
--      ,[w_sub_service_line_id]
--      ,[w_sub_service_line_name]
--      ,[w_opnl_service_id]
--      ,[w_opnl_service_name]
--      ,[w_corp_service_line_id]
--      ,[w_corp_service_line_name]
--      ,[w_report_period]
--      ,[w_report_date]
--      ,[w_hs_area_id]
--      ,[w_hs_area_name]
--      ,[w_pod_id]
--      ,[w_pod_name]
--      ,[w_hub_id]
--      ,[w_hub_name]
--      ,[prov_service_line_id]
--      ,[prov_service_line_name]
--      ,[prov_hs_area_id]
--      ,[prov_hs_area_name]
--      ,[APPT_STATUS_FLAG]
--      ,[APPT_STATUS_C]
--      ,[CANCEL_REASON_C]
--      ,[MRN_int]
--      ,[CONTACT_DATE]
--      ,[APPT_DT]
--      --,[PAT_ENC_CSN_ID]
--      ,[PRC_ID]
--      ,[PRC_NAME]
--      ,[sk_Dim_Physcn]
--      ,[UVaID]
--      ,[VIS_NEW_TO_SYS_YN]
--      ,[VIS_NEW_TO_DEP_YN]
--      ,[VIS_NEW_TO_PROV_YN]
--      ,[VIS_NEW_TO_SPEC_YN]
--      ,[VIS_NEW_TO_SERV_AREA_YN]
--      ,[VIS_NEW_TO_LOC_YN]
--      ,[APPT_MADE_DATE]
--      ,[ENTRY_DATE]
--	  ,[CHANGE_DATE]
--      ,[CHECKIN_DTTM]
--      ,[CHECKOUT_DTTM]
--      ,[VISIT_END_DTTM]
--      ,[CYCLE_TIME_MINUTES]
--      ,[appt_event_No_Show]
--      ,[appt_event_Canceled_Late]
--      ,[appt_event_Canceled]
--      ,[appt_event_Scheduled]
--      ,[appt_event_Provider_Canceled]
--      ,[appt_event_Completed]
--      ,[appt_event_Arrived]
--      ,[appt_event_New_to_Specialty]
--      ,[Appointment_Lag_Days]
--      ,[CYCLE_TIME_MINUTES_Adjusted]
--      ,[Load_Dtm]
--      ,[DEPT_SPECIALTY_NAME]
--      ,[PROV_SPECIALTY_NAME]
--      ,[APPT_DTTM]
--      ,[ENC_TYPE_C]
--      ,[ENC_TYPE_TITLE]
--      ,[APPT_CONF_STAT_NAME]
--      ,[ZIP]
--      ,[APPT_CONF_DTTM]
--      ,[SIGNIN_DTTM]
--      ,[ARVL_LIST_REMOVE_DTTM]
--      ,[ROOMED_DTTM]
--      ,[NURSE_LEAVE_DTTM]
--      ,[PHYS_ENTER_DTTM]
--      ,[CANCEL_REASON_NAME]
--      ,[financial_division]
--      ,[financial_subdivision]
--      ,[CANCEL_INITIATOR]
--      ,[F2_Flag]
--      ,[TIME_TO_ROOM_MINUTES]
--      ,[TIME_IN_ROOM_MINUTES]
--      ,[BEGIN_CHECKIN_DTTM]
--      ,[PAGED_DTTM]
--      ,[FIRST_ROOM_ASSIGN_DTTM]
--      ,[CANCEL_LEAD_HOURS]
--      ,[APPT_CANC_DTTM]
--      ,[Entry_UVaID]
--      ,[Canc_UVaID]
--	  ,[Cancel_Lead_Days]
--      ,[financial_division_id]
--      ,[financial_division_name]
--      ,[financial_sub_division_id]
--      ,[financial_sub_division_name]
--      ,[rev_location_id]
--      ,[rev_location]
--      ,[som_group_id]
--      ,[som_group_name]
--      ,[som_department_id]
--      ,[som_department_name]
--      ,[som_division_id]
--      ,[w_financial_division_id]
--      ,[w_financial_division_name]
--      ,[w_financial_sub_division_id]
--      ,[w_financial_sub_division_name]
--      ,[w_rev_location_id]
--      ,[w_rev_location]
--      ,[w_som_group_id]
--      ,[w_som_group_name]
--      ,[w_som_department_id]
--      ,[w_som_department_name]
--      ,[w_som_division_id]
--      ,[som_division_name]
--      ,[w_som_division_name]
--      ,[APPT_MADE_DTTM]
--      ,[BUSINESS_UNIT]
--      ,[Prov_Typ]
--      ,[Staff_Resource]
--      ,[som_division_5]
--      ,[w_som_hs_area_id]
--      ,[w_som_hs_area_name]
--      ,[APPT_SERIAL_NUM]
--      ,[RESCHED_APPT_CSN_ID]
--      ,[Appointment_Request_Date]
--      ,[Appointment_Lag_Business_Days]
--      ,[BILL_PROV_YN]
--      ,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
--	  ,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
--	  ,CASE WHEN Prov_Typ IN ('Fellow','Nurse Practitioner','Physician','Physician Assistant') THEN 'Y' ELSE 'N' END AS Target_Provider_Type
;

SELECT 
       [APPT_DT]
      ,[APPT_DTTM]
      ,[epic_department_id]
      ,[epic_department_name]
      ,[epic_department_name_external]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
	  ,[provider_id]
      ,[provider_name]
      ,[Staff_Resource]
      ,[Prov_Typ]
      ,[BILL_PROV_YN]
      ,[w_service_line_name]
      ,[sub_service_line]
      ,[opnl_service_name]
      ,[corp_service_line_name]
      ,[hs_area_name]
      ,[pod_name]
      ,[hub_name]
      ,[prov_service_line_name]
      ,[prov_hs_area_name]
      ,[APPT_STATUS_FLAG]
      ,[CANCEL_REASON_C]
      ,[CANCEL_REASON_NAME]
      ,[CANCEL_INITIATOR]
      ,[CANCEL_LEAD_HOURS]
      ,[APPT_CANC_DTTM]
	  ,[PAT_ENC_CSN_ID]
      ,[PRC_NAME]
      ,[UVaID]
      ,[Entry_UVaID]
      ,[Canc_UVaID]
      ,[VIS_NEW_TO_SYS_YN]
      ,[VIS_NEW_TO_DEP_YN]
      ,[VIS_NEW_TO_PROV_YN]
      ,[VIS_NEW_TO_SPEC_YN]
      ,[VIS_NEW_TO_SERV_AREA_YN]
      ,[VIS_NEW_TO_LOC_YN]
      ,[APPT_MADE_DATE]
      ,[ENTRY_DATE]
      ,[CHECKIN_DTTM]
      ,[CHECKOUT_DTTM]
      ,[VISIT_END_DTTM]
      ,[CYCLE_TIME_MINUTES]
	  ,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  --,CASE WHEN Prov_Typ IN ('Fellow','Nurse Practitioner','Physician','Physician Assistant') AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)) THEN 1 ELSE 0 END AS [Appointment]
	  --,CASE WHEN Prov_Typ IN ('Fellow','Nurse Practitioner','Physician','Physician Assistant') AND (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Bump]
	  ,[Cancel_Lead_Days]
      ,[appt_event_Provider_Canceled]
      ,[appt_event_No_Show]
      ,[appt_event_Canceled_Late]
      ,[appt_event_Scheduled]
      ,[appt_event_Completed]
      ,[appt_event_Arrived]
      ,[appt_event_New_to_Specialty]
      ,[Appointment_Lag_Days]
      ,[CYCLE_TIME_MINUTES_Adjusted]
	  ,PHONE_REM_STAT_NAME
      ,[Load_Dtm]
	  --,CASE WHEN Prov_Typ IN ('Fellow','Nurse Practitioner','Physician','Physician Assistant') THEN 'Y' ELSE 'N' END AS Target_Provider_Type

  INTO #events_list

  FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  WHERE
  ((event_count = 1) AND ((appt_event_Canceled = 0) OR ((appt_event_Canceled_Late = 1) OR (appt_event_Provider_Canceled = 1))))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT Staff_Resource FROM @StaffResource WHERE Staff_Resource = Staff_Resource)
  --AND EXISTS(SELECT Provider_Type FROM @ProviderType WHERE Provider_Type = Prov_Typ)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)

--SELECT 
--	FY2007.fyear_num
--  --, FY2007.fmonth_num
--  , FY2007.APPT_STATUS_FLAG
--  , FY2007.CANCEL_INITIATOR
--  , FY2007.Target_Provider_Type
--  , FY2007.Scheduled AS FY2007_Scheduled
--  , FY2007.CLD_NULL AS FY2007_CLD_NULL
--  , FY2007.CLD_LTE_45 AS FY2007_CLD_LE_45
--  , FY2007.CLD_GT_45 AS FY2007_CLD_GT_45
--  , FY2008.Scheduled AS FY2008_Scheduled
--  , FY2008.CLD_NULL AS FY2008_CLD_NULL
--  , FY2008.CLD_LTE_45 AS FY2008_CLD_LE_45
--  , FY2008.CLD_GT_45 AS FY2008_CLD_GT_45
--FROM
--(
--	SELECT fyear_num
--       --, fmonth_num
--	   , APPT_STATUS_FLAG
--	   , CANCEL_INITIATOR
--	   , Target_Provider_Type
--	   , COUNT(*) AS Scheduled
--	   , SUM(CASE WHEN Cancel_Lead_Days IS NULL THEN 1 ELSE 0 END) AS CLD_NULL
--	   , SUM(CASE WHEN Cancel_Lead_Days IS NOT NULL AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END) AS CLD_LTE_45
--	   , SUM(CASE WHEN Cancel_Lead_Days IS NOT NULL AND Cancel_Lead_Days > 45 THEN 1 ELSE 0 END) AS CLD_GT_45
--    FROM #events_list
--    WHERE fyear_num = 2020
--    AND fmonth_num = 1
--    AND APPT_STATUS_FLAG NOT IN ('Left without seen','Scheduled')
--    GROUP BY fyear_num
--           --, fmonth_num
--		   , APPT_STATUS_FLAG
--	       , CANCEL_INITIATOR
--	       , Target_Provider_Type
--)
--FY2007
--LEFT OUTER JOIN
--(
--	SELECT fyear_num
--       --, fmonth_num
--	   , APPT_STATUS_FLAG
--	   , CANCEL_INITIATOR
--	   , Target_Provider_Type
--	   , COUNT(*) AS Scheduled
--	   , SUM(CASE WHEN Cancel_Lead_Days IS NULL THEN 1 ELSE 0 END) AS CLD_NULL
--	   , SUM(CASE WHEN Cancel_Lead_Days IS NOT NULL AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END) AS CLD_LTE_45
--	   , SUM(CASE WHEN Cancel_Lead_Days IS NOT NULL AND Cancel_Lead_Days > 45 THEN 1 ELSE 0 END) AS CLD_GT_45
--    FROM #events_list
--    WHERE fyear_num = 2020
--    AND fmonth_num = 2
--    AND APPT_STATUS_FLAG NOT IN ('Left without seen','Scheduled')
--    GROUP BY fyear_num
--           --, fmonth_num
--		   , APPT_STATUS_FLAG
--	       , CANCEL_INITIATOR
--	       , Target_Provider_Type
--)
--FY2008
--ON FY2008.fyear_num = FY2007.fyear_num
--AND FY2008.APPT_STATUS_FLAG = FY2007.APPT_STATUS_FLAG
--AND COALESCE(FY2008.CANCEL_INITIATOR,'Not Canceled') = COALESCE(FY2007.CANCEL_INITIATOR,'Not Canceled')
--AND FY2008.Target_Provider_Type = FY2007.Target_Provider_Type
--ORDER BY FY2007.fyear_num
--	   --, fmonth_num
--	   , FY2007.APPT_STATUS_FLAG
--	   , FY2007.CANCEL_INITIATOR
--	   , FY2007.Target_Provider_Type
	SELECT *
    --SELECT fyear_num
    --   , fmonth_num
	   --, APPT_STATUS_FLAG
	   --, CANCEL_INITIATOR
	   ----, Prov_Typ
	   --, Target_Provider_Type
	   --, COUNT(*) AS Scheduled
	   --, SUM(CASE WHEN Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END) AS CLD_LTE_45
	   --, SUM(CASE WHEN Cancel_Lead_Days > 45 THEN 1 ELSE 0 END) AS CLD_GT_45
	   ----, Appointment
	   ----, Bump
    ----   ,[appt_event_No_Show]
    ----   ,[appt_event_Canceled_Late]
    ----   ,[appt_event_Canceled]
    ----   ,[appt_event_Provider_Canceled]
    ----   ,[appt_event_Completed]
    ----   ,[appt_event_Arrived]
  FROM #events_list
  --WHERE fyear_num = 2020
  ----AND fmonth_num = 2
  --AND fmonth_num IN (1,2)
  --AND APPT_STATUS_FLAG NOT IN ('Left without seen','Scheduled')
  --GROUP BY fyear_num
  --       , fmonth_num
		-- , APPT_STATUS_FLAG
	 --    , CANCEL_INITIATOR
	 --    , Target_Provider_Type
		-- --, Prov_Typ
  --ORDER BY pod_id
  --       , epic_department_id
		-- , provider_id
  --       , event_date
  --ORDER BY pod_id
  --       , epic_department_id
		-- , CYCLE_TIME_MINUTES_Adjusted DESC
  --ORDER BY w_service_line_id
  --       , epic_department_id
		-- --, provider_id
  --       , event_date
  --ORDER BY PAT_ENC_CSN_ID
  --       , w_service_line_id
  --       , epic_department_id
		-- , provider_id
  --       , event_date
  --ORDER BY person_id
  --       , event_date
  --ORDER BY provider_name
  --       , PAT_ENC_CSN_ID
  --ORDER BY APPT_STATUS_FLAG
  --       , event_date
  --ORDER BY fyear_num
  --       , fmonth_num
		-- , APPT_STATUS_FLAG
	 --    , CANCEL_INITIATOR
	 --    , Target_Provider_Type
		-- --, Prov_Typ
		-- --, Cancel_Lead_Days
  --ORDER BY person_name
  --       , event_date
  ORDER BY epic_department_name
         , APPT_DTTM
