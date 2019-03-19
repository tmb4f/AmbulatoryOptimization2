USE DS_HSDM_App

DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate SMALLDATETIME = NULL

--SET @startdate = '7/20/2018 00:00 AM'
--SET @enddate = '7/20/2018 11:59 PM'
--SET @startdate = '6/5/2018 00:00 AM'
--SET @enddate = '6/5/2018 11:59 PM'
--SET @startdate = '6/4/2018 00:00 AM'
--SET @enddate = '6/6/2018 11:59 PM'
--SET @startdate = '1/1/2017 00:00 AM'
SET @startdate = '10/1/2018 00:00 AM'
SET @enddate = '2/28/2019 11:59 PM'
--SET @enddate = '6/30/2019 11:59 PM'

SET NOCOUNT ON

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
('Digestive Health')
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
 ('73725') -- ROSS, BUERLEIN
;

SELECT 
       [provider_id]
      ,[provider_name]
      ,[PAT_ENC_CSN_ID]
	  --,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  --,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  ,[sk_Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
      ,[event_type]
      ,[event_count]
      ,[event_date]
      ,[event_id]
      ,[event_category]
      ,[epic_department_id]
      ,[epic_department_name]
      ,[epic_department_name_external]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[report_period]
      ,[report_date]
      ,[peds]
      ,[transplant]
      ,[sk_Dim_Pt]
      ,[sk_Fact_Pt_Acct]
      ,[sk_Fact_Pt_Enc_Clrt]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      ,[practice_group_id]
      ,[practice_group_name]
      --,[provider_id]
      --,[provider_name]
      ,[service_line_id]
      ,[service_line]
      ,[sub_service_line_id]
      ,[sub_service_line]
      ,[opnl_service_id]
      ,[opnl_service_name]
      ,[corp_service_line_id]
      ,[corp_service_line_name]
      ,[hs_area_id]
      ,[hs_area_name]
      ,[pod_id]
      ,[pod_name]
      ,[hub_id]
      ,[hub_name]
      ,[w_department_id]
      ,[w_department_name]
      ,[w_department_name_external]
      ,[w_practice_group_id]
      ,[w_practice_group_name]
      ,[w_service_line_id]
      ,[w_service_line_name]
      ,[w_sub_service_line_id]
      ,[w_sub_service_line_name]
      ,[w_opnl_service_id]
      ,[w_opnl_service_name]
      ,[w_corp_service_line_id]
      ,[w_corp_service_line_name]
      ,[w_report_period]
      ,[w_report_date]
      ,[w_hs_area_id]
      ,[w_hs_area_name]
      ,[w_pod_id]
      ,[w_pod_name]
      ,[w_hub_id]
      ,[w_hub_name]
      ,[prov_service_line_id]
      ,[prov_service_line_name]
      ,[prov_hs_area_id]
      ,[prov_hs_area_name]
      ,[APPT_STATUS_FLAG]
      ,[APPT_STATUS_C]
      ,[CANCEL_REASON_C]
      ,[MRN_int]
      ,[CONTACT_DATE]
      ,[APPT_DT]
      --,[PAT_ENC_CSN_ID]
      ,[PRC_ID]
      ,[PRC_NAME]
      ,[sk_Dim_Physcn]
      ,[UVaID]
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
      ,[appt_event_No_Show]
      ,[appt_event_Canceled_Late]
      ,[appt_event_Canceled]
      ,[appt_event_Scheduled]
      ,[appt_event_Provider_Canceled]
      ,[appt_event_Completed]
      ,[appt_event_Arrived]
      ,[appt_event_New_to_Specialty]
      ,[Appointment_Lag_Days]
      ,[CYCLE_TIME_MINUTES_Adjusted]
      ,[Load_Dtm]
      ,[DEPT_SPECIALTY_NAME]
      ,[PROV_SPECIALTY_NAME]
      ,[APPT_DTTM]
      ,[ENC_TYPE_C]
      ,[ENC_TYPE_TITLE]
      ,[APPT_CONF_STAT_NAME]
      ,[ZIP]
      ,[APPT_CONF_DTTM]
      ,[SIGNIN_DTTM]
      ,[ARVL_LIST_REMOVE_DTTM]
      ,[ROOMED_DTTM]
      ,[NURSE_LEAVE_DTTM]
      ,[PHYS_ENTER_DTTM]
      ,[CANCEL_REASON_NAME]
      ,[financial_division]
      ,[financial_subdivision]
      ,[CANCEL_INITIATOR]
      ,[F2_Flag]
      ,[TIME_TO_ROOM_MINUTES]
      ,[TIME_IN_ROOM_MINUTES]
      ,[BEGIN_CHECKIN_DTTM]
      ,[PAGED_DTTM]
      ,[FIRST_ROOM_ASSIGN_DTTM]
      ,[CANCEL_LEAD_HOURS]
      ,[APPT_CANC_DTTM]
      ,[Entry_UVaID]
      ,[Canc_UVaID]
	  ,[Cancel_Lead_Days]
  /* No Show Rate */
--
-- No Show Rate
--				(SUM(appt_event_No_Show = 1) + SUM(appt_event_Canceled_Late = 1))
--              /
--              (SUM(appt_event_Canceled = 0) + SUM(appt_event_Canceled_Late = 1) + SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--
	  --,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  --,CASE WHEN appt_event_No_Show = 1 OR appt_event_Canceled_Late = 1 THEN 1 ELSE 0 END AS [No Show]
  /* Bump Rate */
--
-- Bump Rate
--				SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--              /
--              (SUM(appt_event_Canceled = 0) + SUM(appt_event_Canceled_Late = 1) + SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--
	  --,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  --,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  --,CASE WHEN appt_event_Provider_Canceled = 1 THEN 277
	  --,CASE WHEN appt_event_Provider_Canceled = 1 AND DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT) <= 45 THEN 1 WHEN appt_event_Provider_Canceled = 1 AND DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT) > 45 THEN 0 ELSE NULL END AS [Cancel_Lead_Days_Less_Than_46]
  /* Completed Count */
	  ,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN appt_event_Completed = 1 THEN 1 ELSE 0  END AS [Completed]
  /* New Patient Lag Days */
   --   ,CASE
   --      WHEN ENTRY_DATE IS NULL THEN  APPT_MADE_DATE
		 --WHEN ENTRY_DATE >= APPT_MADE_DATE AND CHANGE_DATE >= APPT_MADE_DATE THEN APPT_MADE_DATE
		 --WHEN ENTRY_DATE < CHANGE_DATE THEN ENTRY_DATE
   --      ELSE CHANGE_DATE
   --    END AS Appointment_Lag_Start_Date
	  --,APPT_DT AS Appointment_Lag_End_Date
	  --,CASE WHEN appt_event_Completed = 1 THEN 1 ELSE 0 END AS [Completed]
	  --,CASE WHEN appt_event_New_to_Specialty = 1 THEN 1 ELSE 0 END AS [New_Patient]
  /* New Patient Visits Percentage */
   --   ,CASE WHEN appt_event_Completed = 1 THEN 1 ELSE 0 END AS [Completed]
	  --,CASE WHEN appt_event_Arrived = 1 THEN 1 ELSE 0 END AS [Arrived]
	  --,CASE WHEN appt_event_New_to_Specialty = 1 THEN 1 ELSE 0 END AS [New_Patient]
  /* Visit Time Minutes Average */
   --   ,CASE WHEN appt_event_Completed = 1 THEN 1 ELSE 0 END AS [Completed]
	  --,CASE WHEN appt_event_Arrived = 1 THEN 1 ELSE 0 END AS [Arrived]
  FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  WHERE
  /* No Show Rate */
  /*
  ((event_count = 1)
   AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @startdate AND @enddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* Bump Rate */
  --((event_count = 1)
  --AND ((appt_event_Canceled = 0) OR ((appt_event_Canceled_Late = 1) OR ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))))
  --AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* Completed Count */
  (event_count = 1)
   AND ((appt_event_Canceled = 0)  OR ((appt_event_Canceled_Late = 1) OR ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))))
  AND event_date BETWEEN @startdate AND @enddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* New Patient Lag Days */
  --(event_count = 1)
  --AND (appt_event_Completed = 1 AND Appointment_Lag_Days >= 0)
  --AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* New Patient Visits Percentage */
  --(event_count = 1)
  --AND ((appt_event_Completed = 1) OR (appt_event_Arrived = 1))
  --AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* Visit Time Minutes Average */
  --(event_count = 1)
  --AND ((appt_event_Completed = 1) OR (appt_event_Arrived = 1))
  --AND ((CYCLE_TIME_MINUTES_Adjusted IS NOT NULL) AND (CYCLE_TIME_MINUTES_Adjusted >= 0))
  --AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)

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
  ORDER BY APPT_STATUS_FLAG
         , event_date

SELECT
  /* No Show Rate */
	  -- SUM(CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointment]
	  --,SUM(CASE WHEN appt_event_No_Show = 1 OR appt_event_Canceled_Late = 1 THEN 1 ELSE 0 END) AS [No Show]
  /* Bump Rate */
	  -- SUM(CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointment]
	  --,SUM(CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END) AS [Bump]
  /* Completed Count */
	  --SUM(CASE WHEN appt_event_Canceled = 0 THEN 1 ELSE 0  END) AS [Appointments]
	  COUNT(*) AS [Appointments]
	 ,SUM(CASE WHEN appt_event_Completed = 1 THEN 1 ELSE 0  END) AS [Completed]
  /* New Patient Lag Days */
  --    SUM(CASE WHEN (appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1) THEN 1 ELSE 0 END) AS [Completed]
	 --,SUM(CASE WHEN (appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Days >= 0) THEN Appointment_Lag_Days ELSE 0 END) AS [Appointment_Lag_Days]
  /* New Patient Visits Percentage */
  --    SUM(CASE WHEN appt_event_Completed = 1 THEN 1 ELSE 0 END) AS [Completed]
	 --,SUM(CASE WHEN appt_event_Arrived = 1 THEN 1 ELSE 0 END) AS [Arrived]
	 --,SUM(CASE WHEN appt_event_New_to_Specialty = 1 THEN 1 ELSE 0 END) AS [New_Patient]
  /* Visit Time Minutes Average */
 --    SUM(CASE WHEN appt_event_Completed = 1 THEN 1 ELSE 0 END) AS [Completed]
	--,SUM(CASE WHEN appt_event_Arrived = 1 THEN 1 ELSE 0 END) AS [Arrived]
	--,SUM(CASE WHEN ((appt_event_Completed = 1) AND (CYCLE_TIME_MINUTES_Adjusted >= 0)) THEN CYCLE_TIME_MINUTES_Adjusted ELSE 0 END) AS [CYCLE_TIME_MINUTES_Adjusted]
  FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  WHERE
  /* No Show Rate */
  /*
  ((event_count = 1)
   AND (appt_event_Canceled = 0  OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @startdate AND @enddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* Bump Rate */
  --((event_count = 1)
  -- AND ((appt_event_Canceled = 0) OR ((appt_event_Canceled_Late = 1) OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))))
  --AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* Completed Count */
  (event_count = 1)
  AND ((appt_event_Canceled = 0)  OR ((appt_event_Canceled_Late = 1) OR ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))))
  AND event_date BETWEEN @startdate AND @enddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* New Patient Lag Days */
  --(event_count = 1)
  --AND (appt_event_Completed = 1 AND Appointment_Lag_Days >= 0)
  --AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* New Patient Visits Percentage */
  --(event_count = 1) AND ((appt_event_Completed = 1) OR (appt_event_Arrived = 1))
  --AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* Visit Time Minutes Average */
  --(event_count = 1)
  --AND ((appt_event_Completed = 1) OR (appt_event_Arrived = 1))
  --AND ((CYCLE_TIME_MINUTES_Adjusted IS NOT NULL) AND (CYCLE_TIME_MINUTES_Adjusted >= 0))
  --AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
