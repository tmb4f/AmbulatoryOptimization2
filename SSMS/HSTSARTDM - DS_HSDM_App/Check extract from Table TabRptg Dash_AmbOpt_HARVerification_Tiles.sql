USE DS_HSDM_App

DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate SMALLDATETIME = NULL

--SET @startdate = '7/20/2018 00:00 AM'
--SET @enddate = '7/20/2018 11:59 PM'
--SET @startdate = '6/5/2018 00:00 AM'
--SET @enddate = '6/5/2018 11:59 PM'
--SET @startdate = '6/4/2018 00:00 AM'
--SET @enddate = '6/6/2018 11:59 PM'
--SET @startdate = '7/1/2018 00:00 AM'
SET @startdate = '2/1/2019 00:00 AM'
SET @enddate = '2/28/2019 11:59 PM'

DECLARE @locdt INTEGER = NULL

SET @locdt = 5 -- Default days between HAR_Verified_Date and event_date

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
('Medical Specialties')
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
 --(10243003) -- UVHE DIGESTIVE HEALTH
 (10239003) -- UVMS NEPHROLOGY
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
 ('73571') -- LEEDS, JOSEPH THOMAS
 ,('29303') -- KALANTARI, KAMBIZ
;

SELECT 
       [PAT_ENC_CSN_ID]
	  ,[sk_Dash_AmbOpt_HARVerification_Tiles]
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
      ,[provider_id]
      ,[provider_name]
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
      ,[fmonth_name]
      ,[HAR Verification]
      ,[Ins Verification]
      ,[E-Verified]
      ,[RTE Enabled]
      ,[RTE Override case Rate]
      --,[PAT_ENC_CSN_ID]
      ,[PAT_id]
      ,[MYCHART_STATUS_C]
      ,[MYCHART_STATUS_NAME]
      ,TabRptg.[APPT_STATUS_C]
	  ,apptst.Appt_Sts_Nme
      ,[APPT_CONF_STAT_C]
      ,[CANCEL_REASON_C]
      ,[APPT_CANCEL_DATE]
      ,[HAR_Verified_Date]
      ,[INS_Verified_Date]
      ,[prov_serviceline]
      ,[PAYOR_ID]
      ,[PAYOR_NAME]
      ,[PLAN_ID]
      ,[BENEFIT_PLAN_NAME]
      ,[ACCT_FIN_CLASS_C]
      ,[HSP_ACCOUNT_ID]
      ,[COVERAGE_ID]
      ,[Load_Dtm]
      ,[Appt_Entry_User]
      ,[HAR_Verif_User]
      ,[HAR_Verif_Status_C]
      ,[HAR_Verif_Status]
	  ,[HAR_Verification_DATEDIFF]
	  ,[APPT_MADE_DTTM]
  /* HAR Verification Rate */
	  ,1 AS Total_Encounters
	  --,[HAR Verification] AS HAR_Verified
      ,CAST(CASE
              WHEN [HAR_Verif_Status_C] IN (   '1'  --verified
                                        --  ,'2'  --New verification records --removed 9/17/2018 Mali
                                            ,'6'  --e-verified
                                            ,'8'  --E-verified- Additional coverage
                                            ,'12' --verifed by phone
                                            ,'13' --verified by website
                                         )
                   AND [HAR_Verification_DATEDIFF] >= @locdt
                       THEN 1
                       ELSE 0
            END AS INT) AS HAR_Verified
  FROM [TabRptg].[Dash_AmbOpt_HARVerification_Tiles] TabRptg
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_Appt_Sts apptst
  ON apptst.APPT_STATUS_C = TabRptg.APPT_STATUS_C
  WHERE
  /* HAR Verification Rate */
  1=1
  AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)

  --ORDER BY pod_id
  --       , epic_department_id
		-- , provider_id
  --       , event_date
  --ORDER BY w_service_line_id
  --       , epic_department_id
		-- , provider_id
  --       , event_date
  ORDER BY PAT_ENC_CSN_ID
         , w_service_line_id
         , epic_department_id
		 , provider_id
         , event_date
  --ORDER BY person_id
  --       , event_date
  --ORDER BY provider_name
  --       , PAT_ENC_CSN_ID

SELECT
  /* HAR Verification Rate */
	   COUNT(*) AS Total_Encounters
	  --,SUM([HAR Verification]) AS HAR_Verified
	  ,SUM(
	   CAST(CASE
              WHEN [HAR_Verif_Status_C] IN (   '1'  --verified
                                        --  ,'2'  --New verification records --removed 9/17/2018 Mali
                                            ,'6'  --e-verified
                                            ,'8'  --E-verified- Additional coverage
                                            ,'12' --verifed by phone
                                            ,'13' --verified by website
                                         )
                   AND [HAR_Verification_DATEDIFF] >= @locdt
                       THEN 1
                       ELSE 0
            END AS INT)
	      ) AS HAR_Verified
	  --,CAST(SUM([HAR Verification]) AS NUMERIC) / CAST(COUNT(*) AS NUMERIC) AS [HAR Verification Rate]
	  ,CAST(
	   SUM(
	   CAST(CASE
              WHEN [HAR_Verif_Status_C] IN (   '1'  --verified
                                        --  ,'2'  --New verification records --removed 9/17/2018 Mali
                                            ,'6'  --e-verified
                                            ,'8'  --E-verified- Additional coverage
                                            ,'12' --verifed by phone
                                            ,'13' --verified by website
                                         )
                   AND [HAR_Verification_DATEDIFF] >= @locdt
                       THEN 1
                       ELSE 0
            END AS INT)
	      )
	      AS NUMERIC) / CAST(COUNT(*) AS NUMERIC) AS [HAR Verification Rate]
  FROM [TabRptg].[Dash_AmbOpt_HARVerification_Tiles]
  WHERE
  /* HAR Verification Rate */
  1=1
  AND event_date BETWEEN @startdate AND @enddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
