USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME
       ,@enddate SMALLDATETIME

SET @startdate = NULL
SET @enddate = NULL
--SET @startdate = '2/1/2019 00:00 AM'
--SET @enddate = '3/4/2019 11:59 PM'
--SET @startdate = '7/1/2017 00:00 AM'
--SET @startdate = '1/1/2018 00:00 AM'
--SET @startdate = '7/1/2019 00:00 AM'
--SET @enddate = '6/30/2020 11:59 PM'

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Provider_Canceled_Appointment_Metric]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Provider_Canceled_Appointment_Metric
--WHO : Tom Burgan
--WHEN: 11/21/19
--WHY : Report provider canceled appointment metrics from Cadence.
-- 
--	Metric Calculations
--
-- Bump: Appointments canceled by provider within 45 days od the scheduled appointment date
--
--				CASE WHEN evnts.appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END
--
-- Appointment: Scheduled appointments that were not canceled, or were canceled late (i.e. PATIENT-canceled within 24 hours of appointment time, PROVIDER-canceled within 45 days of appointment date)
--
--				CASE WHEN COALESCE(evnts.appt_event_Canceled,0) = 0 OR evnts.appt_event_Canceled_Late = 1 OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END
--
-- Bump Rate
--				SUM(Bump) WHERE (event_category = 'Aggregate' AND (Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant'))
--              /
--              SUM(Appointment) WHERE (event_category = 'Aggregate' AND (Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant'))
--
-- Proposed backing view measures
--
-- Number of appointments that have been bumped more than once
--
--				WHERE (event_category = 'Detail' AND (Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant')), GROUP BY APPT_SERIAL_NUM, HAVING SUM(Bump) > 1
--
-- Percent of bumped appointments that were rescheduled over/under 14 days (calendar) of the original appointment date
--
--				SUM(event_count) WHERE (event_category = 'Detail' AND (Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant') AND Bump = 1 AND Rescheduled_Lag_Days <= 14/> 14)
--              /
--				SUM(event_count) WHERE (event_category = 'Detail' AND (Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant') AND Bump = 1)
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--				DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--				DS_HSDW_Prod.Rptg.vwDim_Patient
--				DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart
--				DS_HSDM_App.Stage.AmbOpt_Excluded_Department
--				DS_HSDW_Prod.Rptg.vwDim_Date
--				DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye
--				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--				DS_HSDW_Prod.Rptg.vwDim_Physcn
--				DS_HSDW_Prod.Rptg.vwRef_Service_Line
--				DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Provider_Canceled_Appointment_Metric]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         11/21/2019 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

IF OBJECT_ID('tempdb..#main ') IS NOT NULL
DROP TABLE #main

IF OBJECT_ID('tempdb..#metric ') IS NOT NULL
DROP TABLE #metric

SELECT evnts2.*
     , SUM(evnts2.Bump) OVER (PARTITION BY evnts2.APPT_SERIAL_NUM ORDER BY evnts2.APPT_SERIAL_NUM) AS ASN_Bumps
	 , DATEDIFF(dd, evnts2.APPT_DT, evnts2.Next_APPT_DT) AS Rescheduled_Lag_Days

	INTO #main

FROM
(
	SELECT evnts.*
		 , CASE WHEN evnts.appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS Bump
		 , CASE WHEN COALESCE(evnts.appt_event_Canceled,0) = 0 OR evnts.appt_event_Canceled_Late = 1 OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS Appointment
		 , ROW_NUMBER() OVER (PARTITION BY evnts.APPT_SERIAL_NUM ORDER BY evnts.APPT_MADE_DTTM) AS Seq -- sequence number for identifying and ordering linked appointments
		 , LEAD(evnts.APPT_DT) OVER (PARTITION BY evnts.APPT_SERIAL_NUM ORDER BY evnts.APPT_MADE_DTTM) AS Next_APPT_DT

	FROM
	(
		SELECT DISTINCT
			main.epic_pod AS pod_id,
			--main.mdmloc_pod AS pod_name,
			main.epic_hub AS hub_id,
			--main.mdmloc_hub AS hub_name,
			main.epic_department_id,
			--main.epic_department_name,
			--main.epic_department_name_external,
			main.peds,
			main.transplant,
			main.sk_Dim_Pt,
			main.sk_Fact_Pt_Acct,
			main.sk_Fact_Pt_Enc_Clrt,
			main.person_birth_date,
			main.person_gender,
			main.person_id,
			main.person_name,
			--main.practice_group_id,
			--main.practice_group_name,
			main.provider_id,
			main.provider_name,
			--main.service_line_id,
			--main.service_line,
			--main.prov_service_line_id,
			--main.prov_service_line,
			--main.sub_service_line_id,
			--main.sub_service_line,
			--main.opnl_service_id,
			--main.opnl_service_name,
			--main.corp_service_line_id,
			--main.corp_service_line,
			--main.hs_area_id,
			--main.hs_area_name,
			--main.prov_hs_area_id,
			--main.prov_hs_area_name,
			main.APPT_STATUS_FLAG,
			main.APPT_STATUS_C,
			main.CANCEL_INITIATOR,
			main.CANCEL_REASON_C,
			--main.MRN_int,
			--main.CONTACT_DATE,
			main.APPT_DT,
			main.PAT_ENC_CSN_ID,
			main.PRC_ID,
			main.PRC_NAME,
			main.sk_Dim_Physcn,
			--main.UVaID,
			main.VIS_NEW_TO_SYS_YN,
			main.VIS_NEW_TO_DEP_YN,
			main.VIS_NEW_TO_PROV_YN,
			main.VIS_NEW_TO_SPEC_YN,
			main.VIS_NEW_TO_SERV_AREA_YN,
			main.VIS_NEW_TO_LOC_YN,
			main.APPT_MADE_DATE,
			main.ENTRY_DATE,
			--main.CHECKIN_DTTM,
			--main.CHECKOUT_DTTM,
			--main.VISIT_END_DTTM,
			--main.CYCLE_TIME_MINUTES,
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
				WHEN main.ENTRY_DATE IS NULL THEN
					main.APPT_MADE_DATE
				WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
					main.APPT_MADE_DATE
				WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
					main.ENTRY_DATE
				ELSE
					main.CHANGE_DATE
			END AS Appointment_Request_Date,
			--CASE
			--    WHEN
			--    (
			--        (main.APPT_STATUS_FLAG IS NOT NULL)
			--        AND (main.CYCLE_TIME_MINUTES >= 960)
			--    ) THEN
			--        960 -- Operations has defined 960 minutes (16 hours) as the ceiling for the calculation to use for any times longer than 16 hours
			--    WHEN
			--    (
			--        (main.APPT_STATUS_FLAG IS NOT NULL)
			--        AND (main.CYCLE_TIME_MINUTES < 960)
			--    ) THEN
			--        main.CYCLE_TIME_MINUTES
			--    ELSE
			--        CAST(NULL AS INT)
			--END AS CYCLE_TIME_MINUTES_Adjusted,

			main.DEPT_SPECIALTY_NAME,
			main.PROV_SPECIALTY_NAME,
			main.APPT_DTTM,
			--   main.ENC_TYPE_C,
			--main.ENC_TYPE_TITLE,
			--main.APPT_CONF_STAT_NAME,
			--main.ZIP,
			--main.APPT_CONF_DTTM,
			--main.SIGNIN_DTTM,
			--main.ARVL_LIST_REMOVE_DTTM,
			--main.ROOMED_DTTM,
			--main.NURSE_LEAVE_DTTM,
			--main.PHYS_ENTER_DTTM,
			main.CANCEL_REASON_NAME,
			main.SER_RPT_GRP_SIX,
			main.SER_RPT_GRP_EIGHT,
			--main.F2F_Flag,
			--   main.TIME_TO_ROOM_MINUTES,
			--main.TIME_IN_ROOM_MINUTES,
			--main.BEGIN_CHECKIN_DTTM,
			--main.PAGED_DTTM,
			--main.FIRST_ROOM_ASSIGN_DTTM,
			main.CANCEL_LEAD_HOURS,
			main.APPT_CANC_DTTM,
			--main.Entry_UVaID,
			--main.Canc_UVaID,
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
			--main.BUSINESS_UNIT,
			main.Prov_Typ,
			main.Staff_Resource,
			--   main.som_group_id,
			--main.som_group_name,
			--main.rev_location_id,
			--main.rev_location,
			--main.financial_division_id,
			--main.financial_division_name,
			--main.financial_sub_division_id,
			--main.financial_sub_division_name,
			--main.som_department_id,
			--main.som_department_name,
			--main.som_division_id,
			--main.som_division_name,
			--main.som_hs_area_id,
			--main.som_hs_area_name,
			main.APPT_SERIAL_NUM,
			--main.RESCHED_APPT_CSN_ID,
			main.BILL_PROV_YN,
			main.APPT_ENTRY_USER_ID,
			main.APPT_CANC_USER_ID,
			main.Load_Dtm

		FROM
		( --main
			SELECT
					--appts.RPT_GRP_THIRTY AS epic_service_line,
			--         mdmloc.SERVICE_LINE AS mdmloc_service_line,
					appts.RPT_GRP_SIX AS epic_pod,
					--mdmloc.PFA_POD AS mdmloc_pod,
					appts.RPT_GRP_SEVEN AS epic_hub,
					--mdmloc.HUB AS mdmloc_hub,
					appts.DEPARTMENT_ID AS epic_department_id,
					--mdm.epic_department_name AS epic_department_name,
					--mdm.epic_department_name_external AS epic_department_name_external,
					CAST(CASE
							WHEN FLOOR((CAST(appts.APPT_DT AS INTEGER)
										- CAST(CAST(pat.BirthDate AS DATETIME) AS INTEGER)
										) / 365.25
										) < 18 THEN
								1
							ELSE
								0
						END AS SMALLINT) AS peds,
					CAST(CASE
							WHEN tx.pat_enc_csn_id IS NOT NULL THEN
								1
							ELSE
								0
						END AS SMALLINT) AS transplant,
					appts.sk_Dim_Pt,
					appts.sk_Fact_Pt_Acct,
					appts.sk_Fact_Pt_Enc_Clrt,
					pat.BirthDate AS person_birth_date,
					pat.Sex AS person_gender,
					CAST(appts.IDENTITY_ID AS INT) AS person_id,
					pat.Name AS person_name,
					--CAST(NULL AS INT) AS practice_group_id,
					--CAST(NULL AS VARCHAR(150)) AS practice_group_name,
					appts.PROV_ID AS provider_id,
					appts.PROV_NAME AS provider_name,
					-- MDM
					--mdm.service_line_id,
					--mdm.service_line,
					--physsvc.Service_Line_ID AS prov_service_line_id,
					--physsvc.Service_Line AS prov_service_line,
					--mdm.sub_service_line_id,
					--mdm.sub_service_line,
					--mdm.opnl_service_id,
					--mdm.opnl_service_name,
					--mdm.corp_service_line_id,
					--mdm.corp_service_line,
					--mdm.hs_area_id,
					--mdm.hs_area_name,
					--physsvc.hs_area_id AS prov_hs_area_id,
					--physsvc.hs_area_name AS prov_hs_area_name,
					--Select
					appts.APPT_STATUS_FLAG,
					appts.APPT_STATUS_C,		
					appts.CANCEL_INITIATOR,
		            appts.CANCEL_REASON_C,
					--CAST(appts.IDENTITY_ID AS INTEGER) AS MRN_int,
		--            appts.CONTACT_DATE,
					appts.APPT_DT,
					appts.PAT_ENC_CSN_ID,
					appts.PRC_ID,
					appts.PRC_NAME,
					ser.sk_Dim_Physcn,
					--doc.UVaID,
					COALESCE(appts.VIS_NEW_TO_SYS_YN,'N') AS VIS_NEW_TO_SYS_YN,
					COALESCE(appts.VIS_NEW_TO_DEP_YN,'N') AS VIS_NEW_TO_DEP_YN,
					COALESCE(appts.VIS_NEW_TO_PROV_YN,'N') AS VIS_NEW_TO_PROV_YN,
					COALESCE(appts.VIS_NEW_TO_SPEC_YN,'N') AS VIS_NEW_TO_SPEC_YN,
					COALESCE(appts.VIS_NEW_TO_SERV_AREA_YN,'N') AS VIS_NEW_TO_SERV_AREA_YN,
					COALESCE(appts.VIS_NEW_TO_LOC_YN,'N') AS VIS_NEW_TO_LOC_YN,
		            appts.APPT_MADE_DATE,
		            appts.ENTRY_DATE,
		--            appts.CHECKIN_DTTM,
		--            appts.CHECKOUT_DTTM,
		--            appts.VISIT_END_DTTM,
		--            appts.CYCLE_TIME_MINUTES,
					appts.DEPT_SPECIALTY_NAME,
					appts.PROV_SPECIALTY_NAME,
					appts.APPT_DTTM,
					--appts.ENC_TYPE_C,
					--appts.ENC_TYPE_TITLE,
					--appts.APPT_CONF_STAT_NAME,
					--appts.ZIP,
					--appts.APPT_CONF_DTTM,
					--appts.SIGNIN_DTTM,
					--appts.ARVL_LIST_REMOVE_DTTM,
					--appts.ROOMED_DTTM,
					--appts.NURSE_LEAVE_DTTM,
					--appts.PHYS_ENTER_DTTM,
					appts.CANCEL_REASON_NAME,
					appts.SER_RPT_GRP_SIX,
					appts.SER_RPT_GRP_EIGHT,
					--appts.F2F_Flag,
					--appts.TIME_TO_ROOM_MINUTES,
					--appts.TIME_IN_ROOM_MINUTES,
					--appts.BEGIN_CHECKIN_DTTM,
					--appts.PAGED_DTTM,
					--appts.FIRST_ROOM_ASSIGN_DTTM,
					appts.CANCEL_LEAD_HOURS,
					appts.APPT_CANC_DTTM,
					--entryemp.EMPlye_Systm_Login AS Entry_UVaID,
					--cancemp.EMPlye_Systm_Login AS Canc_UVaID,
					appts.PHONE_REM_STAT_NAME,
					appts.CHANGE_DATE,
					appts.APPT_MADE_DTTM,
					--mdmloc.BUSINESS_UNIT,
					ser.Prov_Typ,
					ser.Staff_Resource,
					--mdmloc.LOC_ID AS rev_location_id,
					--mdmloc.REV_LOC_NAME AS rev_location,				   
					-- SOM
					--physcn.Clrt_Financial_Division AS financial_division_id,
					--physcn.Clrt_Financial_Division_Name AS financial_division_name,
					--physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id,
					--physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name,
					--physcn.SOM_Group_ID AS som_group_id,
					--physcn.SOM_group AS som_group_name,
					--physcn.SOM_department_id AS som_department_id,
					--physcn.SOM_department AS	som_department_name,
					--physcn.SOM_division_5 AS	som_division_id,
					--physcn.SOM_division_name AS som_division_name,
					--physcn.som_hs_area_id AS	som_hs_area_id,
					--physcn.som_hs_area_name AS som_hs_area_name,
					appts.APPT_SERIAL_NUM,
					--appts.RESCHED_APPT_CSN_ID,
					appts.BILL_PROV_YN,
					appts.APPT_ENTRY_USER_ID,
					appts.APPT_CANC_USER_ID,
					appts.Load_Dtm

			FROM Stage.Scheduled_Appointment AS appts
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
					ON ser.PROV_ID = appts.PROV_ID
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
					ON pat.sk_Dim_Pt = appts.sk_Dim_Pt

				-- -------------------------------------
				-- Identify transplant encounter--
				-- -------------------------------------
				LEFT OUTER JOIN
				(
					SELECT DISTINCT
						btd.pat_enc_csn_id,
						btd.Event_Transplanted AS transplant_surgery_dt,
						btd.hosp_admsn_time AS Adm_Dtm
					FROM DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart AS btd
					WHERE (
								btd.TX_Episode_Phase = 'transplanted'
								AND btd.TX_Stat_Dt >= @locstartdate 
								AND btd.TX_Stat_Dt <  @locenddate
							)
							AND btd.TX_GroupedPhaseStatus = 'TX-ADMIT'
				) AS tx
					ON appts.PAT_ENC_CSN_ID = tx.pat_enc_csn_id

				-- -------------------------------------
				-- Excluded departments--
				-- -------------------------------------
				LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
					ON excl.DEPARTMENT_ID = appts.DEPARTMENT_ID

			WHERE (appts.APPT_DT >= @locstartdate
				AND appts.APPT_DT < @locenddate)
			AND excl.DEPARTMENT_ID IS NULL
			--AND appts.RPT_GRP_SIX = '14'
			--AND appts.DEPARTMENT_ID = 10243003
			--AND Prov_Typ IN ('Physician','Physician Assistant','Fellow','Nurse Practitioner')

		) AS main
	) AS evnts
) AS evnts2
ORDER BY
	APPT_DT,
	pod_id,
	hub_id,
	epic_department_id,
	Staff_Resource,
	Prov_Typ,
	provider_id,
	provider_name,
	BILL_PROV_YN,
	sk_Dim_Physcn,
	DEPT_SPECIALTY_NAME,
	PROV_SPECIALTY_NAME,
	SER_RPT_GRP_SIX,
	SER_RPT_GRP_EIGHT

  -- Create index for temp table #main

CREATE UNIQUE CLUSTERED INDEX IX_main ON #main (APPT_DT, pod_id, hub_id, epic_department_id, Staff_Resource, Prov_Typ, provider_id, BILL_PROV_YN, sk_Dim_Physcn, DEPT_SPECIALTY_NAME, PROV_SPECIALTY_NAME, SER_RPT_GRP_SIX, SER_RPT_GRP_EIGHT, PAT_ENC_CSN_ID)
CREATE NONCLUSTERED INDEX IX_NC ON #main (APPT_DT, epic_department_id, sk_Dim_Physcn)

--SELECT main.APPT_SERIAL_NUM,
--       Seq,
--       APPT_MADE_DTTM,
--       Bump,
--       Appointment,
--	   ASN_Bumps,
--       appt_event_Provider_Canceled,
--       Cancel_Lead_Days,
--       pod_id,
--       hub_id,
--       epic_department_id,
--       peds,
--       transplant,
--       provider_id,
--       APPT_STATUS_FLAG,
--       APPT_STATUS_C,
--       CANCEL_INITIATOR,
--       APPT_DT,
--       PAT_ENC_CSN_ID,
--       VIS_NEW_TO_SPEC_YN,
--       appt_event_No_Show,
--       appt_event_Canceled_Late,
--       appt_event_Canceled,
--       appt_event_Scheduled,
--       appt_event_Completed,
--       appt_event_Arrived,
--       appt_event_New_to_Specialty,
--       APPT_DTTM,
--       APPT_CANC_DTTM,
--       Prov_Typ,
--       Staff_Resource
--FROM #main main
--INNER JOIN
--(
--SELECT APPT_SERIAL_NUM
--FROM #main
--GROUP BY APPT_SERIAL_NUM
--HAVING SUM(Bump) > 0
--) bump
--ON main.APPT_SERIAL_NUM = bump.APPT_SERIAL_NUM
--ORDER BY main.APPT_SERIAL_NUM
--       , APPT_MADE_DTTM

--SELECT Prov_Typ,
--       APPT_DT,
--       APPT_SERIAL_NUM,
--       Seq,
--       APPT_MADE_DTTM,
--       Bump,
--       Appointment,
--	   ASN_Bumps,
--       appt_event_Provider_Canceled,
--       Cancel_Lead_Days,
--       pod_id,
--       hub_id,
--       epic_department_id,
--       peds,
--       transplant,
--       provider_id,
--       APPT_STATUS_FLAG,
--       APPT_STATUS_C,
--       CANCEL_INITIATOR,
--       PAT_ENC_CSN_ID,
--       VIS_NEW_TO_SPEC_YN,
--       appt_event_No_Show,
--       appt_event_Canceled_Late,
--       appt_event_Canceled,
--       appt_event_Scheduled,
--       appt_event_Completed,
--       appt_event_Arrived,
--       appt_event_New_to_Specialty,
--       APPT_DTTM,
--       APPT_CANC_DTTM,
--       Staff_Resource
--FROM #main
--ORDER BY Prov_Typ
--       , APPT_DT
--       , APPT_SERIAL_NUM
--       , APPT_MADE_DTTM

--SELECT rpt.APPT_DT,
--       rpt.pod_id,
--       rpt.hub_id,
--       rpt.epic_department_id,
--	   rpt.Staff_Resource,
--	   rpt.Prov_Typ,
--       rpt.provider_id,
--	   rpt.provider_name,
--	   rpt.BILL_PROV_YN,
--	   rpt.sk_Dim_Physcn,
--	   rpt.DEPT_SPECIALTY_NAME,
--	   rpt.PROV_SPECIALTY_NAME,
--       rpt.event_type,
--       rpt.event_count,
--	   rpt.event_date,
--       rpt.fmonth_num,
--       rpt.Fyear_num,
--       rpt.FYear_name,
--       rpt.report_period,
--       rpt.report_date,
--       rpt.peds,
--       rpt.transplant,
--       rpt.sk_Dim_Pt,
--       rpt.sk_Fact_Pt_Acct,
--       rpt.sk_Fact_Pt_Enc_Clrt,
--	   rpt.person_birth_date,
--	   rpt.person_gender,
--	   rpt.person_id,
--	   rpt.person_name,
--	   rpt.APPT_STATUS_FLAG,
--	   rpt.CANCEL_REASON_C,
--	   rpt.PAT_ENC_CSN_ID,
--	   rpt.PRC_ID,
--	   rpt.PRC_NAME,
--	   rpt.VIS_NEW_TO_SYS_YN,
--	   rpt.VIS_NEW_TO_DEP_YN,
--	   rpt.VIS_NEW_TO_PROV_YN,
--	   rpt.VIS_NEW_TO_SPEC_YN,
--	   rpt.VIS_NEW_TO_SERV_AREA_YN,
--	   rpt.VIS_NEW_TO_LOC_YN,
--       rpt.APPT_MADE_DATE,
--       rpt.ENTRY_DATE,
--       rpt.appt_event_No_Show,
--       rpt.appt_event_Canceled_Late,
--       rpt.appt_event_Canceled,
--       rpt.appt_event_Scheduled,
--       rpt.appt_event_Provider_Canceled,
--       rpt.appt_event_Completed,
--       rpt.appt_event_Arrived,
--       rpt.appt_event_New_to_Specialty,
--       rpt.Bump,
--       rpt.Appointment,
--	   rpt.Appointment_Lag_Days,
--	   rpt.APPT_DTTM,
--	   rpt.CANCEL_REASON_NAME,
--	   rpt.CANCEL_INITIATOR,
--	   rpt.CANCEL_LEAD_HOURS,
--	   rpt.APPT_CANC_DTTM,
--	   rpt.Entry_UVaID,
--	   rpt.Canc_UVaID,
--	   rpt.PHONE_REM_STAT_NAME,
--	   rpt.Cancel_Lead_Days,
--	   rpt.APPT_MADE_DTTM,
--	   rpt.APPT_SERIAL_NUM,
--	   rpt.Appointment_Request_Date,
--	   rpt.Load_Dtm,
--       mdm.epic_department_name AS epic_department_name,
--       mdm.epic_department_name_external AS epic_department_name_external,
--	   mdm.service_line_id,
--	   mdm.service_line,
--	   mdm.sub_service_line_id,
--	   mdm.sub_service_line,
--	   mdm.opnl_service_id,
--	   mdm.opnl_service_name,
--	   mdm.corp_service_line_id,
--	   mdm.corp_service_line,
--	   mdm.hs_area_id,
--	   mdm.hs_area_name,
--       mdmloc.PFA_POD AS pod_name,
--	   mdmloc.HUB AS hub_name,
--       physsvc.Service_Line_ID AS prov_service_line_id,
--       physsvc.Service_Line AS prov_service_line,
--       physsvc.hs_area_id AS prov_hs_area_id,
--       physsvc.hs_area_name AS prov_hs_area_name,
--	   doc.UVaID,
--	   mdmloc.LOC_ID AS rev_location_id,
--	   mdmloc.REV_LOC_NAME AS rev_location,				   
--    -- SOM
--	   physcn.Clrt_Financial_Division AS financial_division_id,
--	   physcn.Clrt_Financial_Division_Name AS financial_division_name,
--	   physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id,
--	   physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name,
--	   physcn.SOM_Group_ID AS som_group_id,
--	   physcn.SOM_group AS som_group_name,
--	   physcn.SOM_department_id AS som_department_id,
--	   physcn.SOM_department AS	som_department_name,
--	   physcn.SOM_division_5 AS	som_division_id,
--	   physcn.SOM_division_name AS som_division_name,
--	   physcn.som_hs_area_id AS	som_hs_area_id,
--	   physcn.som_hs_area_name AS som_hs_area_name

SELECT CAST('CanceledByProvider' AS VARCHAR(50)) AS event_type,
       rpt.event_count,
	   rpt.event_date,
       rpt.fmonth_num,
       rpt.Fyear_num,
       rpt.FYear_name,
       rpt.report_period,
       rpt.report_date,
	   rpt.event_category,
       rpt.pod_id,
       mdmloc.PFA_POD AS pod_name,
       rpt.hub_id,
	   mdmloc.HUB AS hub_name,
       rpt.epic_department_id,
       mdm.epic_department_name AS epic_department_name,
       mdm.epic_department_name_external AS epic_department_name_external,
       rpt.peds,
       rpt.transplant,
       rpt.sk_Dim_Pt,
       rpt.sk_Fact_Pt_Acct,
       rpt.sk_Fact_Pt_Enc_Clrt,
	   rpt.person_birth_date,
	   rpt.person_gender,
	   rpt.person_id,
	   rpt.person_name,
       CAST(NULL AS INT) AS practice_group_id,
       CAST(NULL AS VARCHAR(150)) AS practice_group_name,
       rpt.provider_id,
	   rpt.provider_name,
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
	   rpt.APPT_STATUS_FLAG,
	   rpt.CANCEL_REASON_C,
       rpt.APPT_DT,
	   rpt.Next_APPT_DT,
	   rpt.Rescheduled_Lag_Days,
	   rpt.PAT_ENC_CSN_ID,
	   rpt.PRC_ID,
	   rpt.PRC_NAME,
	   rpt.sk_Dim_Physcn,
	   doc.UVaID,
	   rpt.VIS_NEW_TO_SYS_YN,
	   rpt.VIS_NEW_TO_DEP_YN,
	   rpt.VIS_NEW_TO_PROV_YN,
	   rpt.VIS_NEW_TO_SPEC_YN,
	   rpt.VIS_NEW_TO_SERV_AREA_YN,
	   rpt.VIS_NEW_TO_LOC_YN,
       rpt.APPT_MADE_DATE,
       rpt.ENTRY_DATE,
       rpt.appt_event_No_Show,
       rpt.appt_event_Canceled_Late,
       rpt.appt_event_Canceled,
       rpt.appt_event_Scheduled,
       rpt.appt_event_Provider_Canceled,
       rpt.appt_event_Completed,
       rpt.appt_event_Arrived,
       rpt.appt_event_New_to_Specialty,
	   rpt.Appointment_Lag_Days,
	   rpt.DEPT_SPECIALTY_NAME,
	   rpt.PROV_SPECIALTY_NAME,
	   rpt.APPT_DTTM,
	   rpt.CANCEL_REASON_NAME,
	   rpt.SER_RPT_GRP_SIX AS financial_division,
	   rpt.SER_RPT_GRP_EIGHT AS financial_subdivision,
	   rpt.CANCEL_INITIATOR,
	   rpt.CANCEL_LEAD_HOURS,
	   rpt.APPT_CANC_DTTM,
	   rpt.Entry_UVaID,
	   rpt.Canc_UVaID,
	   rpt.PHONE_REM_STAT_NAME,
	   rpt.Cancel_Lead_Days,
	   rpt.APPT_MADE_DTTM,
	   rpt.Prov_Typ,
	   rpt.Staff_Resource,				   
    -- SOM
	   physcn.SOM_Group_ID AS som_group_id,
	   physcn.SOM_group AS som_group_name,
	   mdmloc.LOC_ID AS rev_location_id,
	   mdmloc.REV_LOC_NAME AS rev_location,
	   physcn.Clrt_Financial_Division AS financial_division_id,
	   physcn.Clrt_Financial_Division_Name AS financial_division_name,
	   physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id,
	   physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name,
	   physcn.SOM_department_id AS som_department_id,
	   physcn.SOM_department AS	som_department_name,
	   physcn.SOM_division_5 AS	som_division_id,
	   physcn.SOM_division_name AS som_division_name,
	   physcn.som_hs_area_id AS	som_hs_area_id,
	   physcn.som_hs_area_name AS som_hs_area_name,
	   rpt.APPT_SERIAL_NUM,
	   rpt.Appointment_Request_Date,
	   rpt.BILL_PROV_YN,
       rpt.Bump,
       rpt.Appointment--,
	   --rpt.Load_Dtm--,

INTO #metric

FROM
(
SELECT aggr.APPT_DT,
       aggr.pod_id,
       aggr.hub_id,
       aggr.epic_department_id,
	   aggr.Staff_Resource,
	   aggr.Prov_Typ,
       aggr.provider_id,
       aggr.provider_name,
	   aggr.BILL_PROV_YN,
       aggr.sk_Dim_Physcn,
	   aggr.DEPT_SPECIALTY_NAME,
	   aggr.PROV_SPECIALTY_NAME,
	   aggr.SER_RPT_GRP_SIX,
	   aggr.SER_RPT_GRP_EIGHT,
       CASE
           WHEN aggr.APPT_DT IS NOT NULL THEN
               1
           ELSE
               0
       END AS event_count,
       date_dim.day_date AS event_date,
       date_dim.fmonth_num,
       date_dim.Fyear_num,
       date_dim.FYear_name,
       CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period,
       CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date,
       CAST('Aggregate' AS VARCHAR(150)) AS event_category,
       ISNULL(aggr.peds,CAST(0 AS INT)) AS peds,
       ISNULL(aggr.transplant,CAST(0 AS INT)) AS transplant,
       CAST(NULL AS INT) AS sk_Dim_Pt,
       CAST(NULL AS INT) AS sk_Fact_Pt_Acct,
       CAST(NULL AS INT) AS sk_Fact_Pt_Enc_Clrt,
       CAST(NULL AS DATE) AS person_birth_date,
       CAST(NULL AS VARCHAR(254)) AS person_gender,
       CAST(NULL AS INT) AS person_id,
       CAST(NULL AS VARCHAR(200)) AS person_name,
       CAST(NULL AS VARCHAR(254)) AS APPT_STATUS_FLAG,
       CAST(NULL AS INT) AS CANCEL_REASON_C,
       CAST(NULL AS NUMERIC(18,0)) AS PAT_ENC_CSN_ID,
       CAST(NULL AS VARCHAR(18)) AS PRC_ID,
       CAST(NULL AS VARCHAR(200)) AS PRC_NAME,

       CAST(NULL AS INT) AS VIS_NEW_TO_SYS_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_DEP_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_PROV_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_SPEC_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_SERV_AREA_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_LOC_YN,

       CAST(NULL AS DATETIME) AS APPT_MADE_DATE,
       CAST(NULL AS DATETIME) AS ENTRY_DATE,

--For tableau calc purposes null = 0 per Sue.
       ISNULL(aggr.appt_event_No_Show,CAST(0 AS INT)) AS appt_event_No_Show,
       ISNULL(aggr.appt_event_Canceled_Late,CAST(0 AS INT)) AS appt_event_Canceled_Late,
       ISNULL(aggr.appt_event_Canceled,CAST(0 AS INT)) AS appt_event_Canceled,
       ISNULL(aggr.appt_event_Scheduled,CAST(0 AS INT)) AS appt_event_Scheduled,
       ISNULL(aggr.appt_event_Provider_Canceled,CAST(0 AS INT)) AS appt_event_Provider_Canceled,
       ISNULL(aggr.appt_event_Completed,CAST(0 AS INT)) AS appt_event_Completed,
       ISNULL(aggr.appt_event_Arrived,CAST(0 AS INT)) AS appt_event_Arrived,
       ISNULL(aggr.appt_event_New_to_Specialty,CAST(0 AS INT)) AS appt_event_New_to_Specialty,
	   	   
       ISNULL(aggr.Bump,CAST(0 AS INT)) AS Bump,
       ISNULL(aggr.Appointment,CAST(0 AS INT)) AS Appointment,

       CAST(NULL AS INT) AS Appointment_Lag_Days,
	   CAST(NULL AS DATETIME) AS APPT_DTTM,
       CAST(NULL AS VARCHAR(254)) AS CANCEL_REASON_NAME,
       CAST(NULL AS VARCHAR(55)) AS CANCEL_INITIATOR,
       CAST(NULL AS INT) AS CANCEL_LEAD_HOURS,
       CAST(NULL AS DATETIME) AS APPT_CANC_DTTM,
       CAST(NULL AS VARCHAR(254)) AS Entry_UVaID,
       CAST(NULL AS VARCHAR(254)) AS Canc_UVaID,
       CAST(NULL AS VARCHAR(254)) AS PHONE_REM_STAT_NAME,
       CAST(NULL AS INT) AS Cancel_Lead_Days,
       CAST(NULL AS DATETIME) AS APPT_MADE_DTTM,
       CAST(NULL AS NUMERIC(18,0)) AS APPT_SERIAL_NUM,
       CAST(NULL AS DATETIME) AS Appointment_Request_Date,
       CAST(NULL AS SMALLDATETIME) AS Load_Dtm,
       CAST(NULL AS DATETIME) AS Next_APPT_DT,
       CAST(NULL AS INT) AS Rescheduled_Lag_Days

--INTO #metric

FROM
(
    SELECT day_date,
           fmonth_num,
           Fyear_num,
           FYear_name
    FROM DS_HSDW_Prod.Rptg.vwDim_Date

) date_dim
    LEFT OUTER JOIN
    (
		SELECT
			   evnts.APPT_DT,
			   evnts.pod_id,
			   evnts.hub_id,
			   evnts.epic_department_id,
			   evnts.Staff_Resource,
			   evnts.Prov_Typ,
			   evnts.provider_id,
			   evnts.provider_name,
			   evnts.BILL_PROV_YN,
			   evnts.sk_Dim_Physcn,
			   evnts.DEPT_SPECIALTY_NAME,
			   evnts.PROV_SPECIALTY_NAME,
			   evnts.SER_RPT_GRP_SIX,
			   evnts.SER_RPT_GRP_EIGHT,
			   SUM(evnts.peds) AS peds,
			   SUM(evnts.transplant) AS transplant,
			   SUM(evnts.appt_event_No_Show) AS appt_event_No_Show,
			   SUM(evnts.appt_event_Canceled_Late) AS appt_event_Canceled_Late,
			   SUM(evnts.appt_event_Canceled) AS appt_event_Canceled,
			   SUM(evnts.appt_event_Scheduled) AS appt_event_Scheduled,
			   SUM(evnts.appt_event_Provider_Canceled) AS appt_event_Provider_Canceled,
			   SUM(evnts.appt_event_Completed) AS appt_event_Completed,
			   SUM(evnts.appt_event_Arrived) AS appt_event_Arrived,
			   SUM(evnts.appt_event_New_to_Specialty) AS appt_event_New_to_Specialty,
			   SUM(evnts.Bump) AS Bump,
			   SUM(evnts.Appointment) AS Appointment

		FROM #main evnts
		GROUP BY
			evnts.APPT_DT,
			evnts.pod_id,
			evnts.hub_id,
			evnts.epic_department_id,
			evnts.Staff_Resource,
			evnts.Prov_Typ,
			evnts.provider_id,
			evnts.provider_name,
			evnts.BILL_PROV_YN,
			evnts.sk_Dim_Physcn,
			evnts.DEPT_SPECIALTY_NAME,
			evnts.PROV_SPECIALTY_NAME,
			evnts.SER_RPT_GRP_SIX,
			evnts.SER_RPT_GRP_EIGHT
	) aggr
		
        ON (date_dim.day_date = CAST(aggr.APPT_DT AS SMALLDATETIME))

WHERE date_dim.day_date >= @locstartdate
      AND date_dim.day_date < @locenddate
UNION ALL
SELECT main.APPT_DT,
       main.pod_id,
       main.hub_id,
       main.epic_department_id,
	   main.Staff_Resource,
	   main.Prov_Typ,
       main.provider_id,
       main.provider_name,
	   main.BILL_PROV_YN,
       main.sk_Dim_Physcn,
	   main.DEPT_SPECIALTY_NAME,
	   main.PROV_SPECIALTY_NAME,
	   main.SER_RPT_GRP_SIX,
	   main.SER_RPT_GRP_EIGHT,
	   main.appt_event_Provider_Canceled AS event_count,
       date_dim.day_date AS event_date,
       date_dim.fmonth_num,
       date_dim.Fyear_num,
       date_dim.FYear_name,
       CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period,
       CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date,
       CAST('Detail' AS VARCHAR(150)) AS event_category,
       main.peds,
       main.transplant,
       main.sk_Dim_Pt,
       main.sk_Fact_Pt_Acct,
       main.sk_Fact_Pt_Enc_Clrt,
       main.person_birth_date,
       main.person_gender,
       main.person_id,
       main.person_name,
       main.APPT_STATUS_FLAG,
       main.CANCEL_REASON_C,
       main.PAT_ENC_CSN_ID,
       main.PRC_ID,
       main.PRC_NAME,

---BDD 5/9/2018 per Sue, change these from Y/N varchar(1) to 1/0 ints. Null = 0 per Tom and Sue
       CASE WHEN main.VIS_NEW_TO_SYS_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SYS_YN,
       CASE WHEN main.VIS_NEW_TO_DEP_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_DEP_YN,
       CASE WHEN main.VIS_NEW_TO_PROV_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_PROV_YN,
       CASE WHEN main.VIS_NEW_TO_SPEC_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SPEC_YN,
       CASE WHEN main.VIS_NEW_TO_SERV_AREA_YN = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SERV_AREA_YN,
       CASE WHEN main.VIS_NEW_TO_LOC_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_LOC_YN,

       main.APPT_MADE_DATE,
       main.ENTRY_DATE,

	   main.appt_event_No_Show,
       main.appt_event_Canceled_Late,
       main.appt_event_Canceled,
       main.appt_event_Scheduled,
       main.appt_event_Provider_Canceled,
       main.appt_event_Completed,
       main.appt_event_Arrived,
       main.appt_event_New_to_Specialty,
	   
       main.Bump,
	   main.Appointment,
	   
       CASE
           WHEN (main.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd, main.Appointment_Request_Date, main.APPT_DT)
           ELSE CAST(NULL AS INT)
       END AS Appointment_Lag_Days,
       main.APPT_DTTM,
	   main.CANCEL_REASON_NAME,
	   main.CANCEL_INITIATOR,
	   main.CANCEL_LEAD_HOURS,
	   main.APPT_CANC_DTTM,
	   entryemp.EMPlye_Systm_Login AS Entry_UVaID,
	   cancemp.EMPlye_Systm_Login AS Canc_UVaID,
	   main.PHONE_REM_STAT_NAME,
	   main.Cancel_Lead_Days,
	   main.APPT_MADE_DTTM,
	   main.APPT_SERIAL_NUM,
	   main.Appointment_Request_Date,
	   main.Load_Dtm,
	   main.Next_APPT_DT,
	   main.Rescheduled_Lag_Days
FROM #main main
INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date date_dim
ON main.APPT_DT = date_dim.day_date
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye entryemp
	ON entryemp.EMPlye_Usr_ID = main.APPT_ENTRY_USER_ID
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye cancemp
	ON cancemp.EMPlye_Usr_ID = main.APPT_CANC_USER_ID
WHERE main.ASN_Bumps > 0
) rpt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
ON mdm.epic_department_id = rpt.epic_department_id
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
ON mdmloc.EPIC_DEPARTMENT_ID = rpt.epic_department_id
LEFT JOIN
(
    SELECT sk_Dim_Physcn,
            UVaID,
            Service_Line
    FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
    WHERE current_flag = 1
) AS doc
    ON doc.sk_Dim_Physcn = rpt.sk_Dim_Physcn
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc
    ON physsvc.Physician_Roster_Name = CASE
                                            WHEN (rpt.sk_Dim_Physcn > 0) THEN
                                                doc.Service_Line
                                            ELSE
                                                'No Value Specified'
                                        END
-- -------------------------------------
-- SOM Hierarchy--
-- -------------------------------------
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
	ON physcn.sk_Dim_Physcn = doc.sk_Dim_Physcn
--ORDER BY date_dim.day_date;
--ORDER BY rpt.event_category, rpt.event_date, rpt.pod_id, rpt.hub_id, rpt.epic_department_id, rpt.provider_id;

--SELECT *
----SELECT event_type,
----       APPT_SERIAL_NUM,
----       APPT_MADE_DATE,
----	   APPT_MADE_DTTM,
----       Bump,
----       Appointment,
----       appt_event_Provider_Canceled,
----       Cancel_Lead_Days,
----       APPT_DT,
----       pod_id,
----       hub_id,
----       epic_department_id,
----       Staff_Resource,
----       Prov_Typ,
----       provider_id,
----       provider_name,
----       BILL_PROV_YN,
----       sk_Dim_Physcn,
----       event_count,
----       event_date,
----       fmonth_num,
----       Fyear_num,
----       FYear_name,
----       report_period,
----       report_date,
----       peds,
----       transplant,
----       --person_birth_date,
----       --person_gender,
----       --person_id,
----       --person_name,
----       APPT_STATUS_FLAG,
----       CANCEL_REASON_C,
----       --PAT_ENC_CSN_ID,
----       --PRC_ID,
----       --PRC_NAME,
----       --VIS_NEW_TO_SYS_YN,
----       --VIS_NEW_TO_DEP_YN,
----       --VIS_NEW_TO_PROV_YN,
----       VIS_NEW_TO_SPEC_YN,
----       --VIS_NEW_TO_SERV_AREA_YN,
----       --VIS_NEW_TO_LOC_YN,
----       --ENTRY_DATE,
----       appt_event_No_Show,
----       appt_event_Canceled_Late,
----       appt_event_Canceled,
----       appt_event_Scheduled,
----       appt_event_Completed,
----       appt_event_Arrived,
----       appt_event_New_to_Specialty,
----       Appointment_Lag_Days,
----       APPT_DTTM,
----       CANCEL_REASON_NAME,
----       CANCEL_INITIATOR,
----       CANCEL_LEAD_HOURS,
----       APPT_CANC_DTTM,
----       --Entry_UVaID,
----       --Canc_UVaID,
----       --PHONE_REM_STAT_NAME,
----       Load_Dtm,
----       epic_department_name,
----       --epic_department_name_external,
----       service_line_id,
----       service_line,
----       --sub_service_line_id,
----       --sub_service_line,
----       --opnl_service_id,
----       --opnl_service_name,
----       --corp_service_line_id,
----       --corp_service_line,
----       --hs_area_id,
----       --hs_area_name,
----       pod_name,
----       hub_name--,
----       --prov_service_line_id,
----       --prov_service_line,
----       --prov_hs_area_id,
----       --prov_hs_area_name,
----       --UVaID,
----       --rev_location_id,
----       --rev_location,
----       --financial_division_id,
----       --financial_division_name,
----       --financial_sub_division_id,
----       --financial_sub_division_name,
----       --som_group_id,
----       --som_group_name,
----       --som_department_id,
----       --som_department_name,
----       --som_division_id,
----       --som_division_name,
----       --som_hs_area_id,
----       --som_hs_area_name
--FROM #metric
----WHERE event_type = 'CanceledByProvider'
----ORDER BY event_type
----        ,event_date;
----ORDER BY event_type
----        ,Prov_Typ
----		,epic_department_id
----        ,APPT_DT
----        ,APPT_SERIAL_NUM;
--ORDER BY event_type
--        ,APPT_SERIAL_NUM
--		,APPT_MADE_DTTM

--SELECT APPT_DT,
--       pod_id,
--       hub_id,
--       epic_department_id,
--	   Staff_Resource,
--	   Prov_Typ,
--       provider_id,
--	   provider_name,
--	   BILL_PROV_YN,
--	   sk_Dim_Physcn,
--	   DEPT_SPECIALTY_NAME,
--	   PROV_SPECIALTY_NAME,
--	   financial_division,
--	   financial_subdivision,
--       Bump,
--       Appointment,
--       --event_type,
--       event_count,
--       event_date,
--       fmonth_num,
--       Fyear_num,
--       FYear_name,
--       report_period,
--       report_date,
--       event_category,
--       --pod_id,
--       pod_name,
--       --hub_id,
--       hub_name,
--       --epic_department_id,
--       epic_department_name,
--       epic_department_name_external,
--       peds,
--       transplant,
--       --sk_Dim_Pt,
--       --sk_Fact_Pt_Acct,
--       --sk_Fact_Pt_Enc_Clrt,
--       --person_birth_date,
--       --person_gender,
--       --person_id,
--       --person_name,
--       practice_group_id,
--       practice_group_name,
--       --provider_id,
--       --provider_name,
--       service_line_id,
--       service_line,
--       prov_service_line_id,
--       prov_service_line,
--       sub_service_line_id,
--       sub_service_line,
--       opnl_service_id,
--       opnl_service_name,
--       corp_service_line_id,
--       corp_service_line,
--       hs_area_id,
--       hs_area_name,
--       prov_hs_area_id,
--       prov_hs_area_name,
--       --APPT_STATUS_FLAG,
--       --CANCEL_REASON_C,
--       --APPT_DT,
--       --PAT_ENC_CSN_ID,
--       --PRC_ID,
--       --PRC_NAME,
--       --sk_Dim_Physcn,
--       --UVaID,
--       --VIS_NEW_TO_SYS_YN,
--       --VIS_NEW_TO_DEP_YN,
--       --VIS_NEW_TO_PROV_YN,
--       --VIS_NEW_TO_SPEC_YN,
--       --VIS_NEW_TO_SERV_AREA_YN,
--       --VIS_NEW_TO_LOC_YN,
--       --APPT_MADE_DATE,
--       --ENTRY_DATE,
--       appt_event_No_Show,
--       appt_event_Canceled_Late,
--       appt_event_Canceled,
--       appt_event_Scheduled,
--       appt_event_Provider_Canceled,
--       appt_event_Completed,
--       appt_event_Arrived,
--       appt_event_New_to_Specialty,
--       --Appointment_Lag_Days,
--       --DEPT_SPECIALTY_NAME,
--       --PROV_SPECIALTY_NAME,
--       --APPT_DTTM,
--       --CANCEL_REASON_NAME,
--       --financial_division,
--       --financial_subdivision,
--       --CANCEL_INITIATOR,
--       --CANCEL_LEAD_HOURS,
--       --APPT_CANC_DTTM,
--       --Entry_UVaID,
--       --Canc_UVaID,
--       --PHONE_REM_STAT_NAME,
--       --Cancel_Lead_Days,
--       --APPT_MADE_DTTM,
--       --Prov_Typ,
--       --Staff_Resource,
--       som_group_id,
--       som_group_name,
--       rev_location_id,
--       rev_location,
--       financial_division_id,
--       financial_division_name,
--       financial_sub_division_id,
--       financial_sub_division_name,
--       som_department_id,
--       som_department_name,
--       som_division_id,
--       som_division_name,
--       som_hs_area_id,
--       som_hs_area_name--,
--       --APPT_SERIAL_NUM,
--       --Appointment_Request_Date,
--       --BILL_PROV_YN--,
--       --Bump,
--       --Appointment

--SELECT pod_id,
--       hub_id,
--	   epic_department_id,
--	   Staff_Resource,
--	   Prov_Typ,
--       Fyear_num,
--       fmonth_num,
--       APPT_DT,
--       Appointment,
--       Bump,
--       provider_id,
--	   provider_name,
--	   BILL_PROV_YN,
--	   sk_Dim_Physcn,
--	   DEPT_SPECIALTY_NAME,
--	   PROV_SPECIALTY_NAME,
--	   financial_division,
--	   financial_subdivision,
--       --event_type,
--       event_count,
--       event_date,
--       FYear_name,
--       report_period,
--       report_date,
--       event_category,
--       --pod_id,
--       pod_name,
--       --hub_id,
--       hub_name,
--       --epic_department_id,
--       epic_department_name,
--       epic_department_name_external,
--       peds,
--       transplant,
--       --sk_Dim_Pt,
--       --sk_Fact_Pt_Acct,
--       --sk_Fact_Pt_Enc_Clrt,
--       --person_birth_date,
--       --person_gender,
--       --person_id,
--       --person_name,
--       practice_group_id,
--       practice_group_name,
--       --provider_id,
--       --provider_name,
--       service_line_id,
--       service_line,
--       prov_service_line_id,
--       prov_service_line,
--       sub_service_line_id,
--       sub_service_line,
--       opnl_service_id,
--       opnl_service_name,
--       corp_service_line_id,
--       corp_service_line,
--       hs_area_id,
--       hs_area_name,
--       prov_hs_area_id,
--       prov_hs_area_name,
--       --APPT_STATUS_FLAG,
--       --CANCEL_REASON_C,
--       --APPT_DT,
--       --PAT_ENC_CSN_ID,
--       --PRC_ID,
--       --PRC_NAME,
--       --sk_Dim_Physcn,
--       --UVaID,
--       --VIS_NEW_TO_SYS_YN,
--       --VIS_NEW_TO_DEP_YN,
--       --VIS_NEW_TO_PROV_YN,
--       --VIS_NEW_TO_SPEC_YN,
--       --VIS_NEW_TO_SERV_AREA_YN,
--       --VIS_NEW_TO_LOC_YN,
--       --APPT_MADE_DATE,
--       --ENTRY_DATE,
--       appt_event_No_Show,
--       appt_event_Canceled_Late,
--       appt_event_Canceled,
--       appt_event_Scheduled,
--       appt_event_Provider_Canceled,
--       appt_event_Completed,
--       appt_event_Arrived,
--       appt_event_New_to_Specialty,
--       --Appointment_Lag_Days,
--       --DEPT_SPECIALTY_NAME,
--       --PROV_SPECIALTY_NAME,
--       --APPT_DTTM,
--       --CANCEL_REASON_NAME,
--       --financial_division,
--       --financial_subdivision,
--       --CANCEL_INITIATOR,
--       --CANCEL_LEAD_HOURS,
--       --APPT_CANC_DTTM,
--       --Entry_UVaID,
--       --Canc_UVaID,
--       --PHONE_REM_STAT_NAME,
--       --Cancel_Lead_Days,
--       --APPT_MADE_DTTM,
--       --Prov_Typ,
--       --Staff_Resource,
--       som_group_id,
--       som_group_name,
--       rev_location_id,
--       rev_location,
--       financial_division_id,
--       financial_division_name,
--       financial_sub_division_id,
--       financial_sub_division_name,
--       som_department_id,
--       som_department_name,
--       som_division_id,
--       som_division_name,
--       som_hs_area_id,
--       som_hs_area_name--,
--       --APPT_SERIAL_NUM,
--       --Appointment_Request_Date,
--       --BILL_PROV_YN--,
--       --Bump,
--       --Appointment
--FROM #metric
--WHERE event_category = 'Aggregate'
----ORDER BY event_type
----        ,event_date;
----ORDER BY event_type
----        ,Prov_Typ
----		,epic_department_id
----        ,APPT_DT
----        ,APPT_SERIAL_NUM;
----ORDER BY event_category
----        ,event_date
----        ,APPT_DTTM
----ORDER BY
----	APPT_DT,
----	pod_id,
----	hub_id,
----	epic_department_id,
----	Staff_Resource,
----	Prov_Typ,
----	provider_id,
----	provider_name,
----	BILL_PROV_YN,
----	sk_Dim_Physcn,
----	DEPT_SPECIALTY_NAME,
----	PROV_SPECIALTY_NAME,
----	financial_division,
----	financial_subdivision
----ORDER BY
----	pod_id,
----	hub_id,
----	epic_department_id,
----	Staff_Resource,
----	Prov_Typ,
----	Fyear_num,
----	fmonth_num,
----	APPT_DT,
----	provider_id,
----	provider_name,
----	BILL_PROV_YN,
----	sk_Dim_Physcn,
----	DEPT_SPECIALTY_NAME,
----	PROV_SPECIALTY_NAME,
----	financial_division,
----	financial_subdivision
--ORDER BY
--	pod_id,
--	hub_id,
--	epic_department_id,
--	Fyear_num,
--	fmonth_num,
--	APPT_DT,
--	Staff_Resource,
--	Prov_Typ,
--	provider_id,
--	provider_name,
--	BILL_PROV_YN,
--	sk_Dim_Physcn,
--	DEPT_SPECIALTY_NAME,
--	PROV_SPECIALTY_NAME,
--	financial_division,
--	financial_subdivision

SELECT event_category
      ,APPT_SERIAL_NUM
	  ,APPT_MADE_DTTM
	  ,event_count
	  ,Bump
	  ,Appointment
	  ,APPT_DT
	  ,Next_APPT_DT
	  ,Rescheduled_Lag_Days
	  ,APPT_STATUS_FLAG
	  ,CANCEL_INITIATOR
	  ,Cancel_Lead_Days
	  ,appt_event_Provider_Canceled
FROM #metric
WHERE event_category = 'Detail'
ORDER BY event_category
        ,APPT_SERIAL_NUM
		,APPT_MADE_DTTM

GO


