USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_New_Patient_Appointment_Metric]
    (
     @startdate SMALLDATETIME = NULL
    ,@enddate SMALLDATETIME = NULL
    )
AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_New_Patient_Appointment_Metric
--WHO : Tom Burgan
--WHEN: 8/1/19
--WHY : Report new patient scheduled appointment metrics from Cadence.
-- 
--	Metric Calculations
--
--		Note: "SUM" can be interpreted as "SUM(event_count) WHERE ...."
--
-- Percentage of new patients that had access to a scheduled appointment in specialty within 7 business days
--
--				SUM(AbleToAccess = 1)
--              /
--              SUM(event_count = 1)
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDW_Prod.Rptg.vwDim_Date
--              DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--				DS_HSDW_Prod.Rptg.vwDim_Patient
--				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--				DS_HSDW_Prod.Rptg.vwDim_Physcn
--				DS_HSDW_Prod.Rptg.vwRef_Service_Line
--				DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye
--				DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye
--				DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart
--				DS_HSDM_App.Stage.AmbOpt_Excluded_Department
--				DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_New_Patient_Appointment_Metric]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         08/01/2019 - TMB - create stored procedure
--         11/08/2019 - TMB - allter stored procedure
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



SELECT newpt2.PAT_ENC_CSN_ID,
       newpt2.APPT_SERIAL_NUM,
       newpt2.Seq,
       newpt2.APPT_SERIAL_NUM_COUNT,
       newpt2.APPT_STATUS_FLAG,
       newpt2.APPT_STATUS_C,
       newpt2.CANCEL_INITIATOR,
       newpt2.APPT_DT,
       newpt2.APPT_MADE_DTTM,
       newpt2.Appointment_Request_Date,
       newpt2.Appointment_Lag_Days,
       newpt2.Original_Appointment_Request_Date,
       newpt2.Original_CANCEL_INITIATOR,
       newpt2.Last_APPT_STATUS_C,
       newpt2.Last_APPT_STATUS_FLAG,
       newpt2.Last_Seq,
       newpt2.Last_Appointment_Request_Date,
       newpt2.Last_APPT_DT,
       newpt2.Last_CANCEL_INITIATOR,
       newpt2.Appointment_Lag_Business_Days,
       newpt2.Appointment_Lag_Business_Days_from_Original,
       (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE ddte.weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND ddte.day_date >= newpt2.Original_Appointment_Request_Date AND ddte.day_date < newpt2.Last_APPT_DT) Last_Appointment_Lag_Business_Days_from_Original -- Number of business days between an appointment's original Appointment Request Date and the Appointment date for the latest scheduled appointment linked to this appointment via the Cancel/Reschedule workflow
INTO #newpt -- Tracking appointment status history by APPT_SERIAL_NUM; Measure business days between request and appointment dates.
FROM
(
	SELECT newpt.PAT_ENC_CSN_ID,
           newpt.APPT_SERIAL_NUM,
           newpt.Seq,
           newpt.APPT_SERIAL_NUM_COUNT,
           newpt.APPT_STATUS_FLAG,
           newpt.APPT_STATUS_C,
           newpt.CANCEL_INITIATOR,
           newpt.APPT_DT,
           newpt.APPT_MADE_DTTM,
           newpt.Appointment_Request_Date,
           newpt.Appointment_Lag_Days,
           newpt.Original_Appointment_Request_Date,
           newpt.Original_CANCEL_INITIATOR,
           newpt.Last_APPT_STATUS_C,
           newpt.Last_APPT_STATUS_FLAG,
           newpt.Last_Seq,
           newpt.Last_Appointment_Request_Date,
           newpt.Last_APPT_DT,
           newpt.Last_CANCEL_INITIATOR,
           newpt.Appointment_Lag_Business_Days,
		   (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE ddte.weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND ddte.day_date >= newpt.Original_Appointment_Request_Date AND ddte.day_date < newpt.APPT_DT) Appointment_Lag_Business_Days_from_Original -- Number of business days between an appointment's original Appointment Request Date and the Scheduled Appointment Date of the original appointment or an appointment linked to this original appointment using the Cancel/Reschedule workflow
	FROM
	(
		SELECT  appts.PAT_ENC_CSN_ID,
				appts.APPT_SERIAL_NUM,
				appts.Seq,
				appts.APPT_SERIAL_NUM_COUNT,
				appts.APPT_STATUS_FLAG,
				appts.APPT_STATUS_C,
				appts.CANCEL_INITIATOR,
				appts.APPT_DT,
				appts.APPT_MADE_DTTM,
				appts.Appointment_Request_Date,
				CASE
					WHEN (appts.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd, appts.Appointment_Request_Date, appts.APPT_DT)
					ELSE CAST(NULL AS INT)
				END AS Appointment_Lag_Days, -- Number of calendar days between the Appointment Request Date and the Scheduled Appointment Date
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT = 1 AND appts.Seq = 1 THEN Appointment_Request_Date
					ELSE LAG(appts.Appointment_Request_Date,appts.Seq-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				END AS Original_Appointment_Request_Date, -- Appointment Request Date for the originally scheduled appointment or the first appointment that was rescheduled using the Cancel/Reschedule workflow 
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT = 1 AND appts.Seq = 1 THEN appts.CANCEL_INITIATOR
					ELSE LAG(appts.CANCEL_INITIATOR,appts.Seq-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				END AS Original_CANCEL_INITIATOR, -- Resource that canceled the scheduled appointment or the first appointment that was rescheduled using the Cancel/Reschedule workflow (PATIENT,PROVIDER,OTHER)
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_STATUS_C,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_APPT_STATUS_C, -- Appointment Status Code for last scheduled appointment linked to an appointment that was rescheduled using the Cancel/Reschedule workflow
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_STATUS_FLAG,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_APPT_STATUS_FLAG, -- Appointment Status description for last scheduled appointment linked to an appointment that was rescheduled using the Cancel/Reschedule workflow
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.Seq,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_Seq, -- sequence number of last scheduled appointment linked to an appointment that was rescheduled using the Cancel/Reschedule workflow
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.Appointment_Request_Date,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_Appointment_Request_Date, -- Appointment Request Date for the last scheduled appointment linked to an appointment that was rescheduled using the Cancel/Reschedule workflow
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_DT,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_APPT_DT, -- Appointment date for the last scheduled appointment linked to an appointment that was rescheduled using the Cancel/Reschedule workflow
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.CANCEL_INITIATOR,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_CANCEL_INITIATOR, -- Resource that canceled the last scheduled appointment linked to an appointment that was rescheduled using the Cancel/Reschedule workflow (PATIENT,PROVIDER,OTHER)
				(SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE ddte.weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND ddte.day_date >= appts.Appointment_Request_Date AND ddte.day_date < appts.APPT_DT) Appointment_Lag_Business_Days -- Number of business days between the Appointment Request Date and the Scheduled Appointment Date

		FROM
		(
				SELECT  main.PAT_ENC_CSN_ID,
						main.APPT_SERIAL_NUM,
						ROW_NUMBER() OVER (PARTITION BY main.APPT_SERIAL_NUM ORDER BY main.APPT_MADE_DTTM) AS Seq, -- sequence number for identifying and ordering linked appointments
						COUNT(*) OVER (PARTITION BY main.APPT_SERIAL_NUM) APPT_SERIAL_NUM_COUNT,  -- number of linked appoointments; > 1 indicates a cancellation and a reschedule using the Cancel/Reschedule workflow
						main.APPT_STATUS_FLAG,
						main.APPT_STATUS_C,
						main.CANCEL_INITIATOR,
						main.APPT_DT,
						main.APPT_MADE_DTTM,
						CASE -- Appointment Request Date is the earlier of the referral entry/change dates and the creation date of the appointment
							WHEN main.ENTRY_DATE IS NULL THEN
								main.APPT_MADE_DATE
							WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
								main.APPT_MADE_DATE
							WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
								main.ENTRY_DATE
							ELSE
								main.CHANGE_DATE
						END AS Appointment_Request_Date,
						aggr.NEW_TO_SPEC

				FROM Stage.Scheduled_Appointment AS main
				LEFT OUTER JOIN -- Set flag indicating that an appointment or a set of related ("linked") appointments can be classified as a new patient encounter
				(
					SELECT APPT_SERIAL_NUM,
					ROW_NUMBER() OVER (PARTITION BY APPT_SERIAL_NUM ORDER BY APPT_MADE_DTTM) AS Seq,
					CASE WHEN VIS_NEW_TO_SPEC_YN = 'Y' THEN 1 ELSE 0 END AS NEW_TO_SPEC
					FROM DS_HSDM_App.Stage.Scheduled_Appointment
				    WHERE (APPT_MADE_DATE BETWEEN @locstartdate and @locenddate)
				    OR (ENTRY_DATE BETWEEN @locstartdate and @locenddate)
				    OR (CHANGE_DATE BETWEEN @locstartdate and @locenddate)
				) aggr
					ON aggr.APPT_SERIAL_NUM = main.APPT_SERIAL_NUM
					AND aggr.Seq = 1
				WHERE (main.APPT_MADE_DATE BETWEEN @locstartdate and @locenddate)
				OR (main.ENTRY_DATE BETWEEN @locstartdate and @locenddate)
				OR (main.CHANGE_DATE BETWEEN @locstartdate and @locenddate)
		) appts

		WHERE appts.NEW_TO_SPEC = 1
	) newpt
	WHERE newpt.Original_Appointment_Request_Date BETWEEN @locstartdate AND @locenddate
) newpt2

SELECT 
	   CAST('New Patient Appointment' AS VARCHAR(50)) AS event_type,
       CASE
           WHEN evnts.APPT_STATUS_FLAG IS NOT NULL THEN
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
       evnts.event_category,
       evnts.pod_id,
       evnts.pod_name,
       evnts.hub_id,
       evnts.hub_name,
       evnts.epic_department_id,
       evnts.epic_department_name,
       evnts.epic_department_name_external,
       evnts.peds,
       evnts.transplant,
       evnts.sk_Dim_Pt,
       evnts.sk_Fact_Pt_Acct,
       evnts.sk_Fact_Pt_Enc_Clrt,
       evnts.person_birth_date,
       evnts.person_gender,
       evnts.person_id,
       evnts.person_name,
       evnts.practice_group_id,
       evnts.practice_group_name,
       evnts.provider_id,
       evnts.provider_name,
       evnts.service_line_id,
       evnts.service_line,
       evnts.prov_service_line_id,
       evnts.prov_service_line,
       evnts.sub_service_line_id,
       evnts.sub_service_line,
       evnts.opnl_service_id,
       evnts.opnl_service_name,
       evnts.corp_service_line_id,
       evnts.corp_service_line,
       evnts.hs_area_id,
       evnts.hs_area_name,
       evnts.prov_hs_area_id,
       evnts.prov_hs_area_name,
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
	   evnts.som_division_name,
	   evnts.som_hs_area_id,
	   evnts.som_hs_area_name,

	   CASE
			/* Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Completed','Arrived','Scheduled','No Show','Left without seen','Present', or 'Reschedule' */
			WHEN evnts.APPT_SERIAL_NUM_COUNT = 1 AND evnts.APPT_STATUS_C IN (2,6,1,4,5,7,105) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1
			/* Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT */
			WHEN evnts.APPT_SERIAL_NUM_COUNT = 1 AND evnts.APPT_STATUS_C = 3 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1
			/* Original appointment was 'Canceled' or 'Canceled Late'.  Patient was given access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this rescheduled appointment was 'Completed','Arrived', or 'Scheduled' */
			WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.Last_APPT_STATUS_C IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Last_Appointment_Lag_Business_Days_from_Original <= 6 THEN 1
			/* Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled' */
			WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Last_APPT_STATUS_C NOT IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1
			/* Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  A subsequent rescheduled appointment had a status of 'Completed','Arrived', or 'Scheduled', but the appointment was not within 7 days of the original appointment request date */
			WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Last_APPT_STATUS_C IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 AND evnts.Last_Appointment_Lag_Business_Days_from_Original > 6 THEN 1
			/* Original appointment was 'Canceled' BY PROVIDER or OTHER.  Patient was given access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled' */
			WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR IN ('PROVIDER','OTHER') AND evnts.Last_APPT_STATUS_C = 3 AND evnts.Last_CANCEL_INITIATOR = 'PATIENT' AND evnts.Appointment_Lag_Days >= 0 AND evnts.Last_Appointment_Lag_Business_Days_from_Original <= 6 THEN 1
			ELSE 0
	   END AS AbleToAccess,

	   CASE WHEN evnts.VIS_NEW_TO_SYS_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SYS_YN,
       CASE WHEN evnts.VIS_NEW_TO_DEP_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_DEP_YN,
       CASE WHEN evnts.VIS_NEW_TO_PROV_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_PROV_YN,
       CASE WHEN evnts.VIS_NEW_TO_SPEC_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SPEC_YN,
       CASE WHEN evnts.VIS_NEW_TO_SERV_AREA_YN = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SERV_AREA_YN,
       CASE WHEN evnts.VIS_NEW_TO_LOC_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_LOC_YN,
	   ISNULL(evnts.appt_event_No_Show,CAST(0 AS INT)) AS appt_event_No_Show,
       ISNULL(evnts.appt_event_Canceled_Late,CAST(0 AS INT)) AS appt_event_Canceled_Late,
       ISNULL(evnts.appt_event_Canceled,CAST(0 AS INT)) AS appt_event_Canceled,
       ISNULL(evnts.appt_event_Scheduled,CAST(0 AS INT)) AS appt_event_Scheduled,
       ISNULL(evnts.appt_event_Provider_Canceled,CAST(0 AS INT)) AS appt_event_Provider_Canceled,
       ISNULL(evnts.appt_event_Completed,CAST(0 AS INT)) AS appt_event_Completed,
       ISNULL(evnts.appt_event_Arrived,CAST(0 AS INT)) AS appt_event_Arrived,
       ISNULL(evnts.appt_event_New_to_Specialty,CAST(0 AS INT)) AS appt_event_New_to_Specialty,
       evnts.APPT_STATUS_FLAG,
       evnts.APPT_STATUS_C,
       evnts.CANCEL_REASON_C,
	   evnts.APPT_CANC_DTTM,
	   evnts.CANCEL_REASON_NAME,
	   evnts.CANCEL_INITIATOR,
	   evnts.CANCEL_LEAD_HOURS,
	   evnts.Cancel_Lead_Days,
	   evnts.APPT_MADE_DTTM,
       evnts.APPT_MADE_DATE,
       evnts.ENTRY_DATE,
	   evnts.CHANGE_DATE,
	   evnts.Appointment_Request_Date,
	   evnts.APPT_DTTM,
       evnts.APPT_DT,
       evnts.Appointment_Lag_Days,
	   evnts.Appointment_Lag_Business_Days,
	   evnts.Last_APPT_STATUS_C AS Resch_APPT_STATUS_C,
	   evnts.Last_APPT_STATUS_FLAG AS Resch_APPT_STATUS_FLAG,
	   evnts.Last_CANCEL_INITIATOR AS Resch_CANCEL_INITIATOR,
	   evnts.Last_Appointment_Request_Date AS Resch_Appointment_Request_Date,
	   evnts.Last_APPT_DT AS Resch_APPT_DT,
	   evnts.Last_Appointment_Lag_Business_Days_from_Original AS Resch_Appointment_Lag_Business_Days_from_Initial_Request,
       evnts.MRN_int,
       evnts.CONTACT_DATE,
       evnts.PAT_ENC_CSN_ID,
       evnts.PRC_ID,
       evnts.PRC_NAME,
       evnts.sk_Dim_Physcn,
       evnts.UVaID,
	   evnts.DEPT_SPECIALTY_NAME,
	   evnts.PROV_SPECIALTY_NAME,
	   evnts.ENC_TYPE_C,
	   evnts.ENC_TYPE_TITLE,
	   evnts.APPT_CONF_STAT_NAME,
	   evnts.ZIP,
	   evnts.APPT_CONF_DTTM,
	   evnts.SER_RPT_GRP_SIX AS financial_division,
	   evnts.SER_RPT_GRP_EIGHT AS financial_subdivision,
	   evnts.F2F_Flag,
	   evnts.Entry_UVaID,
	   evnts.Canc_UVaID,
	   evnts.PHONE_REM_STAT_NAME,
	   evnts.BUSINESS_UNIT,
	   evnts.Prov_Typ,
	   evnts.Staff_Resource,
	   evnts.BILL_PROV_YN

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
        SELECT DISTINCT
            CAST(NULL AS VARCHAR(150)) AS event_category,
            main.epic_pod AS pod_id,
            main.mdmloc_pod AS pod_name,
            main.epic_hub AS hub_id,
            main.mdmloc_hub AS hub_name,
            main.epic_department_id,
            main.epic_department_name,
            main.epic_department_name_external,
            main.peds,
            main.transplant,
            main.sk_Dim_Pt,
            main.sk_Fact_Pt_Acct,
            main.sk_Fact_Pt_Enc_Clrt,
            main.person_birth_date,
            main.person_gender,
            main.person_id,
            main.person_name,
            main.practice_group_id,
            main.practice_group_name,
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
			main.som_division_name,
			main.som_hs_area_id,
			main.som_hs_area_name,
            main.VIS_NEW_TO_SYS_YN,
            main.VIS_NEW_TO_DEP_YN,
            main.VIS_NEW_TO_PROV_YN,
            main.VIS_NEW_TO_SPEC_YN,
            main.VIS_NEW_TO_SERV_AREA_YN,
            main.VIS_NEW_TO_LOC_YN,
                                                 -- Appt Status Flags
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'No Show' ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_No_Show,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled Late' ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Canceled_Late,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled' ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Scheduled' ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
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
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Provider_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C IN ( 2 ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Completed,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C IN ( 6 ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Arrived,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.VIS_NEW_TO_SPEC_YN = 'Y')
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_New_to_Specialty,
            main.APPT_STATUS_FLAG,
            main.APPT_STATUS_C,
			main.APPT_CANC_DTTM,
            main.CANCEL_REASON_C,
			main.CANCEL_REASON_NAME,
			main.CANCEL_INITIATOR,
			main.CANCEL_LEAD_HOURS,
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
            main.APPT_MADE_DATE,
            main.ENTRY_DATE,
			main.CHANGE_DATE,
			main.Appointment_Request_Date,
			main.APPT_DTTM,
            main.APPT_DT,
			main.Appointment_Lag_Days,
			main.Appointment_Lag_Business_Days,
			main.APPT_SERIAL_NUM_COUNT,
			main.Seq,
			main.Last_APPT_STATUS_C,
			main.Last_APPT_STATUS_FLAG,
			main.Last_CANCEL_INITIATOR,
			main.Last_Appointment_Request_Date,
			main.Last_APPT_DT,
			main.Last_Appointment_Lag_Business_Days_from_Original,
			main.Original_Appointment_Request_Date,
            main.MRN_int,
            main.CONTACT_DATE,
            main.PAT_ENC_CSN_ID,
            main.PRC_ID,
            main.PRC_NAME,
            main.sk_Dim_Physcn,
            main.UVaID,
			main.DEPT_SPECIALTY_NAME,
			main.PROV_SPECIALTY_NAME,
		    main.ENC_TYPE_C,
			main.ENC_TYPE_TITLE,
			main.APPT_CONF_STAT_NAME,
			main.ZIP,
			main.APPT_CONF_DTTM,
			main.SER_RPT_GRP_SIX,
			main.SER_RPT_GRP_EIGHT,
			main.F2F_Flag,
			main.Entry_UVaID,
			main.Canc_UVaID,
			main.PHONE_REM_STAT_NAME,
			main.BUSINESS_UNIT,
		    main.Prov_Typ,
			main.Staff_Resource,
			main.BILL_PROV_YN

        FROM
        ( --main
		    SELECT newpt.PAT_ENC_CSN_ID,
                   newpt.Seq,
                   newpt.APPT_SERIAL_NUM_COUNT,
                   newpt.APPT_STATUS_FLAG,
                   newpt.APPT_STATUS_C,
                   newpt.CANCEL_INITIATOR,
                   newpt.APPT_DT,
                   newpt.APPT_MADE_DTTM,
                   newpt.Appointment_Request_Date,
                   newpt.Appointment_Lag_Days,
                   newpt.Original_Appointment_Request_Date,
                   newpt.Original_CANCEL_INITIATOR,
                   newpt.Last_APPT_STATUS_C,
                   newpt.Last_APPT_STATUS_FLAG,
                   newpt.Last_Seq,
                   newpt.Last_Appointment_Request_Date,
                   newpt.Last_APPT_DT,
                   newpt.Appointment_Lag_Business_Days,
				   newpt.Appointment_Lag_Business_Days_from_Original,
				   CASE
				     WHEN newpt.APPT_SERIAL_NUM_COUNT = 1 THEN NULL
					 ELSE newpt.Last_Appointment_Lag_Business_Days_from_Original
				   END AS Last_Appointment_Lag_Business_Days_from_Original,
				   newpt.Last_CANCEL_INITIATOR,
                   sched.epic_service_line,
                   sched.mdmloc_service_line,
                   sched.epic_pod,
                   sched.mdmloc_pod,
                   sched.epic_hub,
                   sched.mdmloc_hub,
                   sched.epic_department_id,
                   sched.epic_department_name,
                   sched.epic_department_name_external,
                   sched.peds,
                   sched.transplant,
                   sched.sk_Dim_Pt,
                   sched.sk_Fact_Pt_Acct,
                   sched.sk_Fact_Pt_Enc_Clrt,
                   sched.person_birth_date,
                   sched.person_gender,
                   sched.person_id,
                   sched.person_name,
                   sched.practice_group_id,
                   sched.practice_group_name,
                   sched.provider_id,
                   sched.provider_name,
                   sched.service_line_id,
                   sched.service_line,
                   sched.prov_service_line_id,
                   sched.prov_service_line,
                   sched.sub_service_line_id,
                   sched.sub_service_line,
                   sched.opnl_service_id,
                   sched.opnl_service_name,
                   sched.corp_service_line_id,
                   sched.corp_service_line,
                   sched.hs_area_id,
                   sched.hs_area_name,
                   sched.prov_hs_area_id,
                   sched.prov_hs_area_name,
                   sched.CANCEL_REASON_C,
                   sched.MRN_int,
                   sched.CONTACT_DATE,
                   sched.PRC_ID,
                   sched.PRC_NAME,
                   sched.sk_Dim_Physcn,
                   sched.UVaID,
                   sched.VIS_NEW_TO_SYS_YN,
                   sched.VIS_NEW_TO_DEP_YN,
                   sched.VIS_NEW_TO_PROV_YN,
                   sched.VIS_NEW_TO_SPEC_YN,
                   sched.VIS_NEW_TO_SERV_AREA_YN,
                   sched.VIS_NEW_TO_LOC_YN,
                   sched.APPT_MADE_DATE,
                   sched.ENTRY_DATE,
                   sched.DEPT_SPECIALTY_NAME,
                   sched.PROV_SPECIALTY_NAME,
                   sched.APPT_DTTM,
                   sched.ENC_TYPE_C,
                   sched.ENC_TYPE_TITLE,
                   sched.APPT_CONF_STAT_NAME,
                   sched.ZIP,
                   sched.APPT_CONF_DTTM,
                   sched.CANCEL_REASON_NAME,
                   sched.SER_RPT_GRP_SIX,
                   sched.SER_RPT_GRP_EIGHT,
                   sched.F2F_Flag,
                   sched.CANCEL_LEAD_HOURS,
                   sched.APPT_CANC_DTTM,
                   sched.Entry_UVaID,
                   sched.Canc_UVaID,
                   sched.PHONE_REM_STAT_NAME,
                   sched.CHANGE_DATE,
                   sched.BUSINESS_UNIT,
                   sched.Prov_Typ,
                   sched.Staff_Resource,
                   sched.rev_location_id,
                   sched.rev_location,
                   sched.financial_division_id,
                   sched.financial_division_name,
                   sched.financial_sub_division_id,
                   sched.financial_sub_division_name,
                   sched.som_group_id,
                   sched.som_group_name,
                   sched.som_department_id,
                   sched.som_department_name,
                   sched.som_division_id,
                   sched.som_division_name,
                   sched.som_hs_area_id,
                   sched.som_hs_area_name,
                   sched.BILL_PROV_YN
			FROM #newpt newpt
			INNER JOIN
			(
				SELECT appts.PAT_ENC_CSN_ID,
				       appts.RPT_GRP_THIRTY AS epic_service_line,
					   mdmloc.SERVICE_LINE AS mdmloc_service_line,
					   appts.RPT_GRP_SIX AS epic_pod,
					   mdmloc.PFA_POD AS mdmloc_pod,
					   appts.RPT_GRP_SEVEN AS epic_hub,
					   mdmloc.HUB AS mdmloc_hub,
					   appts.DEPARTMENT_ID AS epic_department_id,
					   mdm.epic_department_name AS epic_department_name,
					   mdm.epic_department_name_external AS epic_department_name_external,
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
					   appts.CANCEL_REASON_C,
					   CAST(appts.IDENTITY_ID AS INTEGER) AS MRN_int,
					   appts.CONTACT_DATE,
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
					   appts.DEPT_SPECIALTY_NAME,
					   appts.PROV_SPECIALTY_NAME,
					   appts.APPT_DTTM,
					   appts.ENC_TYPE_C,
					   appts.ENC_TYPE_TITLE,
					   appts.APPT_CONF_STAT_NAME,
					   appts.ZIP,
					   appts.APPT_CONF_DTTM,
					   appts.CANCEL_REASON_NAME,
					   appts.SER_RPT_GRP_SIX,
					   appts.SER_RPT_GRP_EIGHT,
					   appts.F2F_Flag,
					   appts.CANCEL_LEAD_HOURS,
					   appts.APPT_CANC_DTTM,
					   entryemp.EMPlye_Systm_Login AS Entry_UVaID,
					   cancemp.EMPlye_Systm_Login AS Canc_UVaID,
					   appts.PHONE_REM_STAT_NAME,
					   appts.CHANGE_DATE,
					   mdmloc.BUSINESS_UNIT,
					   ser.Prov_Typ,
					   ser.Staff_Resource,
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
					   physcn.SOM_division_5 AS	som_division_id,
					   physcn.SOM_division_name AS som_division_name,
					   physcn.som_hs_area_id AS	som_hs_area_id,
					   physcn.som_hs_area_name AS som_hs_area_name,
					   --appts.APPT_SERIAL_NUM,
					   appts.RESCHED_APPT_CSN_ID,
					   appts.BILL_PROV_YN

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

					-- -------------------------------------
					-- SOM Hierarchy--
					-- -------------------------------------
					LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
						ON physcn.sk_Dim_Physcn = doc.sk_Dim_Physcn

			) AS sched
			ON sched.PAT_ENC_CSN_ID = newpt.PAT_ENC_CSN_ID
		) AS main
		WHERE main.Seq = 1
    ) evnts
        ON (date_dim.day_date = CAST(evnts.Original_Appointment_Request_Date AS SMALLDATETIME))

WHERE date_dim.day_date >= @locstartdate
      AND date_dim.day_date < @locenddate

ORDER BY date_dim.day_date;

GO


