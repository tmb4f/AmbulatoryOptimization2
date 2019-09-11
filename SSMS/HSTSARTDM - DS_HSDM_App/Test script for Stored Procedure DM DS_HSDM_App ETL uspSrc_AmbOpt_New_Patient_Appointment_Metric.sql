USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME
       ,@enddate SMALLDATETIME

--SET @startdate = NULL
--SET @enddate = NULL
--SET @startdate = '2/1/2019 00:00 AM'
--SET @enddate = '3/4/2019 11:59 PM'
--SET @startdate = '7/1/2017 00:00 AM'
--SET @startdate = '7/1/2018 00:00 AM'
--SET @enddate = '6/30/2019 11:59 PM'
--SET @startdate = '7/1/2019 00:00 AM'
--SET @enddate = '7/31/2019 11:59 PM'

--CREATE PROCEDURE [ETL].[uspSrc_AmbOpt_New_Patient_Appointment_Metric]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 
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
-- Percentage of New Patients Seen Within 7 Business Days
--				SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Business_Days <= 6)
--              /
--              SUM(appt_event_New_to_Specialty = 1)
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDW_Prod.Rptg.vwDim_Date
--              DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--              DS_HSDW_Prod.Rptg.vwDim_Patient
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--              DS_HSDW_Prod.Rptg.vwDim_Physcn
--              DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc
--              DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart
--              DS_HSDM_App.Rptg.vwRef_Crosswalk_HSEntity_Prov
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_New_Patient_Appointment_Metric]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         08/01/2019 - TMB - create stored procedure
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

--SELECT @locstartdate, @locenddate

IF OBJECT_ID('tempdb..#newpt ') IS NOT NULL
DROP TABLE #newpt

--IF OBJECT_ID('tempdb..#newpt2 ') IS NOT NULL
--DROP TABLE #newpt2

IF OBJECT_ID('tempdb..#metric ') IS NOT NULL
DROP TABLE #metric

IF OBJECT_ID('tempdb..#metric2 ') IS NOT NULL
DROP TABLE #metric2

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
       newpt2.NEW_TO_SPEC,
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
       (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE ddte.weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND ddte.day_date >= newpt2.Original_Appointment_Request_Date AND ddte.day_date < newpt2.Last_APPT_DT) Last_Appointment_Lag_Business_Days_from_Original
INTO #newpt
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
           newpt.NEW_TO_SPEC,
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
		   (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE ddte.weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND ddte.day_date >= newpt.Original_Appointment_Request_Date AND ddte.day_date < newpt.APPT_DT) Appointment_Lag_Business_Days_from_Original
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
				appts.NEW_TO_SPEC,
				CASE
					WHEN (appts.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd, appts.Appointment_Request_Date, appts.APPT_DT)
					ELSE CAST(NULL AS INT)
				END AS Appointment_Lag_Days,
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT = 1 AND appts.Seq = 1 THEN Appointment_Request_Date
					ELSE LAG(appts.Appointment_Request_Date,appts.Seq-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				END AS Original_Appointment_Request_Date,
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT = 1 AND appts.Seq = 1 THEN appts.CANCEL_INITIATOR
					ELSE LAG(appts.CANCEL_INITIATOR,appts.Seq-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				END AS Original_CANCEL_INITIATOR,
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_STATUS_C,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_APPT_STATUS_C,
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_STATUS_FLAG,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_APPT_STATUS_FLAG,
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.Seq,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_Seq,
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.Appointment_Request_Date,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_Appointment_Request_Date,
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_DT,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_APPT_DT,
				CASE
					WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.CANCEL_INITIATOR,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
					ELSE NULL
				END AS Last_CANCEL_INITIATOR,
				(SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE ddte.weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND ddte.day_date >= appts.Appointment_Request_Date AND ddte.day_date < appts.APPT_DT) Appointment_Lag_Business_Days

		FROM
		(
				SELECT  main.PAT_ENC_CSN_ID,
						main.APPT_SERIAL_NUM,
						ROW_NUMBER() OVER (PARTITION BY main.APPT_SERIAL_NUM ORDER BY main.APPT_MADE_DTTM) AS Seq,
						COUNT(*) OVER (PARTITION BY main.APPT_SERIAL_NUM) APPT_SERIAL_NUM_COUNT,
						main.APPT_STATUS_FLAG,
						main.APPT_STATUS_C,
						main.CANCEL_INITIATOR,
						main.APPT_DT,
						--main.VIS_NEW_TO_SPEC_YN,
			--            main.APPT_MADE_DATE,
			--            main.ENTRY_DATE,
			--            main.APPT_DTTM,
						--main.CHANGE_DATE,
						main.APPT_MADE_DTTM,
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
						aggr.NEW_TO_SPEC

				FROM Stage.Scheduled_Appointment AS main
				LEFT OUTER JOIN
				(
					SELECT APPT_SERIAL_NUM,
					MAX(CASE WHEN VIS_NEW_TO_SPEC_YN = 'Y' THEN 1 ELSE 0 END) AS NEW_TO_SPEC--,
					--COUNT(*) AS APPT_SERIAL_NUM_COUNT
					FROM DS_HSDM_App.Stage.Scheduled_Appointment
					GROUP BY APPT_SERIAL_NUM
				) aggr
					ON aggr.APPT_SERIAL_NUM = main.APPT_SERIAL_NUM
				--WHERE (main.APPT_MADE_DATE BETWEEN @locstartdate and @locenddate)
				--OR (main.ENTRY_DATE BETWEEN @locstartdate and @locenddate)
				--OR (main.CHANGE_DATE BETWEEN @locstartdate and @locenddate)
		) appts

		WHERE appts.NEW_TO_SPEC = 1
	) newpt
	WHERE newpt.Original_Appointment_Request_Date BETWEEN @locstartdate AND @locenddate
) newpt2
/*
SELECT APPT_SERIAL_NUM_COUNT,
       newpt.APPT_SERIAL_NUM,
       PAT_ENC_CSN_ID,
       Seq,
       NEW_TO_SPEC,
       APPT_MADE_DTTM,
       Appointment_Request_Date,
       APPT_DT,
       Appointment_Lag_Days,
	   Appointment_Lag_Business_Days,
       APPT_STATUS_C,
       APPT_STATUS_FLAG,
       CANCEL_INITIATOR,
       --Original_CANCEL_INITIATOR,
       Original_Appointment_Request_Date,
       Last_Seq,
       Last_Appointment_Request_Date,
       Last_APPT_DT,
       Appointment_Lag_Business_Days_from_Original,
       Last_APPT_STATUS_C,
       Last_APPT_STATUS_FLAG,
       Original_CANCEL_INITIATOR,
	   Last_CANCEL_INITIATOR,
       Last_Appointment_Lag_Business_Days_from_Original,
	   CASE
			WHEN APPT_SERIAL_NUM_COUNT = 1 AND APPT_STATUS_C IN (2,6,1,4,5,7,105) AND Appointment_Lag_Days >= 0 AND Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Completed','Arrived','Scheduled','No Show','Left without seen','Present', or 'Reschedule'
			WHEN APPT_SERIAL_NUM_COUNT = 1 AND APPT_STATUS_C = 3 AND CANCEL_INITIATOR = 'PATIENT' AND Appointment_Lag_Days >= 0 AND Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT
			WHEN APPT_SERIAL_NUM_COUNT > 1 AND Seq = 1 AND Last_APPT_STATUS_C IN (2,6,1) AND Appointment_Lag_Days >= 0 AND Last_Appointment_Lag_Business_Days_from_Original <= 6 THEN 1 -- Original appointment was 'Canceled' or 'Canceled Late'.  Patient was given access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this rescheduled appointment was 'Completed','Arrived', or 'Scheduled'
			WHEN APPT_SERIAL_NUM_COUNT > 1 AND Seq = 1 AND CANCEL_INITIATOR = 'PATIENT' AND Last_APPT_STATUS_C NOT IN (2,6,1) AND Appointment_Lag_Days >= 0 AND Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled'
			WHEN APPT_SERIAL_NUM_COUNT > 1 AND Seq = 1 AND CANCEL_INITIATOR = 'PATIENT' AND Last_APPT_STATUS_C IN (2,6,1) AND Appointment_Lag_Days >= 0 AND Appointment_Lag_Business_Days <= 6 AND Last_Appointment_Lag_Business_Days_from_Original > 6 THEN 1 -- Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  A subsequent rescheduled appointment had a status of 'Completed','Arrived', or 'Scheduled', but the appointment was not within 7 days of the original appointment request date
			WHEN APPT_SERIAL_NUM_COUNT > 1 AND Seq = 1 AND CANCEL_INITIATOR IN ('PROVIDER','OTHER') AND Last_APPT_STATUS_C = 3 AND Last_CANCEL_INITIATOR = 'PATIENT' AND Appointment_Lag_Days >= 0 AND Last_Appointment_Lag_Business_Days_from_Original <= 6 THEN 1 -- Original appointment was 'Canceled' BY PROVIDER or OTHER.  Patient was given access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled'
			ELSE 0
	   END AS AbleToAccess
INTO #newpt2
FROM #newpt newpt
--INNER JOIN
--(
--SELECT DISTINCT APPT_SERIAL_NUM
--FROM #newpt
--WHERE Last_APPT_STATUS_C = 3 AND Last_CANCEL_INITIATOR = 'PATIENT' AND Appointment_Lag_Business_Days_from_Original <= 6
--) newpt2
--ON newpt2.APPT_SERIAL_NUM = newpt.APPT_SERIAL_NUM

SELECT *
FROM #newpt2
WHERE Seq = 1
AND AbleToAccess = 1
ORDER BY APPT_SERIAL_NUM_COUNT
       , APPT_SERIAL_NUM
	   --, Seq
*/

SELECT 
	   --evnts.APPT_SERIAL_NUM,
       --evnts.PAT_ENC_CSN_ID,
	   --evnts.Seq,
	   --evnts.APPT_SERIAL_NUM_COUNT,
       --evnts.APPT_STATUS_FLAG,
	   --evnts.CANCEL_INITIATOR,
	   --evnts.Appointment_Request_Date,
       --evnts.APPT_DT,
	   --evnts.Appointment_Lag_Days,
	   --evnts.Appointment_Lag_Business_Days,
	   --evnts.Last_APPT_STATUS_FLAG,
	   --evnts.Last_Appointment_Request_Date,
	   --evnts.Original_Appointment_Request_Date,
	   --evnts.Last_APPT_DT,
	   --evnts.Appointment_Lag_Business_Days_from_Original,
	   --evnts.Last_Appointment_Lag_Business_Days_from_Original,
	   --evnts.Last_CANCEL_INITIATOR,
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
			WHEN evnts.APPT_SERIAL_NUM_COUNT = 1 AND evnts.APPT_STATUS_C IN (2,6,1,4,5,7,105) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was giveN evnts.Access to aN evnts.Appointment within 7 days of the appointment request date.  The status of this appointment was 'Completed','Arrived','Scheduled','No Show','Left without seen','Present', or 'Reschedule'
			WHEN evnts.APPT_SERIAL_NUM_COUNT = 1 AND evnts.APPT_STATUS_C = 3 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was giveN evnts.Access to aN evnts.Appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT
			WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.Last_APPT_STATUS_C IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Last_Appointment_Lag_Business_Days_from_Original <= 6 THEN 1 -- Original appointment was 'Canceled' or 'Canceled Late'.  Patient was giveN evnts.Access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this rescheduled appointment was 'Completed','Arrived', or 'Scheduled'
			WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Last_APPT_STATUS_C NOT IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was giveN evnts.Access to aN evnts.Appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled'
			WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Last_APPT_STATUS_C IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 AND evnts.Last_Appointment_Lag_Business_Days_from_Original > 6 THEN 1 -- Patient was giveN evnts.Access to aN evnts.Appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  A subsequent rescheduled appointment had a status of 'Completed','Arrived', or 'Scheduled', but the appointment was not within 7 days of the original appointment request date
			WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR IN ('PROVIDER','OTHER') AND evnts.Last_APPT_STATUS_C = 3 AND evnts.Last_CANCEL_INITIATOR = 'PATIENT' AND evnts.Appointment_Lag_Days >= 0 AND evnts.Last_Appointment_Lag_Business_Days_from_Original <= 6 THEN 1 -- Original appointment was 'Canceled' BY PROVIDER or OTHER.  Patient was given access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled'
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
       --evnts.CHECKIN_DTTM,
       --evnts.CHECKOUT_DTTM,
       --evnts.VISIT_END_DTTM,
       --evnts.CYCLE_TIME_MINUTES,
	   
       --CASE
       --    WHEN (evnts.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd, evnts.Appointment_Request_Date, evnts.APPT_DT)
       --    ELSE CAST(NULL AS INT)
       --END AS Appointment_Lag_Days,
       --evnts.CYCLE_TIME_MINUTES_Adjusted,

	   evnts.DEPT_SPECIALTY_NAME,
	   evnts.PROV_SPECIALTY_NAME,
	   evnts.ENC_TYPE_C,
	   evnts.ENC_TYPE_TITLE,
	   evnts.APPT_CONF_STAT_NAME,
	   evnts.ZIP,
	   evnts.APPT_CONF_DTTM,
	   --evnts.SIGNIN_DTTM,
	   --evnts.ARVL_LIST_REMOVE_DTTM,
	   --evnts.ROOMED_DTTM,
	   --evnts.NURSE_LEAVE_DTTM,
	   --evnts.PHYS_ENTER_DTTM,
	   evnts.SER_RPT_GRP_SIX AS financial_division,
	   evnts.SER_RPT_GRP_EIGHT AS financial_subdivision,
	   evnts.F2F_Flag,
	   --evnts.TIME_TO_ROOM_MINUTES,
	   --evnts.TIME_IN_ROOM_MINUTES,
	   --evnts.BEGIN_CHECKIN_DTTM,
	   --evnts.PAGED_DTTM,
	   --evnts.FIRST_ROOM_ASSIGN_DTTM,
	   evnts.Entry_UVaID,
	   evnts.Canc_UVaID,
	   evnts.PHONE_REM_STAT_NAME,

	   evnts.BUSINESS_UNIT,
	   evnts.Prov_Typ,
	   evnts.Staff_Resource,
	   --evnts.APPT_SERIAL_NUM,
	   --evnts.RESCHED_APPT_CSN_ID,
       --(SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date WHERE weekday_ind = 1 AND day_date >= evnts.Appointment_Request_Date AND day_date < evnts.APPT_DT) Appointment_Lag_Business_Days,
       --(SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND day_date >= evnts.Appointment_Request_Date AND day_date < evnts.APPT_DT) Appointment_Lag_Business_Days,
	   evnts.BILL_PROV_YN--,
	   --evnts.NEW_TO_SPEC,
	   --evnts.Seq
	   --evnts.APPT_SERIAL_NUM_COUNT
	   --COUNT(*) OVER (PARTITION BY evnts.APPT_SERIAL_NUM) APPT_SERIAL_NUM_COUNT,
	   --ROW_NUMBER() OVER (PARTITION BY APPT_SERIAL_NUM ORDER BY APPT_MADE_DTTM) AS Seq
	  -- CASE
			--WHEN evnts.APPT_SERIAL_NUM_COUNT = 1 AND evnts.APPT_STATUS_C IN (2,6,1,4,5,7,105) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was given access to an appointment within 7 days of the appointment request date.  The status of this appointment was 'Completed','Arrived','Scheduled','No Show','Left without seen','Present','Reschedule'
			--WHEN evnts.APPT_SERIAL_NUM_COUNT = 1 AND evnts.APPT_STATUS_C IN (3) AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was given access to an appointment within 7 days of the appointment request date.  'Canceled' or 'Canceled Late' BY PATIENT
			----WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.APPT_STATUS_C IN (3) AND evnts.Last_APPT_STATUS_C IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days_from_Original <= 6 THEN 1 -- Original appointment was 'Canceled' or 'Canceled Late'.  Patient was given access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this rescheduled appointment was 'Completed','Arrived', or 'Scheduled'
			----WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.APPT_STATUS_C IN (3) AND evnts.CANCEL_INITIATOR IN ('PATIENT') AND evnts.Last_APPT_STATUS_C IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 AND evnts.Appointment_Lag_Business_Days_from_Original > 6 THEN 1 -- Patient was given access to an appointment within 7 days of the appointment request date.  'Canceled' or 'Canceled Late' BY PATIENT.  Patient was given access to a rescheduled appointment that was not within 7 days of the original appointment request date.  The status of this rescheduled appointment was 'Completed','Arrived', or 'Scheduled'
			----WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.APPT_STATUS_C IN (3) AND evnts.CANCEL_INITIATOR IN ('PATIENT') AND evnts.Last_APPT_STATUS_C NOT IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was given access to an appointment within 7 days of the appointment request date.  'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled'
			--ELSE 0
	  -- END AS AbleToAccess
	  -- CASE
			--WHEN evnts.APPT_SERIAL_NUM_COUNT = 1 AND evnts.APPT_STATUS_C IN (2,6,1,4,5,7,105) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was giveN evnts.Access to aN evnts.Appointment within 7 days of the appointment request date.  The status of this appointment was 'Completed','Arrived','Scheduled','No Show','Left without seen','Present', or 'Reschedule'
			--WHEN evnts.APPT_SERIAL_NUM_COUNT = 1 AND evnts.APPT_STATUS_C = 3 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was giveN evnts.Access to aN evnts.Appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT
			--WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.Last_APPT_STATUS_C IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Last_Appointment_Lag_Business_Days_from_Original <= 6 THEN 1 -- Original appointment was 'Canceled' or 'Canceled Late'.  Patient was giveN evnts.Access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this rescheduled appointment was 'Completed','Arrived', or 'Scheduled'
			--WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Last_APPT_STATUS_C NOT IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 THEN 1 -- Patient was giveN evnts.Access to aN evnts.Appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled'
			--WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR = 'PATIENT' AND evnts.Last_APPT_STATUS_C IN (2,6,1) AND evnts.Appointment_Lag_Days >= 0 AND evnts.Appointment_Lag_Business_Days <= 6 AND evnts.Last_Appointment_Lag_Business_Days_from_Original > 6 THEN 1 -- Patient was giveN evnts.Access to aN evnts.Appointment within 7 days of the appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  A subsequent rescheduled appointment had a status of 'Completed','Arrived', or 'Scheduled', but the appointment was not within 7 days of the original appointment request date
			--WHEN evnts.APPT_SERIAL_NUM_COUNT > 1 AND evnts.Seq = 1 AND evnts.CANCEL_INITIATOR IN ('PROVIDER','OTHER') AND evnts.Last_APPT_STATUS_C = 3 AND evnts.Last_CANCEL_INITIATOR = 'PATIENT' AND evnts.Appointment_Lag_Days >= 0 AND evnts.Last_Appointment_Lag_Business_Days_from_Original <= 6 THEN 1 -- Original appointment was 'Canceled' BY PROVIDER or OTHER.  Patient was given access to a rescheduled appointment within 7 days of the original appointment request date.  The status of this appointment was 'Canceled' or 'Canceled Late' BY PATIENT.  There were no subsequent rescheduled appointments with a status of 'Completed','Arrived', or 'Scheduled'
			--ELSE 0
	  -- END AS AbleToAccess

INTO #metric

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
            --main.CHECKIN_DTTM,
            --main.CHECKOUT_DTTM,
            --main.VISIT_END_DTTM,
            --main.CYCLE_TIME_MINUTES,
                                                 -- Calculated columns
-- Assumes that there is always a referral creation date (CHANGE_DATE) documented when a referral entry date (ENTRY_DATE) is documented
			--CASE
			--	WHEN main.ENTRY_DATE IS NULL THEN
			--		main.APPT_MADE_DATE
			--	WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
			--		main.APPT_MADE_DATE
			--	WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
			--		main.ENTRY_DATE
			--	ELSE
			--		main.CHANGE_DATE
			--END AS Appointment_Request_Date,
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
		    main.ENC_TYPE_C,
			main.ENC_TYPE_TITLE,
			main.APPT_CONF_STAT_NAME,
			main.ZIP,
			main.APPT_CONF_DTTM,
			--main.SIGNIN_DTTM,
			--main.ARVL_LIST_REMOVE_DTTM,
			--main.ROOMED_DTTM,
			--main.NURSE_LEAVE_DTTM,
			--main.PHYS_ENTER_DTTM,
			main.SER_RPT_GRP_SIX,
			main.SER_RPT_GRP_EIGHT,
			main.F2F_Flag,
		 --   main.TIME_TO_ROOM_MINUTES,
			--main.TIME_IN_ROOM_MINUTES,
			--main.BEGIN_CHECKIN_DTTM,
			--main.PAGED_DTTM,
			--main.FIRST_ROOM_ASSIGN_DTTM,
			main.Entry_UVaID,
			main.Canc_UVaID,
			main.PHONE_REM_STAT_NAME,
			main.BUSINESS_UNIT,
		    main.Prov_Typ,
			main.Staff_Resource,
			--main.APPT_SERIAL_NUM,
			--main.RESCHED_APPT_CSN_ID,
			main.BILL_PROV_YN--,
			--aggr.NEW_TO_SPEC--,
			--MAX(main.Seq) OVER (PARTITION BY main.APPT_SERIAL_NUM) MaxSeq
			--main.Original_CANCEL_INITIATOR,
			--main.Appointment_Lag_Business_Days_from_Original--,

        FROM
        ( --main
		    SELECT newpt.PAT_ENC_CSN_ID,
                   --newpt.APPT_SERIAL_NUM,
                   newpt.Seq,
                   newpt.APPT_SERIAL_NUM_COUNT,
                   newpt.APPT_STATUS_FLAG,
                   newpt.APPT_STATUS_C,
                   newpt.CANCEL_INITIATOR,
                   newpt.APPT_DT,
                   newpt.APPT_MADE_DTTM,
                   newpt.Appointment_Request_Date,
                   newpt.NEW_TO_SPEC,
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
				   --newpt.Last_Appointment_Lag_Business_Days_from_Original,
				   CASE
				     WHEN newpt.APPT_SERIAL_NUM_COUNT = 1 THEN NULL
					 ELSE newpt.Last_Appointment_Lag_Business_Days_from_Original
				   END AS Last_Appointment_Lag_Business_Days_from_Original,
				   newpt.Last_CANCEL_INITIATOR,
				   --sched.PAT_ENC_CSN_ID,
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
                   --sched.CHECKIN_DTTM,
                   --sched.CHECKOUT_DTTM,
                   --sched.VISIT_END_DTTM,
                   --sched.CYCLE_TIME_MINUTES,
                   sched.DEPT_SPECIALTY_NAME,
                   sched.PROV_SPECIALTY_NAME,
                   sched.APPT_DTTM,
                   sched.ENC_TYPE_C,
                   sched.ENC_TYPE_TITLE,
                   sched.APPT_CONF_STAT_NAME,
                   sched.ZIP,
                   sched.APPT_CONF_DTTM,
                   --sched.SIGNIN_DTTM,
                   --sched.ARVL_LIST_REMOVE_DTTM,
                   --sched.ROOMED_DTTM,
                   --sched.NURSE_LEAVE_DTTM,
                   --sched.PHYS_ENTER_DTTM,
                   sched.CANCEL_REASON_NAME,
                   sched.SER_RPT_GRP_SIX,
                   sched.SER_RPT_GRP_EIGHT,
                   sched.F2F_Flag,
                   --sched.TIME_TO_ROOM_MINUTES,
                   --sched.TIME_IN_ROOM_MINUTES,
                   --sched.BEGIN_CHECKIN_DTTM,
                   --sched.PAGED_DTTM,
                   --sched.FIRST_ROOM_ASSIGN_DTTM,
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
                   --sched.RESCHED_APPT_CSN_ID,
                   sched.BILL_PROV_YN
			FROM #newpt newpt
			--FROM
			--(
			--	SELECT Appointment_Lag_Business_Days,
   --                    Last_APPT_DT,
   --                    Last_Appointment_Request_Date,
   --                    Last_Seq,
   --                    Last_APPT_STATUS_FLAG,
   --                    Last_APPT_STATUS_C,
			--		   Last_Appointment_Lag_Business_Days_from_Original,
			--		   Last_CANCEL_INITIATOR,
   --                    Original_CANCEL_INITIATOR,
   --                    Original_Appointment_Request_Date,
   --                    Appointment_Lag_Days,
   --                    NEW_TO_SPEC,
   --                    Appointment_Request_Date,
   --                    APPT_MADE_DTTM,
   --                    APPT_DT,
   --                    CANCEL_INITIATOR,
   --                    APPT_STATUS_C,
   --                    APPT_STATUS_FLAG,
   --                    APPT_SERIAL_NUM_COUNT,
   --                    Seq,
   --                    APPT_SERIAL_NUM,
   --                    PAT_ENC_CSN_ID,
   --                    Appointment_Lag_Business_Days_from_Original
			--	FROM #newpt
			--	WHERE APPT_SERIAL_NUM_COUNT = 1
			--	UNION ALL
			--	SELECT Appointment_Lag_Business_Days,
   --                    Last_APPT_DT,
   --                    Last_Appointment_Request_Date,
   --                    Last_Seq,
   --                    Last_APPT_STATUS_FLAG,
   --                    Last_APPT_STATUS_C,
			--		   Last_Appointment_Lag_Business_Days_from_Original,
			--		   Last_CANCEL_INITIATOR,
   --                    Original_CANCEL_INITIATOR,
   --                    Original_Appointment_Request_Date,
   --                    Appointment_Lag_Days,
   --                    NEW_TO_SPEC,
   --                    Appointment_Request_Date,
   --                    APPT_MADE_DTTM,
   --                    APPT_DT,
   --                    CANCEL_INITIATOR,
   --                    APPT_STATUS_C,
   --                    APPT_STATUS_FLAG,
   --                    APPT_SERIAL_NUM_COUNT,
   --                    Seq,
   --                    APPT_SERIAL_NUM,
   --                    PAT_ENC_CSN_ID,
   --                    Appointment_Lag_Business_Days_from_Original
			--	FROM #newpt
			--	WHERE (APPT_SERIAL_NUM_COUNT > 1 AND Seq = 1)
			--	OR (APPT_SERIAL_NUM_COUNT > 1 AND Seq > 1 AND Appointment_Lag_Business_Days_from_Original <= 6)
			--) newpt
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
					   --appts.APPT_STATUS_FLAG,
					   --appts.APPT_STATUS_C,
					   --appts.CANCEL_INITIATOR,
					   appts.CANCEL_REASON_C,
					   CAST(appts.IDENTITY_ID AS INTEGER) AS MRN_int,
					   appts.CONTACT_DATE,
					   --appts.APPT_DT,
					   --appts.PAT_ENC_CSN_ID,
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
					   --appts.CHECKIN_DTTM,
					   --appts.CHECKOUT_DTTM,
					   --appts.VISIT_END_DTTM,
					   --appts.CYCLE_TIME_MINUTES,
					   appts.DEPT_SPECIALTY_NAME,
					   appts.PROV_SPECIALTY_NAME,
					   appts.APPT_DTTM,
					   appts.ENC_TYPE_C,
					   appts.ENC_TYPE_TITLE,
					   appts.APPT_CONF_STAT_NAME,
					   appts.ZIP,
					   appts.APPT_CONF_DTTM,
					   --appts.SIGNIN_DTTM,
					   --appts.ARVL_LIST_REMOVE_DTTM,
					   --appts.ROOMED_DTTM,
					   --appts.NURSE_LEAVE_DTTM,
					   --appts.PHYS_ENTER_DTTM,
					   appts.CANCEL_REASON_NAME,
					   appts.SER_RPT_GRP_SIX,
					   appts.SER_RPT_GRP_EIGHT,
					   appts.F2F_Flag,
					   --appts.TIME_TO_ROOM_MINUTES,
					   --appts.TIME_IN_ROOM_MINUTES,
					   --appts.BEGIN_CHECKIN_DTTM,
					   --appts.PAGED_DTTM,
					   --appts.FIRST_ROOM_ASSIGN_DTTM,
					   appts.CANCEL_LEAD_HOURS,
					   appts.APPT_CANC_DTTM,
					   entryemp.EMPlye_Systm_Login AS Entry_UVaID,
					   cancemp.EMPlye_Systm_Login AS Canc_UVaID,
					   appts.PHONE_REM_STAT_NAME,
					   appts.CHANGE_DATE,
					   --appts.APPT_MADE_DTTM,
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
					   appts.BILL_PROV_YN--,
					   --ROW_NUMBER() OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.APPT_MADE_DTTM) AS Seq

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

				--WHERE
				--      (CASE
				--		   WHEN appts.ENTRY_DATE IS NULL THEN
				--			   appts.APPT_MADE_DATE
				--		   WHEN appts.ENTRY_DATE >= appts.APPT_MADE_DATE AND appts.CHANGE_DATE >= appts.APPT_MADE_DATE THEN
				--			   appts.APPT_MADE_DATE
				--		   WHEN appts.ENTRY_DATE < appts.CHANGE_DATE THEN
				--			   appts.ENTRY_DATE
				--		   ELSE
				--			   appts.CHANGE_DATE
				--	   END >= @locstartdate
				--  AND CASE
				--		   WHEN appts.ENTRY_DATE IS NULL THEN
				--			   appts.APPT_MADE_DATE
				--		   WHEN appts.ENTRY_DATE >= appts.APPT_MADE_DATE AND appts.CHANGE_DATE >= appts.APPT_MADE_DATE THEN
				--			   appts.APPT_MADE_DATE
				--		   WHEN appts.ENTRY_DATE < appts.CHANGE_DATE THEN
				--			   appts.ENTRY_DATE
				--		   ELSE
				--			   appts.CHANGE_DATE
				--	   END < @locenddate)
				--AND excl.DEPARTMENT_ID IS NULL
				      --excl.DEPARTMENT_ID IS NULL

			) AS sched
			ON sched.PAT_ENC_CSN_ID = newpt.PAT_ENC_CSN_ID
		) AS main
		--LEFT OUTER JOIN
		--(
  --          SELECT APPT_SERIAL_NUM,
		--	MAX(CASE WHEN VIS_NEW_TO_SPEC_YN = 'Y' THEN 1 ELSE 0 END) AS NEW_TO_SPEC--,
		--	--COUNT(*) AS APPT_SERIAL_NUM_COUNT
		--	FROM DS_HSDM_App.Stage.Scheduled_Appointment
		--	GROUP BY APPT_SERIAL_NUM
	 --   ) aggr
		--    ON aggr.APPT_SERIAL_NUM = main.APPT_SERIAL_NUM
		WHERE main.Seq = 1
    ) evnts
        --ON (date_dim.day_date = CAST(evnts.Appointment_Request_Date AS SMALLDATETIME))
        ON (date_dim.day_date = CAST(evnts.Original_Appointment_Request_Date AS SMALLDATETIME))
  --      ON evnts.Seq = 1
		--AND (date_dim.day_date = CAST(evnts.Original_Appointment_Request_Date AS SMALLDATETIME))

WHERE date_dim.day_date >= @locstartdate
      AND date_dim.day_date < @locenddate
	  --AND evnts.appt_event_New_to_Specialty = 1
	  --AND evnts.NEW_TO_SPEC = 1
	  --AND evnts.F2F_Flag = 1
	  --AND evnts.Seq = 1

--ORDER BY date_dim.day_date;

--SELECT day_date
--     , date_key
--	 , day_of_week_num
--	 , day_of_week
--	 , weekday_ind
--FROM DS_HSDW_Prod.Rptg.vwDim_Date
--ORDER BY day_date

--SELECT DISTINCT APPT_SERIAL_NUM
--INTO #metric2
--FROM #metric
--WHERE APPT_SERIAL_NUM_COUNT > 1
--AND AbleToAccess = 0

--SELECT *
--FROM #newpt newpt
--INNER JOIN #metric2 metric2
--ON metric2.APPT_SERIAL_NUM = newpt.APPT_SERIAL_NUM
----ORDER BY APPT_SERIAL_NUM_COUNT,
----         APPT_SERIAL_NUM,
----		 Seq
--ORDER BY newpt.APPT_SERIAL_NUM_COUNT,
--         newpt.APPT_SERIAL_NUM,
--		 newpt.Seq

SELECT *
--SELECT APPT_SERIAL_NUM,
--		PAT_ENC_CSN_ID,
--	    --Seq,
--        --APPT_SERIAL_NUM_COUNT,
--        APPT_STATUS_FLAG,
--        APPT_STATUS_C,
--        CANCEL_INITIATOR,
--        APPT_MADE_DTTM,
--        Appointment_Request_Date,
--        APPT_DT,
--        Appointment_Lag_Days,
--        Appointment_Lag_Business_Days,
--		AbleToAccess,
--        --Original_Appointment_Request_Date,
--  --      Last_APPT_STATUS_FLAG,
--  --      Last_Appointment_Request_Date,
--  --      Last_APPT_DT,
--  --      Appointment_Lag_Business_Days_from_Original,
--		--Last_Appointment_Lag_Business_Days_from_Original,
--		--Last_CANCEL_INITIATOR
--        Resch_APPT_STATUS_FLAG,
--        Resch_Appointment_Request_Date,
--        Resch_APPT_DT,
--		Resch_Appointment_Lag_Business_Days_from_Initial_Request,
--		Resch_CANCEL_INITIATOR
--INTO #metric2
FROM #metric metric
--WHERE
--	metric.Original_Appointment_Request_Date BETWEEN @locstartdate AND @locenddate
--WHERE APPT_SERIAL_NUM_COUNT > 1
ORDER BY event_count DESC
        ,AbleToAccess DESC
        ,event_date
		,PAT_ENC_CSN_ID
		--,APPT_SERIAL_NUM
		--,APPT_MADE_DTTM

--SELECT *
--FROM #metric2
----WHERE Seq = 1
--ORDER BY AbleToAccess DESC
--        --,APPT_SERIAL_NUM_COUNT
--	    ,APPT_SERIAL_NUM
--		--,Seq

/*
SELECT COUNT(DISTINCT metric.APPT_SERIAL_NUM) AS New_Patient_Appointments_Scheduled
FROM #metric metric
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND (metric.Appointment_Lag_Days >= 0)

SELECT COUNT(*) AS New_Patient_Appointments_Scheduled_Within_7_Days
FROM #metric metric
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND metric.APPT_SERIAL_NUM_COUNT = 1
AND ((metric.Appointment_Lag_Days >= 0) AND (metric.Appointment_Lag_Business_Days <= 6))
AND ((metric.CANCEL_REASON_NAME IS NULL) OR (metric.CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))

SELECT APPT_STATUS_FLAG
      ,COUNT(*) AS New_Patient_Appointments_Scheduled_Within_7_Days
FROM #metric metric
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND metric.APPT_SERIAL_NUM_COUNT = 1
AND ((metric.Appointment_Lag_Days >= 0) AND (metric.Appointment_Lag_Business_Days <= 6))
AND ((metric.CANCEL_REASON_NAME IS NULL) OR (metric.CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))
GROUP BY metric.APPT_STATUS_FLAG
ORDER BY metric.APPT_STATUS_FLAG

SELECT APPT_STATUS_FLAG
      ,COUNT(*) AS New_Patients_Seen_Within_7_Days
FROM #metric metric
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND metric.APPT_SERIAL_NUM_COUNT = 1
AND metric.APPT_STATUS_FLAG IN ('Completed','Arrived')
AND ((metric.Appointment_Lag_Days >= 0) AND (metric.Appointment_Lag_Business_Days <= 6))
GROUP BY metric.APPT_STATUS_FLAG
ORDER BY metric.APPT_STATUS_FLAG

SELECT metric.CANCEL_INITIATOR
      ,COUNT(*) AS New_Patients_Scheduled_Within_7_Days_Canceled
FROM #metric metric
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND metric.APPT_SERIAL_NUM_COUNT = 1
AND metric.APPT_STATUS_C = 3
AND ((metric.Appointment_Lag_Days >= 0) AND (metric.Appointment_Lag_Business_Days <= 6))
AND metric.CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')
GROUP BY CANCEL_INITIATOR
ORDER BY CANCEL_INITIATOR

SELECT metric.CANCEL_INITIATOR
      ,metric.CANCEL_REASON_NAME
      ,COUNT(*) AS New_Patients_Scheduled_Within_7_Days_Canceled
FROM #metric metric
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND metric.APPT_SERIAL_NUM_COUNT = 1
AND metric.APPT_STATUS_C = 3
AND ((metric.Appointment_Lag_Days >= 0) AND (metric.Appointment_Lag_Business_Days <= 6))
AND metric.CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')
GROUP BY CANCEL_INITIATOR
        ,CANCEL_REASON_NAME
ORDER BY CANCEL_INITIATOR
        ,CANCEL_REASON_NAME

SELECT APPT_STATUS_FLAG,
       APPT_STATUS_C,
	   APPT_SERIAL_NUM,
       APPT_MADE_DATE,
	   event_type,
       epic_department_name,
       event_count,
	   event_date,
       ENTRY_DATE,
       CHANGE_DATE,
	   Appointment_Request_Date,
       APPT_DT,
	   Appointment_Lag_Days,
	   Appointment_Lag_Business_Days,
	   APPT_CANC_DTTM,
	   CANCEL_INITIATOR,
	   CANCEL_REASON_NAME,
       CONTACT_DATE,
       PAT_ENC_CSN_ID,
	   RESCHED_APPT_CSN_ID,
       VIS_NEW_TO_SPEC_YN,
	   BILL_PROV_YN,
	   NEW_TO_SPEC,
	   APPT_SERIAL_NUM_COUNT
FROM #metric metric
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND metric.APPT_SERIAL_NUM_COUNT = 1
AND metric.APPT_STATUS_FLAG NOT IN ('Completed','Arrived')
--AND metric.APPT_STATUS_C = 3
AND metric.APPT_STATUS_C <> 3
AND ((metric.Appointment_Lag_Days >= 0) AND (metric.Appointment_Lag_Business_Days <= 6))
ORDER BY APPT_STATUS_FLAG
       , APPT_SERIAL_NUM
       , APPT_MADE_DTTM
*/
/*
SELECT APPT_STATUS_FLAG,
       APPT_STATUS_C,
	   APPT_SERIAL_NUM,
	   Seq,
	   Appointment_Request_Date,
	   Original_Appointment_Request_Date,
       APPT_DT,
	   Appointment_Lag_Days,
	   Appointment_Lag_Business_Days,
       (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND day_date >= metric3.Original_Appointment_Request_Date AND day_date < APPT_DT) Appointment_Lag_Business_Days_From_Original,
	   APPT_SERIAL_NUM_COUNT,
	   APPT_CANC_DTTM,
	   CANCEL_INITIATOR,
	   CANCEL_REASON_NAME,
	   Original_CANCEL_REASON_NAME,
       APPT_MADE_DATE,
	   APPT_MADE_DTTM,
	   event_type,
       epic_department_name,
       event_count,
	   event_date,
       ENTRY_DATE,
       CHANGE_DATE,
       CONTACT_DATE,
       PAT_ENC_CSN_ID,
	   RESCHED_APPT_CSN_ID,
       VIS_NEW_TO_SPEC_YN,
	   BILL_PROV_YN,
	   NEW_TO_SPEC
INTO #metric2
FROM
(
SELECT APPT_STATUS_FLAG,
       APPT_STATUS_C,
	   metric.APPT_SERIAL_NUM,
	   Seq,
	   Appointment_Request_Date,
	   metric2.Original_Appointment_Request_Date,
       APPT_DT,
	   Appointment_Lag_Days,
	   Appointment_Lag_Business_Days,
	   APPT_SERIAL_NUM_COUNT,
	   APPT_CANC_DTTM,
	   CANCEL_INITIATOR,
	   CANCEL_REASON_NAME,
	   metric2.Original_CANCEL_REASON_NAME,
       APPT_MADE_DATE,
	   APPT_MADE_DTTM,
	   event_type,
       epic_department_name,
       event_count,
	   event_date,
       ENTRY_DATE,
       CHANGE_DATE,
       CONTACT_DATE,
       PAT_ENC_CSN_ID,
	   RESCHED_APPT_CSN_ID,
       VIS_NEW_TO_SPEC_YN,
	   BILL_PROV_YN,
	   NEW_TO_SPEC
FROM #metric metric
LEFT OUTER JOIN
(
SELECT APPT_SERIAL_NUM
      ,MAX(CASE WHEN Seq = 1 THEN Appointment_Request_Date ELSE '1/1/1900 00:00 AM' END) AS Original_Appointment_Request_Date
      ,MAX(CASE WHEN Seq = 1 THEN CANCEL_REASON_NAME ELSE '' END) AS Original_CANCEL_REASON_NAME
FROM #metric
WHERE APPT_SERIAL_NUM IS NOT NULL
AND APPT_SERIAL_NUM_COUNT > 1
AND Appointment_Lag_Days >= 0
GROUP BY APPT_SERIAL_NUM
) metric2
ON metric2.APPT_SERIAL_NUM = metric.APPT_SERIAL_NUM
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND metric.APPT_SERIAL_NUM_COUNT > 1
AND metric.Appointment_Lag_Days >= 0
) metric3

--SELECT APPT_STATUS_FLAG,
--       APPT_STATUS_C,
--       APPT_SERIAL_NUM,
--       Appointment_Request_Date,
--       Original_Appointment_Request_Date,
--       APPT_DT,
--       Appointment_Lag_Days,
--       Appointment_Lag_Business_Days,
--       Appointment_Lag_Business_Days_From_Original,
--       APPT_SERIAL_NUM_COUNT,
--       APPT_CANC_DTTM,
--       CANCEL_INITIATOR,
--       CANCEL_REASON_NAME,
--       APPT_MADE_DATE,
--       APPT_MADE_DTTM,
--       event_type,
--       epic_department_name,
--       event_count,
--       event_date,
--       ENTRY_DATE,
--       CHANGE_DATE,
--       CONTACT_DATE,
--       PAT_ENC_CSN_ID,
--       RESCHED_APPT_CSN_ID,
--       VIS_NEW_TO_SPEC_YN,
--       BILL_PROV_YN,
--       NEW_TO_SPEC
--FROM #metric2
--WHERE Seq = 1
--AND Appointment_Lag_Business_Days_From_Original <= 6
----ORDER BY APPT_SERIAL_NUM
----       , APPT_MADE_DTTM
--ORDER BY APPT_SERIAL_NUM

SELECT metric4.APPT_STATUS_FLAG,
       metric4.APPT_STATUS_C,
       metric4.APPT_SERIAL_NUM,
	   metric4.Seq,
       metric4.Appointment_Request_Date,
       metric4.Original_Appointment_Request_Date,
       metric4.APPT_DT,
       metric4.Appointment_Lag_Days,
       metric4.Appointment_Lag_Business_Days,
       metric4.Appointment_Lag_Business_Days_From_Original,
	   metric4.Min_Appointment_Lag_Business_Days_From_Original,
       metric4.APPT_SERIAL_NUM_COUNT,
       metric4.APPT_CANC_DTTM,
       metric4.CANCEL_INITIATOR,
       metric4.CANCEL_REASON_NAME,
	   metric4.Original_CANCEL_REASON_NAME,
       metric4.APPT_MADE_DATE,
       metric4.APPT_MADE_DTTM,
       metric4.event_type,
       metric4.epic_department_name,
       metric4.event_count,
       metric4.event_date,
       metric4.ENTRY_DATE,
       metric4.CHANGE_DATE,
       metric4.CONTACT_DATE,
       metric4.PAT_ENC_CSN_ID,
       metric4.RESCHED_APPT_CSN_ID,
       metric4.VIS_NEW_TO_SPEC_YN,
       metric4.BILL_PROV_YN,
       metric4.NEW_TO_SPEC
INTO #metric3
FROM
(
SELECT metric2.APPT_STATUS_FLAG,
       metric2.APPT_STATUS_C,
       metric2.APPT_SERIAL_NUM,
	   metric2.Seq,
       metric2.Appointment_Request_Date,
       metric2.Original_Appointment_Request_Date,
       metric2.APPT_DT,
       metric2.Appointment_Lag_Days,
       metric2.Appointment_Lag_Business_Days,
       metric2.Appointment_Lag_Business_Days_From_Original,
	   metric3.Min_Appointment_Lag_Business_Days_From_Original,
       metric2.APPT_SERIAL_NUM_COUNT,
       metric2.APPT_CANC_DTTM,
       metric2.CANCEL_INITIATOR,
       metric2.CANCEL_REASON_NAME,
	   metric2.Original_CANCEL_REASON_NAME,
       metric2.APPT_MADE_DATE,
       metric2.APPT_MADE_DTTM,
       metric2.event_type,
       metric2.epic_department_name,
       metric2.event_count,
       metric2.event_date,
       metric2.ENTRY_DATE,
       metric2.CHANGE_DATE,
       metric2.CONTACT_DATE,
       metric2.PAT_ENC_CSN_ID,
       metric2.RESCHED_APPT_CSN_ID,
       metric2.VIS_NEW_TO_SPEC_YN,
       metric2.BILL_PROV_YN,
       metric2.NEW_TO_SPEC
FROM #metric2 metric2
LEFT OUTER JOIN
(
SELECT APPT_SERIAL_NUM
     , MIN(Appointment_Lag_Business_Days_From_Original) AS Min_Appointment_Lag_Business_Days_From_Original
FROM #metric2
GROUP BY APPT_SERIAL_NUM
) metric3
ON metric3.APPT_SERIAL_NUM = metric2.APPT_SERIAL_NUM
) metric4
WHERE metric4.Min_Appointment_Lag_Business_Days_From_Original <= 6

SELECT metric.APPT_STATUS_FLAG,
       metric.APPT_STATUS_C,
       metric.APPT_SERIAL_NUM,
	   metric.Seq,
       metric.Appointment_Request_Date,
       metric.Original_Appointment_Request_Date,
       metric.APPT_DT,
       metric.Appointment_Lag_Days,
       metric.Appointment_Lag_Business_Days,
       metric.Appointment_Lag_Business_Days_From_Original,
	   metric4.APPT_STATUS_FLAG_LAST,
	   metric4.Seq_LAST,
	   metric4.Appointment_Request_Date_LAST,
	   metric4.APPT_DT_LAST,
	   metric4.Appointment_Lag_Business_Days_From_Original_LAST,
	   LEAD(metric.APPT_STATUS_C) OVER (PARTITION BY metric.APPT_SERIAL_NUM ORDER BY Seq) NEXT_APPT_STATUS_C,
	   LEAD(metric.APPT_STATUS_FLAG) OVER (PARTITION BY metric.APPT_SERIAL_NUM ORDER BY Seq) NEXT_APPT_STATUS_FLAG,
	   LEAD(metric.Appointment_Request_Date) OVER (PARTITION BY metric.APPT_SERIAL_NUM ORDER BY Seq) NEXT_Appointment_Request_Date,
	   LEAD(metric.APPT_DT) OVER (PARTITION BY metric.APPT_SERIAL_NUM ORDER BY Seq) NEXT_APPT_DT,
	   LEAD(metric.Appointment_Lag_Business_Days_From_Original) OVER (PARTITION BY metric.APPT_SERIAL_NUM ORDER BY Seq) NEXT_Appointment_Lag_Business_Days_From_Original,
	   metric.Min_Appointment_Lag_Business_Days_From_Original,
       metric.APPT_SERIAL_NUM_COUNT,
       metric.APPT_CANC_DTTM,
       metric.CANCEL_INITIATOR,
       metric.CANCEL_REASON_NAME,
	   metric.Original_CANCEL_REASON_NAME,
       metric.APPT_MADE_DATE,
       metric.APPT_MADE_DTTM,
       metric.event_type,
       metric.epic_department_name,
       metric.event_count,
       metric.event_date,
       metric.ENTRY_DATE,
       metric.CHANGE_DATE,
       metric.CONTACT_DATE,
       metric.PAT_ENC_CSN_ID,
       metric.RESCHED_APPT_CSN_ID,
       metric.VIS_NEW_TO_SPEC_YN,
       metric.BILL_PROV_YN,
       metric.NEW_TO_SPEC
INTO #metric4
--SELECT DISTINCT
--	APPT_SERIAL_NUM
FROM #metric3 metric
--WHERE Original_CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')
LEFT OUTER JOIN
(
SELECT APPT_SERIAL_NUM
     , MAX(CASE WHEN APPT_STATUS_C IN (2,6) THEN APPT_STATUS_C ELSE NULL END) AS APPT_STATUS_C_LAST
     , MAX(CASE WHEN APPT_STATUS_C IN (2,6) THEN APPT_STATUS_FLAG ELSE NULL END) AS APPT_STATUS_FLAG_LAST
     , MAX(CASE WHEN APPT_STATUS_C IN (2,6) THEN Seq ELSE NULL END) AS Seq_LAST
     , MAX(CASE WHEN APPT_STATUS_C IN (2,6) THEN Appointment_Request_Date ELSE NULL END) AS Appointment_Request_Date_LAST
     , MAX(CASE WHEN APPT_STATUS_C IN (2,6) THEN APPT_DT ELSE NULL END) AS APPT_DT_LAST
     , MAX(CASE WHEN APPT_STATUS_C IN (2,6) THEN Appointment_Lag_Business_Days_From_Original ELSE NULL END) AS Appointment_Lag_Business_Days_From_Original_LAST
FROM #metric3
GROUP BY APPT_SERIAL_NUM
) metric4
ON metric4.APPT_SERIAL_NUM = metric.APPT_SERIAL_NUM

SELECT *
FROM #metric4
--WHERE Seq = 1
ORDER BY APPT_SERIAL_NUM

SELECT *
FROM #metric4
WHERE Seq = 1
AND Appointment_Lag_Business_Days >= 7
AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
AND Appointment_Lag_Business_Days_From_Original_LAST <= 6
ORDER BY APPT_SERIAL_NUM

--SELECT COUNT(*) AS APPT_SERIAL_NUM_COUNT_GT_1
--FROM #metric4
--WHERE Seq = 1

--SELECT CANCEL_INITIATOR
--      ,COUNT(*) AS APPT_SERIAL_NUM_COUNT_GT_1
--FROM #metric4
--WHERE Seq = 1
--GROUP BY CANCEL_INITIATOR
--ORDER BY CANCEL_INITIATOR

--SELECT COUNT(*) AS APPT_SERIAL_NUM_COUNT_GT_1_PROVIDER_INITIATED
--FROM #metric4
--WHERE Seq = 1
--AND CANCEL_INITIATOR = 'PROVIDER'

--SELECT *
--FROM #metric4
--WHERE Seq = 1
--AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
--AND CANCEL_INITIATOR = 'PROVIDER'
----ORDER BY metric.APPT_SERIAL_NUM
----       , metric.APPT_MADE_DTTM
--ORDER BY Appointment_Lag_Business_Days_From_Original_LAST
--       , APPT_SERIAL_NUM
----ORDER BY APPT_SERIAL_NUM

--SELECT COUNT(*) AS APPT_SERIAL_NUM_COUNT_GT_1_PATIENT_INITIATED
--FROM #metric4
--WHERE Seq = 1
--AND CANCEL_INITIATOR = 'PATIENT'

--SELECT *
--FROM #metric4
--WHERE Seq = 1
--AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
--AND CANCEL_INITIATOR = 'PATIENT'
----ORDER BY metric.APPT_SERIAL_NUM
----       , metric.APPT_MADE_DTTM
--ORDER BY Appointment_Lag_Business_Days_From_Original_LAST
--       , APPT_SERIAL_NUM
----ORDER BY APPT_SERIAL_NUM

--SELECT *
--FROM #metric4
--WHERE Seq = 1
--AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
--AND CANCEL_INITIATOR = 'PATIENT'
--AND Appointment_Lag_Business_Days_From_Original_LAST > 6
----ORDER BY metric.APPT_SERIAL_NUM
----       , metric.APPT_MADE_DTTM
--ORDER BY Appointment_Lag_Business_Days
--       , APPT_SERIAL_NUM
----ORDER BY APPT_SERIAL_NUM

--SELECT COUNT(*) AS APPT_SERIAL_NUM_COUNT_GT_1_OTHER_INITIATED
--FROM #metric4
--WHERE Seq = 1
--AND ((CANCEL_INITIATOR IS NULL) OR (CANCEL_INITIATOR = 'OTHER'))

--SELECT *
--FROM #metric4
--WHERE Seq = 1
--AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
--AND ((CANCEL_INITIATOR IS NULL) OR (CANCEL_INITIATOR = 'OTHER'))
----ORDER BY metric.APPT_SERIAL_NUM
----       , metric.APPT_MADE_DTTM
----ORDER BY Appointment_Lag_Business_Days_From_Original_LAST
----       , APPT_SERIAL_NUM
--ORDER BY APPT_SERIAL_NUM
*/
/*
SELECT APPT_STATUS_FLAG,
       APPT_STATUS_C,
	   APPT_SERIAL_NUM,
	   ROW_NUMBER() OVER (PARTITION BY APPT_SERIAL_NUM ORDER BY APPT_MADE_DTTM) AS Seq,
	   Appointment_Request_Date,
       APPT_DT,
	   Appointment_Lag_Days,
	   Appointment_Lag_Business_Days,
	   MIN_Appointment_Lag_Business_Days,
	   APPT_SERIAL_NUM_COUNT,
	   APPT_CANC_DTTM,
	   CANCEL_INITIATOR,
	   CANCEL_REASON_NAME,
       APPT_MADE_DATE,
	   APPT_MADE_DTTM,
	   event_type,
       epic_department_name,
       event_count,
	   event_date,
       ENTRY_DATE,
       CHANGE_DATE,
       CONTACT_DATE,
       PAT_ENC_CSN_ID,
	   RESCHED_APPT_CSN_ID,
       VIS_NEW_TO_SPEC_YN,
	   BILL_PROV_YN,
	   NEW_TO_SPEC
INTO #metric4
FROM
(
SELECT APPT_STATUS_FLAG,
       APPT_STATUS_C,
	   metric.APPT_SERIAL_NUM,
       APPT_MADE_DATE,
	   APPT_MADE_DTTM,
	   APPT_SERIAL_NUM_COUNT,
	   event_type,
       epic_department_name,
       event_count,
	   event_date,
       ENTRY_DATE,
       CHANGE_DATE,
	   Appointment_Request_Date,
       APPT_DT,
	   Appointment_Lag_Days,
	   Appointment_Lag_Business_Days,
	   metric2.MIN_Appointment_Lag_Business_Days,
	   APPT_CANC_DTTM,
	   CANCEL_INITIATOR,
	   CANCEL_REASON_NAME,
       CONTACT_DATE,
       PAT_ENC_CSN_ID,
	   RESCHED_APPT_CSN_ID,
       VIS_NEW_TO_SPEC_YN,
	   BILL_PROV_YN,
	   NEW_TO_SPEC
FROM #metric metric
LEFT OUTER JOIN
(
SELECT APPT_SERIAL_NUM
      ,MIN(CASE WHEN Appointment_Lag_Days >= 0 THEN Appointment_Lag_Business_Days ELSE 7 END) AS MIN_Appointment_Lag_Business_Days
FROM #metric
WHERE APPT_SERIAL_NUM IS NOT NULL
AND APPT_SERIAL_NUM_COUNT > 1
GROUP BY APPT_SERIAL_NUM
) metric2
ON metric2.APPT_SERIAL_NUM = metric.APPT_SERIAL_NUM
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
AND metric.APPT_SERIAL_NUM_COUNT > 1
) metric3
WHERE metric3.MIN_Appointment_Lag_Business_Days <= 6
--AND metric.APPT_STATUS_FLAG NOT IN ('Completed','Arrived')
--AND metric.APPT_STATUS_C = 3
--AND metric.APPT_STATUS_C <> 3
--AND ((metric.Appointment_Lag_Days >= 0) AND (metric.Appointment_Lag_Business_Days <= 6))

--SELECT *
--FROM #metric4
----ORDER BY APPT_STATUS_FLAG
----       , APPT_SERIAL_NUM
----       , APPT_MADE_DTTM
--ORDER BY APPT_SERIAL_NUM_COUNT
--       , APPT_SERIAL_NUM
--       , APPT_MADE_DTTM

SELECT metric.APPT_STATUS_FLAG
     , metric.APPT_STATUS_C
	 , metric.APPT_SERIAL_NUM
	 , metric.Seq
	 , metric.Appointment_Request_Date
	 , metric.APPT_DT
	 , metric.Appointment_Lag_Business_Days
	 , metric.APPT_SERIAL_NUM_COUNT
	 , metric.CANCEL_INITIATOR
	 , metric.CANCEL_REASON_NAME
     , metric2.APPT_STATUS_C_LAST,
       metric2.APPT_STATUS_FLAG_LAST,
       metric2.Seq_LAST,
       metric2.Appointment_Request_Date_LAST,
       metric2.APPT_DT_LAST,
       metric2.Appointment_Lag_Business_Days_LAST,
       (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND day_date >= metric.Appointment_Request_Date AND day_date < metric2.APPT_DT_LAST) Appointment_Lag_Business_Days_FINAL
INTO #metric5
FROM #metric4 metric
LEFT OUTER JOIN
(
SELECT APPT_SERIAL_NUM
     , MAX(CASE WHEN APPT_STATUS_C <> 3 THEN APPT_STATUS_C ELSE NULL END) AS APPT_STATUS_C_LAST
     , MAX(CASE WHEN APPT_STATUS_C <> 3 THEN APPT_STATUS_FLAG ELSE NULL END) AS APPT_STATUS_FLAG_LAST
     , MAX(CASE WHEN APPT_STATUS_C <> 3 THEN Seq ELSE NULL END) AS Seq_LAST
     , MAX(CASE WHEN APPT_STATUS_C <> 3 THEN Appointment_Request_Date ELSE NULL END) AS Appointment_Request_Date_LAST
     , MAX(CASE WHEN APPT_STATUS_C <> 3 THEN APPT_DT ELSE NULL END) AS APPT_DT_LAST
     , MAX(CASE WHEN APPT_STATUS_C <> 3 THEN Appointment_Lag_Business_Days ELSE NULL END) AS Appointment_Lag_Business_Days_LAST
FROM #metric4
GROUP BY APPT_SERIAL_NUM
) metric2
ON metric2.APPT_SERIAL_NUM = metric.APPT_SERIAL_NUM

SELECT *
FROM #metric5
WHERE Seq = 1
AND ((CANCEL_REASON_NAME IS NULL) OR (CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))
ORDER BY APPT_SERIAL_NUM_COUNT
       , APPT_SERIAL_NUM

--SELECT CANCEL_INITIATOR
--      ,COUNT(*) AS Freq
--FROM #metric5
--WHERE Seq = 1
--AND ((CANCEL_REASON_NAME IS NULL) OR (CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))
--AND APPT_STATUS_FLAG IN ('Canceled','Canceled Late')
----AND CANCEL_INITIATOR = 'PROVIDER'
----AND APPT_STATUS_FLAG_LAST = 'Completed'
--GROUP BY CANCEL_INITIATOR
--ORDER BY CANCEL_INITIATOR

--SELECT *
--FROM #metric5
--WHERE Seq = 1
--AND ((CANCEL_REASON_NAME IS NULL) OR (CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))
--AND APPT_STATUS_FLAG IN ('Canceled','Canceled Late')
--AND CANCEL_INITIATOR = 'PROVIDER'
--AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
--ORDER BY Appointment_Lag_Business_Days_FINAL
--       , APPT_SERIAL_NUM

--SELECT *
--FROM #metric5
--WHERE Seq = 1
--AND ((CANCEL_REASON_NAME IS NULL) OR (CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))
--AND APPT_STATUS_FLAG IN ('Canceled','Canceled Late')
--AND CANCEL_INITIATOR = 'PATIENT'
--AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
--ORDER BY Appointment_Lag_Business_Days_FINAL
--       , APPT_SERIAL_NUM

SELECT *
FROM #metric5
WHERE Seq = 1
AND ((CANCEL_REASON_NAME IS NULL) OR (CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))
AND APPT_STATUS_FLAG IN ('Canceled','Canceled Late')
AND CANCEL_INITIATOR = 'PATIENT'
AND ((APPT_STATUS_FLAG_LAST IS NULL) OR (APPT_STATUS_FLAG_LAST NOT IN ('Completed','Arrived')))
--ORDER BY APPT_STATUS_FLAG_LAST
--       , Appointment_Lag_Business_Days
--       , APPT_SERIAL_NUM
ORDER BY Appointment_Lag_Business_Days
       , APPT_STATUS_FLAG_LAST
       , APPT_SERIAL_NUM

--SELECT *
--FROM #metric5
--WHERE Seq = 1
--AND ((CANCEL_REASON_NAME IS NULL) OR (CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))
--AND APPT_STATUS_FLAG IN ('Canceled','Canceled Late')
--AND CANCEL_INITIATOR = 'PATIENT'
--AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
--AND Appointment_Lag_Business_Days_FINAL > 6
--ORDER BY Appointment_Lag_Business_Days
--       , APPT_SERIAL_NUM

--SELECT *
--FROM #metric5
--WHERE Seq = 1
--AND ((CANCEL_REASON_NAME IS NULL) OR (CANCEL_REASON_NAME NOT IN ('Appointment Scheduled in Error','Error')))
--AND APPT_STATUS_FLAG IN ('Canceled','Canceled Late')
--AND CANCEL_INITIATOR = 'OTHER'
--AND APPT_STATUS_FLAG_LAST IN ('Completed','Arrived')
--ORDER BY Appointment_Lag_Business_Days_FINAL
--       , APPT_SERIAL_NUM
*/
/*
SELECT event_type,
       epic_department_name,
       event_count,
	   event_date,
       APPT_MADE_DATE,
       ENTRY_DATE,
       CHANGE_DATE,
	   Appointment_Request_Date,
       APPT_DT,
	   Appointment_Lag_Days,
	   Appointment_Lag_Business_Days,
	   --ddte1.day_of_week AS APPT_MADE_DATE_dow,
	   --ddte2.day_of_week AS ENTRY_DATE_dow,
	   --ddte3.day_of_week AS CHANGE_DATE_dow,
	   --ddte4.day_of_week AS APPT_DT_dow,
       APPT_STATUS_FLAG,
       APPT_STATUS_C,
	   APPT_CANC_DTTM,
	   CANCEL_INITIATOR,
       CONTACT_DATE,
       PAT_ENC_CSN_ID,
	   APPT_SERIAL_NUM,
	   RESCHED_APPT_CSN_ID,
       VIS_NEW_TO_SPEC_YN,
	   BILL_PROV_YN,
	   NEW_TO_SPEC,
	   --Seq
	   APPT_SERIAL_NUM_COUNT

--SELECT metric.event_type,
--       metric.epic_department_name,
--       metric.event_count,
--	   metric.event_date,
--       appt.APPT_MADE_DATE,
--       appt.ENTRY_DATE,
--       appt.CHANGE_DATE,
--	   --metric.Appointment_Request_Date,
--	   CASE
--		   WHEN appt.ENTRY_DATE IS NULL THEN
--			   appt.APPT_MADE_DATE
--		   WHEN appt.ENTRY_DATE >= appt.APPT_MADE_DATE AND appt.CHANGE_DATE >= appt.APPT_MADE_DATE THEN
--			   appt.APPT_MADE_DATE
--		   WHEN appt.ENTRY_DATE < appt.CHANGE_DATE THEN
--			   appt.ENTRY_DATE
--		   ELSE
--			   appt.CHANGE_DATE
--		END AS Appointment_Request_Date,
--       appt.APPT_DT,
--	   --metric.Appointment_Lag_Days,
--	   --metric.Appointment_Lag_Business_Days,
--       appt.APPT_STATUS_FLAG,
--       appt.APPT_STATUS_C,
--	   appt.APPT_CANC_DTTM,
--	   appt.CANCEL_INITIATOR,
--       --appt.CONTACT_DATE,
--       appt.PAT_ENC_CSN_ID,
--	   appt.APPT_SERIAL_NUM,
--	   appt.RESCHED_APPT_CSN_ID,
--       appt.VIS_NEW_TO_SPEC_YN,
--	   appt.BILL_PROV_YN
--SELECT *
FROM #metric metric
--FROM DS_HSDM_App.Stage.Scheduled_Appointment appt
--LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte1
--ON ddte1.day_date = metric.APPT_MADE_DATE
--LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte2
--ON ddte2.day_date = metric.ENTRY_DATE
--LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte3
--ON ddte3.day_date = metric.CHANGE_DATE
--LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte4
--ON ddte4.day_date = metric.APPT_DT
--LEFT OUTER JOIN DS_HSDM_App.Stage.Scheduled_Appointment appt
--ON appt.APPT_SERIAL_NUM = metric.APPT_SERIAL_NUM
--INNER JOIN #metric metric
--ON appt.APPT_SERIAL_NUM = metric.APPT_SERIAL_NUM
--WHERE appt_event_Completed = 1
--AND appt_event_New_to_Specialty = 1
--AND Appointment_Lag_Days >= 0
--AND ((ddte1.day_of_week IN ('Saturday','Sunday'))
--OR (ddte2.day_of_week IN ('Saturday','Sunday'))
--OR (ddte3.day_of_week IN ('Saturday','Sunday'))
--OR (ddte4.day_of_week IN ('Saturday','Sunday')))
WHERE metric.APPT_SERIAL_NUM IS NOT NULL
--ORDER BY APPT_SERIAL_NUM
ORDER BY APPT_SERIAL_NUM
       , APPT_MADE_DTTM
--ORDER BY event_date
--ORDER BY APPT_DT_dow
--ORDER BY ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY event_date) desc
*/
/*
SELECT
	APPT_SERIAL_NUM
  , MAX(CASE WHEN RESCHED_APPT_CSN_ID IS NOT NULL THEN 1 ELSE 0 END) AS Rescheduled
  , MIN(APPT_MADE_DTTM) AS Min_APPT_MADE_DTTM
INTO #canclate
FROM #metric
WHERE appt_event_Canceled_Late = 1
--AND RESCHED_APPT_CSN_ID IS NOT NULL
GROUP BY APPT_SERIAL_NUM

SELECT PAT_ENC_CSN_ID
     , APPT_MADE_DTTM
	 , Min_APPT_MADE_DTTM
     , #metric.APPT_SERIAL_NUM
	 , #canclate.Rescheduled
	 , RESCHED_APPT_CSN_ID
	 , event_date
	 , APPT_DTTM
	 , APPT_CANC_DTTM
	 , provider_id
	 , epic_department_id
	 , APPT_STATUS_C
	 , appt_event_Canceled
	 , appt_event_Canceled_Late
	 , appt_event_Provider_Canceled
	 , appt_event_Completed
	 , Cancel_Lead_Days
	 , CANCEL_REASON_NAME
	 , CANCEL_INITIATOR
	 --, CASE WHEN Rescheduled = 1 AND CANCEL_INITIATOR = 'PROVIDER' THEN ROW_NUMBER() OVER (PARTITION BY #metric.APPT_SERIAL_NUM ORDER BY APPT_MADE_DTTM) ELSE 0 END AS Seq
--SELECT DISTINCT
--       APPT_MADE_DTTM
--	 , #metric.APPT_SERIAL_NUM
--INTO #bumprsch
FROM #metric
INNER JOIN #canclate ON #metric.APPT_SERIAL_NUM = #canclate.APPT_SERIAL_NUM
--WHERE Appointment_Lag_Days >= 0 AND appt_event_New_to_Specialty = 1 AND appt_event_Completed = 1
--WHERE appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)
--WHERE appt_event_Completed = 1
--WHERE som_group_id IS NOT NULL
--WHERE ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) AND Rescheduled = 0)
--OR ((APPT_MADE_DTTM >= Min_APPT_MADE_DTTM) AND Rescheduled = 1)
WHERE (appt_event_Canceled_Late = 1 AND Rescheduled = 0)
OR ((APPT_MADE_DTTM >= Min_APPT_MADE_DTTM) AND Rescheduled = 1)
--WHERE ((APPT_MADE_DTTM >= Min_APPT_MADE_DTTM) AND Rescheduled = 1) AND (CANCEL_INITIATOR = 'PROVIDER')
--GROUP BY APPT_STATUS_FLAG
ORDER BY #canclate.Min_APPT_MADE_DTTM
       , #metric.APPT_SERIAL_NUM
       , #metric.APPT_MADE_DTTM
*/
/*
--SELECT
--	APPT_SERIAL_NUM
--  , MAX(CASE WHEN RESCHED_APPT_CSN_ID IS NOT NULL THEN 1 ELSE 0 END) AS Rescheduled
--  , MIN(APPT_MADE_DTTM) AS Min_APPT_MADE_DTTM
--INTO #bump
--FROM #metric
--WHERE (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)
----AND RESCHED_APPT_CSN_ID IS NOT NULL
--GROUP BY APPT_SERIAL_NUM
SELECT person_id
     , PAT_ENC_CSN_ID
     , #metric.APPT_SERIAL_NUM
	 , APPT_CANC_DTTM
     , APPT_MADE_DTTM
	 , APPT_DTTM
	 , APPT_STATUS_FLAG
	 , provider_id
	 , epic_department_id
	 , CANCEL_REASON_NAME
	 , CANCEL_INITIATOR
FROM #metric
WHERE APPT_SERIAL_NUM = 200008508343
ORDER BY person_id,
         PAT_ENC_CSN_ID

SELECT
	person_id
  , PAT_ENC_CSN_ID AS Bump_PAT_ENC_CSN_ID
  , APPT_DTTM AS Bump_APPT_DTTM
  , APPT_CANC_DTTM AS Bump_APPT_CANC_DTTM
INTO #bump
FROM #metric
WHERE (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)

SELECT #bump.person_id
     , PAT_ENC_CSN_ID
     , #metric.APPT_SERIAL_NUM
     , Bump_PAT_ENC_CSN_ID
	 , APPT_CANC_DTTM
     , APPT_MADE_DTTM
	 , Bump_APPT_DTTM
	 , APPT_DTTM
	 --, CAST(ROUND(CAST(DATEDIFF(HOUR, Bump_APPT_DTTM, APPT_DTTM) AS NUMERIC(7,2))/24.0,1) AS NUMERIC(4,1)) AS Days_To_Appointment
	 , CAST(ROUND(CAST(DATEDIFF(HOUR, Bump_APPT_DTTM, APPT_DTTM) AS NUMERIC(8,3))/24.0,2) AS NUMERIC(5,2)) AS Days_To_Appointment
	 , APPT_STATUS_FLAG
	 , provider_id
	 , epic_department_id
	 --, Min_APPT_MADE_DTTM
	 --, #bump.Rescheduled
	 , RESCHED_APPT_CSN_ID
	 , event_date
	 , appt_event_Canceled
	 , appt_event_Canceled_Late
	 , appt_event_Provider_Canceled
	 , appt_event_Completed
	 , Cancel_Lead_Days
	 , CANCEL_REASON_NAME
	 , CANCEL_INITIATOR
	 --, CASE WHEN Rescheduled = 1 AND CANCEL_INITIATOR = 'PROVIDER' THEN ROW_NUMBER() OVER (PARTITION BY #metric.APPT_SERIAL_NUM ORDER BY APPT_MADE_DTTM) ELSE 0 END AS Seq
--SELECT DISTINCT
--       APPT_MADE_DTTM
--	 , #metric.APPT_SERIAL_NUM
INTO #bumprsch
FROM #metric
--INNER JOIN #bump ON #metric.APPT_SERIAL_NUM = #bump.APPT_SERIAL_NUM
INNER JOIN #bump ON #metric.person_id = #bump.person_id
--WHERE Appointment_Lag_Days >= 0 AND appt_event_New_to_Specialty = 1 AND appt_event_Completed = 1
--WHERE appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)
--WHERE appt_event_Completed = 1
--WHERE som_group_id IS NOT NULL
--WHERE ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) AND Rescheduled = 0)
--OR ((APPT_MADE_DTTM >= Min_APPT_MADE_DTTM) AND Rescheduled = 1)
--WHERE ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) AND Rescheduled = 0)
--OR ((APPT_MADE_DTTM >= Min_APPT_MADE_DTTM) AND ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) AND Rescheduled = 1))
--WHERE ((APPT_MADE_DTTM >= Min_APPT_MADE_DTTM) AND Rescheduled = 1) AND (CANCEL_INITIATOR = 'PROVIDER')
WHERE ((#metric.PAT_ENC_CSN_ID >= #bump.Bump_PAT_ENC_CSN_ID)
       AND ((#metric.PAT_ENC_CSN_ID = #bump.Bump_PAT_ENC_CSN_ID) OR
	        ((#metric.PAT_ENC_CSN_ID <> #bump.Bump_PAT_ENC_CSN_ID) AND (#metric.APPT_MADE_DTTM >= #bump.Bump_APPT_CANC_DTTM))))
--GROUP BY APPT_STATUS_FLAG

SELECT *
FROM #bumprsch
--ORDER BY Appointment_Lag_Days DESC
--ORDER BY APPT_STATUS_FLAG
--ORDER BY COUNT(*) DESC
--ORDER BY ENC_TYPE_TITLE
--ORDER BY event_date,
--         PAT_ENC_CSN_ID
--ORDER BY w_financial_division_id,
--         event_date
--ORDER BY PAT_ENC_CSN_ID,
--         event_date
--ORDER BY APPT_SERIAL_NUM,
--         PAT_ENC_CSN_ID,
--         event_date
--ORDER BY APPT_SERIAL_NUM
--ORDER BY Rescheduled DESC,
--         APPT_SERIAL_NUM ,
--         PAT_ENC_CSN_ID,
--         event_date
ORDER BY person_id,
         Bump_PAT_ENC_CSN_ID,
         PAT_ENC_CSN_ID

--SELECT DISTINCT
--	APPT_SERIAL_NUM
--FROM #bumprsch
----WHERE Rescheduled = 1 AND CANCEL_INITIATOR = 'PROVIDER'
--WHERE Rescheduled = 1
*/
/*
SELECT aggr.BumpCount
      ,COUNT(*) AS Freq
FROM
(
SELECT COUNT(*) AS BumpCount
FROM #bumprsch
WHERE Rescheduled = 1
GROUP BY APPT_SERIAL_NUM
) aggr
GROUP BY aggr.BumpCount
ORDER BY aggr.BumpCount
*/

GO


