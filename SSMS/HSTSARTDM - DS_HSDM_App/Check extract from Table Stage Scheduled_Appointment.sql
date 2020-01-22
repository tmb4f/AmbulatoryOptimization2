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
--SET @startdate = '1/1/2018 00:00 AM'
SET @startdate = '7/1/2019 00:00 AM'
SET @enddate = '6/30/2020 11:59 PM'

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Scheduled_Appointment_Metric]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Scheduled_Appointment_Metric
--WHO : Tom Burgan
--WHEN: 5/7/18
--WHY : Report scheduled appointment metrics from Cadence.
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
-- Bump Rate
--				SUM((Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant') AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)
--              /
--              SUM((Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant') AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
--
-- Percentage of New Patient Visits
--				SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1) / SUM(appt_event_Completed = 1)
--
-- Average Lag Time to Appointment for New Patients in days
--				SUM(CASE WHEN (appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Days >= 0) THEN Appointment_Lag_Days ELSE 0 END)
--              /
--              SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Days >= 0)
--
-- Total Visits
--				SUM(appt_event_Completed = 1)
--
-- Average Visit Time
--				SUM(CASE WHEN (appt_event_Completed = 1 OR appt_event_Arrived = 1) THEN CYCLE_TIME_MINUTES_Adjusted ELSE 0 END)
--              /
--              SUM((appt_event_Completed = 1 OR appt_event_Arrived = 1) AND CYCLE_TIME_MINUTES_Adjusted >= 0)
--
-- Percentage of New Patients Seen Within 7 Business Days
--				SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Business_Days <= 6)
--              /
--              SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1)
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
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Scheduled_Appointment_Metric]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         05/07/2018 - TMB - create stored procedure
--         06/12/2018 - TMB - include columns added to the Stage.Scheduled_Appointment table
--         06/25/2018 - TMB - include columns added to the Stage.Scheduled_Appointment table; use CANCEL_INITIATOR
--                            to identify provider-initiated cancellations
--         06/27/2018 - TMB - include columns added to the Stage.Scheduled_Appointment table
--         07/06/2018 - TMB - use sk_Dim_Pt, sk_Fact_Pt_Acct, sk_Fact_Pt_Enc_Clrt, IDENTITY_ID (MRN) values populated
--                            in the loading of the Stage table.
--         07/12/2018 - TMB - add TIME_TO_ROOM_MINUTES, TIME_IN_ROOM_MINUTES, BEGIN_CHECKIN_DTTM, PAGED_DTTM, and FIRST_ROOM_ASSIGN_DTTM
--                            to TabRptg table; add logic to exclude departments; change Appointment_Lag_Days calculation;
--                            update Metric Calculations documentation
--         07/18/2018 - TMB - add CANCEL_LEAD_HOURS to TabRptg table
--         08/09/2018 - TMB - use IDENTITY_ID from staging table to set person_id value
--         08/17/2018 - TMB - add APPT_CANC_DTTM, Entry_UVaID, Canc_UVaID to TabRptg table
--         09/20/2018 - TMB - add PHONE_REM_STAT_NAME to TabRptg table
--         11/08/2018 - TMB - add CHANGE_DATE, Cancel_Lead_Days calculation to TabRptg table; update logic for calculating Appointment_Lag_Days;
--                            update Metric Calculations documentation
--         03/28/2019 - TMB - add APPT_MADE_DTTM, BUSINESS_UNIT, Prov_Typ, Staff_Resource, and the new standard portal columns
--
--         03/28/2019 - BDD     ---cast various columns as proper data type for portal tables removed w_ from new column names to match other portal processes.
--         04/05/2019 - TMB - correct statement setting value of Clrt_Financial_Division_Name
--         05/07/2019 - TMB - add logic for updated/new views Rptg.vwRef_Crosswalk_HSEntity_Prov and Rptg.vwRef_SOM_Hierarchy
--         05/10/2019 - TMB - edit logic to resolve issue resulting from multiple primary, active wd jobs for a provider;
--                            add place-holder columns for w_som_hs_area_id (smallint) and w_som_hs_area_name (VARCHAR(150))
--         07/09/2019 - TMB - change logic for setting SOM hierarchy values; add APPT_SERIAL_NUM, RESCHED_APPT_CSN_ID
--         07/26/2019 - TMB - add columns Appointment_Request_Date and Appointment_Lag_Business_Days
--         07/29/2019 - TMB - add column BILL_PROV_YN
--         08/07/2019 - TMB - edit Appointment_Lag_Business_Days calculation: exclude holidays from business days classification;
--                            change documentation defining Bump Rate calculation
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

IF OBJECT_ID('tempdb..#metric ') IS NOT NULL
DROP TABLE #metric

IF OBJECT_ID('tempdb..#events_list ') IS NOT NULL
DROP TABLE #events_list

--IF OBJECT_ID('tempdb..#metric2 ') IS NOT NULL
--DROP TABLE #metric2

SELECT CAST('Appointment' AS VARCHAR(50)) AS event_type,
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

---BDD 5/9/2018 per Sue, change these from Y/N varchar(1) to 1/0 ints. Null = 0 per Tom and Sue
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

--For tableau calc purposes null = 0 per Sue.
       ISNULL(evnts.appt_event_No_Show,CAST(0 AS INT)) AS appt_event_No_Show,
       ISNULL(evnts.appt_event_Canceled_Late,CAST(0 AS INT)) AS appt_event_Canceled_Late,
       ISNULL(evnts.appt_event_Canceled,CAST(0 AS INT)) AS appt_event_Canceled,
       ISNULL(evnts.appt_event_Scheduled,CAST(0 AS INT)) AS appt_event_Scheduled,
       ISNULL(evnts.appt_event_Provider_Canceled,CAST(0 AS INT)) AS appt_event_Provider_Canceled,
       ISNULL(evnts.appt_event_Completed,CAST(0 AS INT)) AS appt_event_Completed,
       ISNULL(evnts.appt_event_Arrived,CAST(0 AS INT)) AS appt_event_Arrived,
       ISNULL(evnts.appt_event_New_to_Specialty,CAST(0 AS INT)) AS appt_event_New_to_Specialty,
	   
       CASE
           WHEN (evnts.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd, evnts.Appointment_Request_Date, evnts.APPT_DT)
           ELSE CAST(NULL AS INT)
       END AS Appointment_Lag_Days,
       evnts.CYCLE_TIME_MINUTES_Adjusted,

	   evnts.DEPT_SPECIALTY_NAME,
	   evnts.PROV_SPECIALTY_NAME,
	   evnts.APPT_DTTM,
	   evnts.ENC_TYPE_C,
	   evnts.ENC_TYPE_TITLE,
	   evnts.APPT_CONF_STAT_NAME,
	   evnts.ZIP,
	   evnts.APPT_CONF_DTTM,
	   evnts.SIGNIN_DTTM,
	   evnts.ARVL_LIST_REMOVE_DTTM,
	   evnts.ROOMED_DTTM,
	   evnts.NURSE_LEAVE_DTTM,
	   evnts.PHYS_ENTER_DTTM,
	   evnts.CANCEL_REASON_NAME,
	   evnts.SER_RPT_GRP_SIX AS financial_division,
	   evnts.SER_RPT_GRP_EIGHT AS financial_subdivision,
	   evnts.CANCEL_INITIATOR,
	   evnts.F2F_Flag,
	   evnts.TIME_TO_ROOM_MINUTES,
	   evnts.TIME_IN_ROOM_MINUTES,
	   evnts.BEGIN_CHECKIN_DTTM,
	   evnts.PAGED_DTTM,
	   evnts.FIRST_ROOM_ASSIGN_DTTM,
	   evnts.CANCEL_LEAD_HOURS,
	   evnts.APPT_CANC_DTTM,
	   evnts.Entry_UVaID,
	   evnts.Canc_UVaID,
	   evnts.PHONE_REM_STAT_NAME,
	   evnts.CHANGE_DATE,
	   evnts.Cancel_Lead_Days,

	   evnts.APPT_MADE_DTTM,
	   evnts.BUSINESS_UNIT,
	   evnts.Prov_Typ,
	   evnts.Staff_Resource,

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
	   evnts.APPT_SERIAL_NUM,
	   evnts.RESCHED_APPT_CSN_ID,
	   evnts.Appointment_Request_Date,
       (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND day_date >= evnts.Appointment_Request_Date AND day_date < evnts.APPT_DT) Appointment_Lag_Business_Days,
	   evnts.BILL_PROV_YN

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

			main.DEPT_SPECIALTY_NAME,
			main.PROV_SPECIALTY_NAME,
			main.APPT_DTTM,
		    main.ENC_TYPE_C,
			main.ENC_TYPE_TITLE,
			main.APPT_CONF_STAT_NAME,
			main.ZIP,
			main.APPT_CONF_DTTM,
			main.SIGNIN_DTTM,
			main.ARVL_LIST_REMOVE_DTTM,
			main.ROOMED_DTTM,
			main.NURSE_LEAVE_DTTM,
			main.PHYS_ENTER_DTTM,
			main.CANCEL_REASON_NAME,
			main.SER_RPT_GRP_SIX,
			main.SER_RPT_GRP_EIGHT,
			main.F2F_Flag,
		    main.TIME_TO_ROOM_MINUTES,
			main.TIME_IN_ROOM_MINUTES,
			main.BEGIN_CHECKIN_DTTM,
			main.PAGED_DTTM,
			main.FIRST_ROOM_ASSIGN_DTTM,
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
			main.BUSINESS_UNIT,
		    main.Prov_Typ,
			main.Staff_Resource,
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
			main.APPT_SERIAL_NUM,
			main.RESCHED_APPT_CSN_ID,
			main.BILL_PROV_YN

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
				   appts.DEPT_SPECIALTY_NAME,
				   appts.PROV_SPECIALTY_NAME,
				   appts.APPT_DTTM,
				   appts.ENC_TYPE_C,
				   appts.ENC_TYPE_TITLE,
				   appts.APPT_CONF_STAT_NAME,
				   appts.ZIP,
				   appts.APPT_CONF_DTTM,
				   appts.SIGNIN_DTTM,
				   appts.ARVL_LIST_REMOVE_DTTM,
				   appts.ROOMED_DTTM,
				   appts.NURSE_LEAVE_DTTM,
				   appts.PHYS_ENTER_DTTM,
				   appts.CANCEL_REASON_NAME,
				   appts.SER_RPT_GRP_SIX,
				   appts.SER_RPT_GRP_EIGHT,
				   appts.F2F_Flag,
				   appts.TIME_TO_ROOM_MINUTES,
				   appts.TIME_IN_ROOM_MINUTES,
				   appts.BEGIN_CHECKIN_DTTM,
				   appts.PAGED_DTTM,
				   appts.FIRST_ROOM_ASSIGN_DTTM,
				   appts.CANCEL_LEAD_HOURS,
				   appts.APPT_CANC_DTTM,
				   entryemp.EMPlye_Systm_Login AS Entry_UVaID,
				   cancemp.EMPlye_Systm_Login AS Canc_UVaID,
				   appts.PHONE_REM_STAT_NAME,
				   appts.CHANGE_DATE,
				   appts.APPT_MADE_DTTM,
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
				   appts.APPT_SERIAL_NUM,
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

            WHERE (appts.APPT_DT >= @locstartdate
              AND appts.APPT_DT < @locenddate)
			AND excl.DEPARTMENT_ID IS NULL
			--AND appts.RPT_GRP_SIX = '14'
			--AND appts.DEPARTMENT_ID = 10243003

        ) AS main
    ) evnts
        ON (date_dim.day_date = CAST(evnts.APPT_DT AS SMALLDATETIME))

WHERE date_dim.day_date >= @locstartdate
      AND date_dim.day_date < @locenddate

--ORDER BY date_dim.day_date;

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
--('Transplant')
('Digestive Health')
--('Primary Care')
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
--('Primary Care')
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
 --(10242012) -- UVPC FAMILY MEDICINE
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
--('Fellow') -- Person
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

SELECT 
       [provider_id]
      ,[provider_name]
      ,[PAT_ENC_CSN_ID]
	  --,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  --,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  --,[sk_Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
      ,[event_type]
      ,[event_count]
      ,[event_date]
      --,[event_id]
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
      --,[corp_service_line_name]
      ,[corp_service_line]
      ,[hs_area_id]
      ,[hs_area_name]
      ,[pod_id]
      ,[pod_name]
      ,[hub_id]
      ,[hub_name]
      --,[w_department_id]
      --,[w_department_name]
      --,[w_department_name_external]
      --,[w_practice_group_id]
      --,[w_practice_group_name]
      --,[w_service_line_id]
      --,[w_service_line_name]
      --,[w_sub_service_line_id]
      --,[w_sub_service_line_name]
      --,[w_opnl_service_id]
      --,[w_opnl_service_name]
      --,[w_corp_service_line_id]
      --,[w_corp_service_line_name]
      --,[w_report_period]
      --,[w_report_date]
      --,[w_hs_area_id]
      --,[w_hs_area_name]
      --,[w_pod_id]
      --,[w_pod_name]
      --,[w_hub_id]
      --,[w_hub_name]
      ,[prov_service_line_id]
      --,[prov_service_line_name]
      ,[prov_service_line]
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
	  ,[CHANGE_DATE]
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
      --,[Load_Dtm]
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
      --,[F2_Flag]
	  ,F2F_Flag
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
      ,[financial_division_id]
      ,[financial_division_name]
      ,[financial_sub_division_id]
      ,[financial_sub_division_name]
      ,[rev_location_id]
      ,[rev_location]
      ,[som_group_id]
      ,[som_group_name]
      ,[som_department_id]
      ,[som_department_name]
      ,[som_division_id]
      --,[w_financial_division_id]
      --,[w_financial_division_name]
      --,[w_financial_sub_division_id]
      --,[w_financial_sub_division_name]
      --,[w_rev_location_id]
      --,[w_rev_location]
      --,[w_som_group_id]
      --,[w_som_group_name]
      --,[w_som_department_id]
      --,[w_som_department_name]
      --,[w_som_division_id]
      ,[som_division_name]
      --,[w_som_division_name]
      ,[APPT_MADE_DTTM]
      ,[BUSINESS_UNIT]
      ,[Prov_Typ]
      ,[Staff_Resource]
      --,[som_division_5]
      --,[w_som_hs_area_id]
      --,[w_som_hs_area_name]
      ,[APPT_SERIAL_NUM]
      ,[RESCHED_APPT_CSN_ID]
      ,[Appointment_Request_Date]
      ,[Appointment_Lag_Business_Days]
      ,[BILL_PROV_YN]
  /* No Show Rate */
--
-- No Show Rate
--				(SUM(appt_event_No_Show = 1) + SUM(appt_event_Canceled_Late = 1))
--              /
--              (SUM(appt_event_Canceled = 0) + SUM(appt_event_Canceled_Late = 1) + SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--
  /*
	  ,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN appt_event_No_Show = 1 OR appt_event_Canceled_Late = 1 THEN 1 ELSE 0 END AS [No Show]
  */
  /* Bump Rate */
--
-- Bump Rate
--				SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--              /
--              (SUM(appt_event_Canceled = 0) + SUM(appt_event_Canceled_Late = 1) + SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--
  /*
	  ,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  --,CASE WHEN appt_event_Provider_Canceled = 1 THEN 277
	  --,CASE WHEN appt_event_Provider_Canceled = 1 AND DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT) <= 45 THEN 1 WHEN appt_event_Provider_Canceled = 1 AND DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT) > 45 THEN 0 ELSE NULL END AS [Cancel_Lead_Days_Less_Than_46]
  */
--
-- Bump Rate (Billing Providers)
--				SUM(BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)
--              /
--              (SUM(BILL_PROV_YN = 1 AND appt_event_Canceled = 0) + SUM(BILL_PROV_YN = 1 AND appt_event_Canceled_Late = 1) + SUM(BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--
  /*
	  ,CASE WHEN (BILL_PROV_YN = 1 AND appt_event_Canceled = 0) OR (BILL_PROV_YN = 1 AND appt_event_Canceled_Late = 1) OR (BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  --,CASE WHEN appt_event_Provider_Canceled = 1 THEN 277
	  --,CASE WHEN appt_event_Provider_Canceled = 1 AND DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT) <= 45 THEN 1 WHEN appt_event_Provider_Canceled = 1 AND DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT) > 45 THEN 0 ELSE NULL END AS [Cancel_Lead_Days_Less_Than_46]
  */
--
-- Bump Rate (Provider Type)
--				SUM((Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant') AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)
--              /
--              SUM((Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant') AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
--
  --/*
	  ,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  --,CASE WHEN appt_event_Provider_Canceled = 1 THEN 277
	  --,CASE WHEN appt_event_Provider_Canceled = 1 AND DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT) <= 45 THEN 1 WHEN appt_event_Provider_Canceled = 1 AND DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT) > 45 THEN 0 ELSE NULL END AS [Cancel_Lead_Days_Less_Than_46]
  --*/
  /* Completed Count */
  /*
	  ,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN appt_event_Completed = 1 OR appt_event_Arrived = 1 THEN 1 ELSE 0  END AS [Completed]
  */
  /* New Patient Lag Days */
--
-- Average Lag Time to Appointment for New Patients in days
--				SUM(CASE WHEN (appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Days >= 0) THEN Appointment_Lag_Days ELSE 0 END)
--              /
--              SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Days >= 0)
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

  INTO #events_list

  --FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  FROM #metric
  WHERE
  /* No Show Rate */
  /*
  ((event_count = 1)
   AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* Bump Rate */
  /*
  ((event_count = 1)
  AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* Bump Rate (Billing Providers) */
  /*
  ((event_count = 1)
  AND ((BILL_PROV_YN = 1 AND appt_event_Canceled = 0) OR (BILL_PROV_YN = 1 AND appt_event_Canceled_Late = 1) OR (BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* Bump Rate (Provider Type)*/
  --/*
  ((event_count = 1)
  AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  AND EXISTS(SELECT Staff_Resource FROM @StaffResource WHERE Staff_Resource = Staff_Resource)
  AND EXISTS(SELECT Provider_Type FROM @ProviderType WHERE Provider_Type = Prov_Typ)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  --*/
  /* Completed Count */
  /*
  (event_count = 1)
   AND ((appt_event_Canceled = 0)  OR ((appt_event_Canceled_Late = 1) OR ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* New Patient Lag Days */
  /*
  (event_count = 1)
  AND (appt_event_Completed = 1 AND Appointment_Lag_Days >= 0)
  AND event_date BETWEEN @locstartdate AND @locenddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* New Patient Visits Percentage */
  --(event_count = 1)
  --AND ((appt_event_Completed = 1) OR (appt_event_Arrived = 1))
  --AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* Visit Time Minutes Average */
  /*
  (event_count = 1)
  AND ((appt_event_Completed = 1) OR (appt_event_Arrived = 1))
  AND ((CYCLE_TIME_MINUTES_Adjusted IS NOT NULL) AND (CYCLE_TIME_MINUTES_Adjusted >= 0))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */

  SELECT *
  FROM #events_list
  --WHERE fyear_num = 2020
  --AND fmonth_num = 2
  --AND fmonth_num IN (1,2)
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
		-- , Bump
		-- , APPT_STATUS_FLAG
  --       , event_date
  --ORDER BY person_name
  --       , event_date
  --ORDER BY event_date
  --       , epic_department_id
		-- , Bump DESC
  ORDER BY epic_department_id
         , Prov_Typ
		 , Bump DESC

--SELECT
SELECT
	epic_department_id
   ,Prov_Typ
   ,fyear_num
   ,fmonth_num
   --,event_date
  /* No Show Rate */
  /*
	   SUM(CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointment]
	  ,SUM(CASE WHEN appt_event_No_Show = 1 OR appt_event_Canceled_Late = 1 THEN 1 ELSE 0 END) AS [No Show]
  */
  /* Bump Rate */
  /*
	   SUM(CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointment]
	  ,SUM(CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END) AS [Bump]
  */
  /* Bump Rate (Billing Providers) */
  /*
	   SUM(CASE WHEN (BILL_PROV_YN = 1 AND appt_event_Canceled = 0) OR (BILL_PROV_YN = 1 AND appt_event_Canceled_Late = 1) OR (BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointment]
	  ,SUM(CASE WHEN BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END) AS [Bump]
  */
  /* Bump Rate (Provider Type) */
  --/*
	  ,SUM(CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointment]
	  ,SUM(CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END) AS [Bump]
  --*/
  /* Completed Count */
  /*
	  SUM(CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointments]
	 ,SUM(CASE WHEN appt_event_Completed = 1 OR appt_event_Arrived = 1 THEN 1 ELSE 0  END) AS [Completed]
  */
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
	--,SUM(CASE WHEN (appt_event_Completed = 1 OR appt_event_Arrived = 1) AND (CYCLE_TIME_MINUTES_Adjusted IS NOT NULL AND CYCLE_TIME_MINUTES_Adjusted >= 0) THEN CYCLE_TIME_MINUTES_Adjusted ELSE 0 END) AS [CYCLE_TIME_MINUTES_Adjusted]
  --FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  FROM #metric
  WHERE
  /* No Show Rate */
  /*
  ((event_count = 1)
   AND (appt_event_Canceled = 0  OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* Bump Rate */
  /*
  ((event_count = 1)
   AND ((appt_event_Canceled = 0) OR ((appt_event_Canceled_Late = 1) OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))))
  AND event_date BETWEEN @locstartdate AND @locenddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* Bump Rate (Billing Providers) */
  /*
  ((event_count = 1)
   AND ((BILL_PROV_YN = 1 AND appt_event_Canceled = 0) OR (BILL_PROV_YN = 1 AND appt_event_Canceled_Late = 1) OR (BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* Bump Rate (Provider Type)*/
  --/*
  ((event_count = 1)
  AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  AND EXISTS(SELECT Staff_Resource FROM @StaffResource WHERE Staff_Resource = Staff_Resource)
  AND EXISTS(SELECT Provider_Type FROM @ProviderType WHERE Provider_Type = Prov_Typ)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  --*/
  /* Completed Count */
  /*
  (event_count = 1)
  AND ((appt_event_Canceled = 0)  OR ((appt_event_Canceled_Late = 1) OR ((appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* New Patient Lag Days */
  /*
  (event_count = 1)
  AND (appt_event_Completed = 1 AND Appointment_Lag_Days >= 0)
  AND event_date BETWEEN @locstartdate AND @locenddate
  AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
  /* New Patient Visits Percentage */
  --(event_count = 1) AND ((appt_event_Completed = 1) OR (appt_event_Arrived = 1))
  --AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  /* Visit Time Minutes Average */
  /*
  (event_count = 1)
  AND ((appt_event_Completed = 1) OR (appt_event_Arrived = 1))
  AND ((CYCLE_TIME_MINUTES_Adjusted IS NOT NULL) AND (CYCLE_TIME_MINUTES_Adjusted >= 0))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
  */
 -- GROUP BY
	--fyear_num
 -- , fmonth_num
 -- GROUP BY
	--epic_department_id
 --  ,event_date
 -- GROUP BY
	--epic_department_id
 --  ,Prov_Typ
 --  ,fyear_num
 --  ,fmonth_num
  GROUP BY
	epic_department_id
   ,fyear_num
   ,fmonth_num
   ,Prov_Typ
 -- ORDER BY
	--fyear_num
 -- , fmonth_num
 -- ORDER BY
	--epic_department_id
 --  ,event_date
 -- ORDER BY
	--epic_department_id
 --  ,Prov_Typ
 --  ,fyear_num
 --  ,fmonth_num
  ORDER BY
	epic_department_id
   ,fyear_num
   ,fmonth_num
   ,Prov_Typ

GO


