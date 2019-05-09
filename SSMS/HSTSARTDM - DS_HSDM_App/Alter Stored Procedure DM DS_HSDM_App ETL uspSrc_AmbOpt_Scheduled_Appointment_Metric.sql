USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Scheduled_Appointment_Metric]
    (
     @startdate SMALLDATETIME = NULL
    ,@enddate SMALLDATETIME = NULL
    )
AS 
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
--				SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--              /
--              (SUM(appt_event_Canceled = 0) + SUM(appt_event_Canceled_Late = 1) + SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
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

       evnts.Appointment_Lag_Days,
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
	   evnts.som_division_5 -- VARCHAR(150)

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
			main.som_division_5

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

				   cwlk.Clrt_Financial_Division AS financial_division_id,
				   CAST(cwlk.Clrt_Financial_Division_Name AS VARCHAR(150)) AS financial_division_name,
				   cwlk.Clrt_Financial_SubDivision AS financial_sub_division_id,
				   CAST(cwlk.Clrt_Financial_SubDivision_Name AS VARCHAR(150)) AS financial_sub_division_name,

				   som.SOM_Group_ID AS som_group_id,
				   CAST(som.SOM_group AS VARCHAR(150)) AS som_group_name,
				   som.SOM_department_id AS som_department_id,
				   CAST(som.SOM_department AS VARCHAR(150)) AS som_department_name,
				   som.SOM_division_id AS som_division_id,
				   CAST(som.SOM_division_name AS VARCHAR(150)) AS som_division_name,
				   CAST(som.SOM_division_5 AS VARCHAR(150)) AS som_division_5

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
                LEFT OUTER JOIN Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
				    ON cwlk.sk_Dim_Physcn = doc.sk_Dim_Physcn
                       AND cwlk.wd_Is_Primary_Job = 1
                       AND cwlk.wd_Is_Position_Active = 1
                LEFT OUTER JOIN Rptg.vwRef_SOM_Hierarchy AS som
			        ON cwlk.wd_Dept_Code=som.SOM_division_5

            WHERE (appts.APPT_DT >= @locstartdate
              AND appts.APPT_DT < @locenddate)
			AND excl.DEPARTMENT_ID IS NULL

        ) AS main
    ) evnts
        ON (date_dim.day_date = CAST(evnts.APPT_DT AS SMALLDATETIME))

WHERE date_dim.day_date >= @locstartdate
      AND date_dim.day_date < @locenddate

ORDER BY date_dim.day_date;

GO


