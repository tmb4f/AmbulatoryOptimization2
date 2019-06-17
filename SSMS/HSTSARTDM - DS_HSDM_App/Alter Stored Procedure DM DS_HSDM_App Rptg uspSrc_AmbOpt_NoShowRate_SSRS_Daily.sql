USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Daily]
    (
     @StartDate SMALLDATETIME = NULL,
     @EndDate SMALLDATETIME = NULL,
     @in_servLine VARCHAR(MAX),
     @in_deps VARCHAR(MAX),
	 @in_pods VARCHAR(MAX),
	 @in_hubs VARCHAR(MAX),
     @in_somdeps VARCHAR(MAX),
	 @in_somdivs VARCHAR(MAX)
    )
AS 
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
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--              DS_HSDW_Prod.Rptg.vwDim_Patient
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--              DS_HSDW_Prod.Rptg.vwDim_Physcn
--              DS_HSDW_Prod.Rptg.vwRef_Service_Line
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye
--              DS_HSDM_App.Stage.AmbOpt_Excluded_Department
--              DS_HSDM_App.Rptg.vwRef_Crosswalk_HSEntity_Prov
--              DS_HSDM_App.Rptg.vwRef_SOM_Hierarchy
--              DS_HSDW_Prod.Rptg.vwDim_Date
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Daily]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/03/2019 - TMB - create stored procedure
--         06/06/2019 - TMB - remove parameters/arguments logic
--         06/12/2019 - TMB - edit logic: StartDate and EndDate arguments may not have time values
--************************************************************************************************************************

    SET NOCOUNT ON;

/*
---BDD 6/3/2019 per Tom don't need the defaults here. They will be set in SSRS 
	----get default Balanced Scorecard date range
IF @StartDate IS NULL
   AND @EndDate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @StartDate OUTPUT, @EndDate OUTPUT;
*/ 

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
         		   uwd.Clrt_Financial_Division AS financial_division_id,
				   CAST(uwd.Clrt_Financial_Division_Name AS VARCHAR(150)) AS financial_division_name,
				   uwd.Clrt_Financial_SubDivision AS financial_sub_division_id, 
				   uwd.Clrt_Financial_SubDivision_Name AS financial_sub_division_name,
				   uwd.SOM_Group_ID AS som_group_id,
				   uwd.SOM_group AS som_group_name,
				   uwd.SOM_department_id AS som_department_id,
				   uwd.SOM_department AS som_department_name,
				   uwd.SOM_division_id AS som_division_id,
				   uwd.SOM_division_name AS som_division_name,
				   uwd.SOM_division_5 AS som_division_5,
				   CASE WHEN uwd.SOM_Group_ID IS NULL THEN CAST(NULL AS SMALLINT) ELSE CAST(3 AS SMALLINT) END AS som_hs_area_id,
				   CASE WHEN uwd.SOM_Group_ID IS NULL THEN CAST(NULL AS VARCHAR(150)) ELSE CAST('School of Medicine' AS VARCHAR(150)) END AS som_hs_area_name,
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
	            LEFT OUTER JOIN
	            (
					SELECT DISTINCT
					    wd.sk_Dim_Physcn,
						wd.PROV_ID,
             			wd.Clrt_Financial_Division,
			    		wd.Clrt_Financial_Division_Name,
						wd.Clrt_Financial_SubDivision, 
					    wd.Clrt_Financial_SubDivision_Name,
					    wd.wd_Dept_Code,
					    wd.SOM_Group_ID,
					    wd.SOM_Group,
						wd.SOM_department_id,
					    wd.SOM_department,
						wd.SOM_division_id,
						wd.SOM_division_name,
						wd.SOM_division_5
					FROM
					(
					    SELECT
						    cwlk.sk_Dim_Physcn,
							cwlk.PROV_ID,
             			    cwlk.Clrt_Financial_Division,
			    		    cwlk.Clrt_Financial_Division_Name,
						    cwlk.Clrt_Financial_SubDivision, 
							cwlk.Clrt_Financial_SubDivision_Name,
							cwlk.wd_Dept_Code,
							som.SOM_Group_ID,
							som.SOM_Group,
							som.SOM_department_id,
							som.SOM_department,
							som.SOM_division_id,
							som.SOM_division_name,
							som.SOM_division_5,
							ROW_NUMBER() OVER (PARTITION BY cwlk.sk_Dim_Physcn ORDER BY som.som_group_id ASC) AS [SOMSeq]
						FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
						    LEFT OUTER JOIN (SELECT DISTINCT
							                     SOM_Group_ID,
												 SOM_Group,
												 SOM_department_id,
												 SOM_department,
												 SOM_division_id,
												 SOM_division_name,
												 SOM_division_5
						                     FROM Rptg.vwRef_SOM_Hierarchy
						                    ) AS som
						        ON cwlk.wd_Dept_Code = som.SOM_division_5
					    WHERE cwlk.wd_Is_Primary_Job = 1
                              AND cwlk.wd_Is_Position_Active = 1
					) AS wd
					WHERE wd.SOMSeq = 1
				) AS uwd
				    ON uwd.sk_Dim_Physcn = doc.sk_Dim_Physcn

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


