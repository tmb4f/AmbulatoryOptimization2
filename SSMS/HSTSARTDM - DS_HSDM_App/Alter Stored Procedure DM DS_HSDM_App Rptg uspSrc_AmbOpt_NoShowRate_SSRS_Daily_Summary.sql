USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Daily_Summary]
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
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_NoShowRate_SSRS_Daily_Summary
--WHO : Tom Burgan
--WHEN: 6/7/19
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
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Daily_Summary]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/07/2019 - TMB - create stored procedure
--         06/12/2019 - TMB - edit logic: StartDate and EndDate arguments may not have time values
--		   07/01/2019 - TMB - change logic for setting SOM hierarchy values: som_division_id (INT) => som_division_name_id (VARCHAR(150))
--         07/09/2019 - TMB - restore som_division_id to data type int
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

SELECT SUM(CASE WHEN evnts.appt_event_Canceled = 0 OR evnts.appt_event_Canceled_Late = 1 OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointment],
	   SUM(CASE WHEN (evnts.appt_event_No_Show = 1 OR evnts.appt_event_Canceled_Late = 1) THEN 1 ELSE 0 END) AS [No Show]

FROM

    (
        SELECT DISTINCT
            main.epic_pod AS pod_id,
            main.epic_hub AS hub_id,
            main.epic_department_id,
            main.service_line_id,
            main.opnl_service_id,
            main.APPT_DT,
            main.PAT_ENC_CSN_ID,
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
                    AND (main.APPT_STATUS_C = 3)
                    AND (main.CANCEL_INITIATOR = 'PROVIDER')
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Provider_Canceled,
                                                 -- Calculated columns
-- Assumes that there is always a referral creation date (CHANGE_DATE) documented when a referral entry date (ENTRY_DATE) is documented
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
			main.som_department_id,
			main.som_division_id,
	        main.som_division_name

        FROM
        ( --main
            SELECT
			       appts.RPT_GRP_SIX AS epic_pod,
				   appts.RPT_GRP_SEVEN AS epic_hub,
				   appts.DEPARTMENT_ID AS epic_department_id,
				   mdm.service_line_id,
                   mdm.opnl_service_id,
                   --Select
                   appts.APPT_STATUS_FLAG,
                   appts.APPT_STATUS_C,
				   appts.CANCEL_INITIATOR,
                   appts.APPT_DT,
                   appts.PAT_ENC_CSN_ID,
                   appts.APPT_CANC_DTTM,
				   -- SOM
				   physcn.SOM_department_id AS som_department_id,
				   physcn.SOM_division_id AS som_division_id,
				   physcn.SOM_division_name AS som_division_name

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
      );

GO


