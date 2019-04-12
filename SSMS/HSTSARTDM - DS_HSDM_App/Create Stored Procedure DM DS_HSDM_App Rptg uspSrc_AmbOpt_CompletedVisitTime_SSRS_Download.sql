USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================
-- Author:		Tom Burgan
-- Create date: 04/10/2019
-- Description:	Completed Visit Time Data Portal SSRS export script
-- =========================================================
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         04/10/2019 - Tom		-- create stored procedure
--************************************************************************************************************************
CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_CompletedVisitTime_SSRS_Download]
    @StartDate SMALLDATETIME,
    @EndDate SMALLDATETIME,
    @in_servLine VARCHAR(MAX),
    @in_deps VARCHAR(MAX),
    @in_depid VARCHAR(MAX),
	@in_pods VARCHAR(MAX),
	@in_podid VARCHAR(MAX),
	@in_hubs VARCHAR(MAX),
	@in_hubid VARCHAR(MAX)
AS

SET NOCOUNT ON

DECLARE @tab_servLine TABLE
(
    Service_Line_Id VARCHAR(MAX)
);
INSERT INTO @tab_servLine
SELECT Param
FROM ETL.fn_ParmParse(@in_servLine, ',');
DECLARE @tab_pods TABLE
(
    pod_id VARCHAR(MAX)
);
INSERT INTO @tab_pods
SELECT Param
FROM ETL.fn_ParmParse(@in_pods, ',');
DECLARE @tab_podid TABLE
(
    pod_id VARCHAR(MAX)
);
INSERT INTO @tab_podid
SELECT Param
FROM ETL.fn_ParmParse(@in_podid, ',');
DECLARE @tab_hubs TABLE
(
    hub_id VARCHAR(MAX)
);
INSERT INTO @tab_hubs
SELECT Param
FROM ETL.fn_ParmParse(@in_hubs, ',');
DECLARE @tab_hubid TABLE
(
    hub_id VARCHAR(MAX)
);
INSERT INTO @tab_hubid
SELECT Param
FROM ETL.fn_ParmParse(@in_hubid, ',');
DECLARE @tab_deps TABLE
(
    epic_department_id VARCHAR(MAX)
);
INSERT INTO @tab_deps
SELECT Param
FROM ETL.fn_ParmParse(@in_deps, ',');
DECLARE @tab_depid TABLE
(
    epic_department_id VARCHAR(MAX)
);
INSERT INTO @tab_depid
SELECT Param
FROM ETL.fn_ParmParse(@in_depid, ',');

SELECT
       event_date,
       event_type, -- 'Appointment'
	   event_category, -- NULL
       event_count, -- 1
       fyear_num,
       Load_Dtm,
       hs_area_name,
       COALESCE(w_service_line_id, w_opnl_service_id, w_corp_service_line_id) service_line_id,
       COALESCE(w_service_line_name, w_opnl_service_name, w_corp_service_line_name) Service_Line,
	   w_pod_id,
	   w_pod_name,
	   w_hub_id,
	   w_hub_name,
       w_department_id,
       w_department_name,
       w_department_name_external,
       peds,
       transplant,
       person_id,
       provider_id,
	   enc.PAT_ENC_CSN_ID,
	   acct.AcctNbr_int,
	   CASE WHEN appt_event_Completed = 1 THEN 1 ELSE 0 END AS [Completed],
	   CASE WHEN appt_event_Arrived = 1 THEN 1 ELSE 0 END AS [Arrived],
	   APPT_STATUS_FLAG,
	   APPT_DTTM,
	   APPT_MADE_DATE,
	   appt_event_Completed,
	   appt_event_Arrived,
	   appt_event_New_to_Specialty,
	   PHONE_REM_STAT_NAME,
	   APPT_MADE_DTTM,
	   BUSINESS_UNIT,
	   Prov_Typ,
	   w_rev_location_id,
	   w_rev_location,
	   w_som_department_id,
	   w_som_department_name,
	   w_financial_division_id,
	   w_financial_division_name,
	   w_financial_sub_division_id,
	   w_financial_sub_division_name,
	   w_som_division_id,
	   w_som_division_name,
	   PRC_NAME,
	   ENTRY_DATE,
	   CHECKIN_DTTM,
	   CHECKOUT_DTTM,
	   VISIT_END_DTTM,
	   CYCLE_TIME_MINUTES,
	   Appointment_Lag_Days,
	   CYCLE_TIME_MINUTES_Adjusted,
	   TIME_TO_ROOM_MINUTES,
	   TIME_IN_ROOM_MINUTES,
	   BEGIN_CHECKIN_DTTM,
	   ARVL_LIST_REMOVE_DTTM,
	   ROOMED_DTTM,
	   NURSE_LEAVE_DTTM,
	   PHYS_ENTER_DTTM,
	   SIGNIN_DTTM,
	   PAGED_DTTM,
	   FIRST_ROOM_ASSIGN_DTTM,
	   Entry_UVaID
FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt enc
ON enc.sk_Fact_Pt_Enc_Clrt = tabrptg.sk_Fact_Pt_Enc_Clrt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct_Aggr acct
ON acct.sk_Fact_Pt_Acct = tabrptg.sk_Fact_Pt_Acct
WHERE 1 = 1
      AND event_date >= @StartDate
      AND event_date <= @EndDate
	  AND ((event_count = 1)
	       AND (appt_event_Completed = 1 OR appt_event_Arrived = 1) AND (CYCLE_TIME_MINUTES_Adjusted IS NOT NULL AND CYCLE_TIME_MINUTES_Adjusted >= 0))
      AND
      (
          0 IN
          (
              SELECT epic_department_id FROM @tab_deps
          )
          OR epic_department_id IN
             (
                 SELECT epic_department_id FROM @tab_deps
             )
      )
      AND
      (
          0 IN
          (
              SELECT epic_department_id FROM @tab_depid
          )
          OR epic_department_id IN
             (
                 SELECT epic_department_id FROM @tab_depid
             )
      )
      AND
      (
          0 IN
          (
              SELECT pod_id FROM @tab_pods
          )
          OR pod_id IN
             (
                 SELECT pod_id FROM @tab_pods
             )
      )
      AND
      (
          0 IN
          (
              SELECT pod_id FROM @tab_podid
          )
          OR pod_id IN
             (
                 SELECT pod_id FROM @tab_podid
             )
      )
      AND
      (
          0 IN
          (
              SELECT hub_id FROM @tab_hubs
          )
          OR hub_id IN
             (
                 SELECT hub_id FROM @tab_hubs
             )
      )
      AND
      (
          0 IN
          (
              SELECT hub_id FROM @tab_hubid
          )
          OR hub_id IN
             (
                 SELECT hub_id FROM @tab_hubid
             )
      )
      AND
      (
          0 IN
          (
              SELECT Service_Line_Id FROM @tab_servLine
          )
          OR COALESCE(w_service_line_id, w_opnl_service_id) IN
             (
                 SELECT Service_Line_Id FROM @tab_servLine
             )
      );

GO


