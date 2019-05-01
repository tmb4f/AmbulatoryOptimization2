USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================
-- Author:		Tom Burgan
-- Create date: 04/12/2019
-- Description:	HAR Verification Rate Data Portal SSRS export script
-- =========================================================
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         04/12/2019 - Tom		-- create stored procedure
--         04/16/2019 - Tom     -- edit logic for filtering by event_date
--         05/01/2019 - TMB     -- add logic for SOM Department and SOM Division report parameters
--************************************************************************************************************************
ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_HARVerificationRate_SSRS_Download]
    @StartDate SMALLDATETIME,
    @EndDate SMALLDATETIME,
    @in_servLine VARCHAR(MAX),
    @in_deps VARCHAR(MAX),
    @in_depid VARCHAR(MAX),
	@in_pods VARCHAR(MAX),
	@in_podid VARCHAR(MAX),
	@in_hubs VARCHAR(MAX),
	@in_hubid VARCHAR(MAX),
    @in_somdeps VARCHAR(MAX),
	@in_somdepid VARCHAR(MAX),
	@in_somdivs VARCHAR(MAX),
	@in_somdivid VARCHAR(MAX)
AS

SET NOCOUNT ON;

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME,
        @locdt INT;

SET @locstartdate = @StartDate;
SET @locenddate   = DATEADD(mi,-1,DATEADD(dd,1,@EndDate)); -- TabRptg table event_date value has date and time of appointment
SET @locdt = 5; -- Default days between HAR Verification date and appointment date for scorecard

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
DECLARE @tab_somdeps TABLE
(
    som_department_id VARCHAR(MAX)
);
INSERT INTO @tab_somdeps
(
    som_department_id
)
SELECT Param
FROM ETL.fn_ParmParse(@in_somdeps, ',');
DECLARE @tab_somdepid TABLE
(
    som_department_id VARCHAR(MAX)
);
INSERT INTO @tab_somdepid
SELECT Param
FROM ETL.fn_ParmParse(@in_somdepid, ',');
DECLARE @tab_somdivs TABLE
(
    som_division_id VARCHAR(MAX)
);
INSERT INTO @tab_somdivs
(
    som_division_id
)
SELECT Param
FROM ETL.fn_ParmParse(@in_somdivs, ',');
DECLARE @tab_somdivid TABLE
(
    som_division_id VARCHAR(MAX)
);
INSERT INTO @tab_somdivid
SELECT Param
FROM ETL.fn_ParmParse(@in_somdivid, ',');

SELECT
       event_date,
       event_type, -- 'HAR_Verification'
	   event_category, -- NULL
       event_count, -- NULL
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
	   1 AS [HAR Encounter],
	   CAST(CASE WHEN [HAR_Verif_Status_C] IN ('1','6','8','12','13') AND [HAR_Verification_DATEDIFF] >= @locdt THEN 1 ELSE 0 END AS INT) AS [HAR Verification],
	   [Ins Verification],
	   [E-Verified],
	   [RTE Enabled],
	   [RTE Override case Rate],
	   MYCHART_STATUS_NAME,
	   APPT_STATUS_C,
	   enc.Appt_Dtm AS APPT_DTTM,
	   CANCEL_REASON_C,
	   CANCEL_INITIATOR,
	   CANCEL_LEAD_HOURS,
	   Cancel_Lead_Days,
	   APPT_CANCEL_DATE,
	   appt_event_Canceled,
	   appt_event_Canceled_Late,
	   appt_event_Provider_Canceled,
	   HAR_Verified_Date,
	   INS_Verified_Date,
	   HAR_Verif_Status,
	   HAR_Verif_Status_C,
	   PAYOR_ID,
	   PAYOR_NAME,
	   PLAN_ID,
	   BENEFIT_PLAN_NAME,
	   ACCT_FIN_CLASS_C,
	   HSP_ACCOUNT_ID,
	   COVERAGE_ID,
	   Appt_Entry_User,
	   HAR_Verif_User,
	   HAR_Verification_DATEDIFF,
	   APPT_MADE_DTTM,
	   BUSINESS_UNIT,
	   STAFF_RESOURCE,
	   PROV_TYPE,
	   w_rev_location_id,
	   w_rev_location,
	   w_som_department_id,
	   w_som_department_name,
	   w_financial_division_id,
	   w_financial_division_name,
	   w_financial_sub_division_id,
	   w_financial_sub_division_name,
	   w_som_division_id,
	   w_som_division_name
FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_HARVerification_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt enc
ON enc.sk_Fact_Pt_Enc_Clrt = tabrptg.sk_Fact_Pt_Enc_Clrt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct_Aggr acct
ON acct.sk_Fact_Pt_Acct = tabrptg.sk_Fact_Pt_Acct
WHERE 1 = 1
      AND event_date >= @locstartdate
      AND event_date <= @locenddate
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
      )
      AND
      (
          0 IN
          (
              SELECT som_department_id FROM @tab_somdeps
          )
          OR som_department_id IN
             (
                 SELECT som_department_id FROM @tab_somdeps
             )
      )
      AND
      (
          0 IN
          (
              SELECT som_department_id FROM @tab_somdepid
          )
          OR som_department_id IN
             (
                 SELECT som_department_id FROM @tab_somdepid
             )
      )
      AND
      (
          0 IN
          (
              SELECT som_division_id FROM @tab_somdivs
          )
          OR som_division_id IN
             (
                 SELECT som_division_id FROM @tab_somdivs
             )
      )
      AND
      (
          0 IN
          (
              SELECT som_division_id FROM @tab_somdivid
          )
          OR som_division_id IN
             (
                 SELECT som_division_id FROM @tab_somdivid
             )
      );

GO


