/*="SELECT [sk_Dash_AmbOpt_ScheduledAppointmentMetric_Tiles], [event_date], [epic_department_id], [epic_department_name], [epic_department_name_external], [fmonth_num], [fyear_num], [fyear_name], [report_period], [sk_Dim_Pt], [sk_Fact_Pt_Acct], [sk_Fact_Pt_Enc_Clrt], [person_birth_date], [person_gender], [person_id], [person_name], [provider_id], [provider_name], [service_line], [sub_service_line], [opnl_service_name], [corp_service_line_name], [hs_area_name], [pod_name], [hub_name], [w_service_line_name], [w_sub_service_line_name], [w_opnl_service_name], [w_corp_service_line_name], [w_hs_area_name], [prov_service_line_name], [prov_hs_area_name], [APPT_STATUS_FLAG], [APPT_STATUS_C], [CANCEL_REASON_C], [MRN_int], [CONTACT_DATE], [APPT_DT], [PAT_ENC_CSN_ID], [PRC_ID], [PRC_NAME], [sk_Dim_Physcn], [UVaID], [VIS_NEW_TO_SYS_YN], [VIS_NEW_TO_DEP_YN], [VIS_NEW_TO_PROV_YN], [VIS_NEW_TO_SPEC_YN], [VIS_NEW_TO_SERV_AREA_YN], [VIS_NEW_TO_LOC_YN], [APPT_MADE_DATE], [ENTRY_DATE], [CHECKIN_DTTM], [CHECKOUT_DTTM], [VISIT_END_DTTM], [CYCLE_TIME_MINUTES]  ,CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment], CASE WHEN (appt_event_No_Show = 1 OR appt_event_Canceled_Late = 1) THEN 1 ELSE 0 END AS [No Show], [appt_event_No_Show], [appt_event_Canceled_Late], [appt_event_Scheduled], [appt_event_Provider_Canceled], [appt_event_Completed], [appt_event_Arrived], [appt_event_New_to_Specialty], [Appointment_Lag_Days], [CYCLE_TIME_MINUTES_Adjusted], [APPT_DTTM], [CANCEL_INITIATOR], [CANCEL_REASON_NAME], [CANCEL_LEAD_HOURS], [APPT_CANC_DTTM], [Entry_UVaID], [Canc_UVaID], [PHONE_REM_STAT_NAME], [Cancel_Lead_Days], [Load_Dtm]  FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]  WHERE ((event_count = 1)  AND ((appt_event_Canceled = 0)  OR ((appt_event_No_Show = 1)  OR (appt_event_Canceled_Late = 1) OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))) AND event_date BETWEEN @ApptStartDate AND @ApptEndDate  AND CAST(epic_department_id AS VARCHAR(18)) IN (@DepartmentId)  AND provider_id IN (@ProviderId) AND COALESCE(" & Parameters!DepartmentGrouperColumn.Value & ",'" & Parameters!DepartmentGrouperNoValue.Value & "') IN (@PodServiceLine);"*/

USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Manikesh Iruku
-- Create date: 02/07/2019
-- Description:	ART Data port SSRS export Template
-- =============================================
CREATE PROCEDURE [Rptg].[uspSrc_ART_DataPortal_SSRS_Template]
    @StartDate SMALLDATETIME,
    @EndDate SMALLDATETIME,
    @in_servLine VARCHAR(MAX),
    @in_deps VARCHAR(MAX),
    @in_depid VARCHAR(MAX)
AS
DECLARE @tab_servLine TABLE
(
    Service_Line_Id VARCHAR(MAX)
);
INSERT INTO @tab_servLine
SELECT Param
FROM ETL.fn_ParmParse(@in_servLine, ',');
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
	   CASE
	     WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 'Appointment'
		 WHEN (appt_event_No_Show = 1 OR appt_event_Canceled_Late = 1) THEN 'No Show'
	   END AS event_category,
       event_count, -- 1 for event_category 'No Show' and 'Scheduled', 0 otherwise
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
       CAST(Discharge_Disposition_Dt AS DATE) Disp_Date,
       CAST(Discharge_Disposition_Dt AS TIME) Disp_Time,
       REASON_VISIT_NAME Reason_for_Visit,
       Acuity_Level,
       longest_provider,
       CARE_AREA_NAME Care_Area,
       --event_date,
       Preadmit_Order_Dt,
       Inpatient_Order_Dt,
       CAST(Departure_Dt AS TIME) Depart_Time,
       LOS_in_Hours,
       admitting_provider,
       admitting_unit
FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
WHERE 1 = 1
      AND event_date >= @StartDate
      AND event_date <= @EndDate
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
              SELECT Service_Line_Id FROM @tab_servLine
          )
          OR COALESCE(service_line_id, opnl_service_id) IN
             (
                 SELECT Service_Line_Id FROM @tab_servLine
             )
      );
GO


