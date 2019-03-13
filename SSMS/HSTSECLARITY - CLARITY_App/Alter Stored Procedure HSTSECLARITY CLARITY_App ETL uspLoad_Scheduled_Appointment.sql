USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [ETL].[uspLoad_Scheduled_Appointment]
(
@startdate SMALLDATETIME=NULL,
@enddate SMALLDATETIME=NULL
)
AS
/****************************************************************************************************************************************
WHAT: Create procedure ETL.uspLoad_Scheduled_Appointment
WHO : Tom Burgan
WHEN: 11/29/2017
WHY : Load staging table (HSTSDSSQLDM DS_HSDM_App Stage.Scheduled_Appointment) containing Cadence scheduled appointment detail.
      To facilitate the development of the stored procedures for the Ambulatory Optimization metrics.
----------------------------------------------------------------------------------------------------------------------------------------
INFO: 
      INPUTS:	HSTSECLARITY.CLARITY.dbo.V_SCHED_APPT
	            HSTSECLARITY.CLARITY.dbo.PAT_ENC
				HSTSECLARITY.CLARITY.dbo.PAT_ENC_4
				HSTSECLARITY.CLARITY.dbo.CLARITY_DEP
				HSTSECLARITY.CLARITY.dbo.PAT_ENC_3
				HSTSECLARITY.CLARITY.dbo.V_COVERAGE_PAYOR_PLAN
				HSTSECLARITY.CLARITY.dbo.PATIENT_MYC
				HSTSECLARITY.CLARITY.dbo.CLARITY_SER
				HSTSECLARITY.CLARITY.dbo.IDENTITY_ID
				HSTSECLARITY.CLARITY.dbo.REFERRAL
				HSTSECLARITY.CLARITY.dbo.ZC_MYCHART_STATUS
				HSTSECLARITY.CLARITY.dbo.ZC_RFL_STATUS
				HSTSECLARITY.CLARITY.dbo.ZC_RFL_TYPE
				HSTSECLARITY.CLARITY.dbo.ZC_SPECIALTY
				HSTSECLARITY.CLARITY.dbo.ZC_DISP_ENC_TYPE
                  
      OUTPUTS:  CLARITY_App.ETL.uspLoad_Scheduled_Appointment
   
----------------------------------------------------------------------------------------------------------------------------------------
MODS: 	11/29/2017--TMB--Create stored procedure
        01/16/2018--TMB--Change logic for identifying patient-initiated late cancellations
		04/30/2018--TMB--Add VIS_NEW columns, REFERRAL table information
		05/03/2018--TMB--Add PROV_SPECIALTY_C, PROV_SPECIALTY_NAME; use APPT_DTTM for date of scheduled appointment
		06/04/2018--TMB--Change NAME to MYCHART_STATUS_NAME; add ENC_TYPE_C, ENC_TYPE_TITLE, APPT_CONF_STAT_NAME, ZIP
		06/20/2018--TMB--Add CLARITY_SER.RPT_GRP_SIX, CLARITY_SER.RPT_GRP_EIGHT
		06/22/2018--TMB--Replace CLARITY_SER.RPT_GRP_SIX, CLARITY_SER.RPT_GRP_EIGHT values with their look-up names
		06/27/2018--TMB--Use CANCEL_INITIATOR value to identify patient-initiated cancellations; add F2F flag
		08/17/2018--TMB--Change criteria for "Canceled Late" appointment status; add APPT_CANC_DTTM, APPT_CANC_USER_ID,
		                  APPT_CANC_USER_NAME_WID, APPT_CONF_USER_ID, APPT_CONF_USER_NAME
		10/29/2018--TMB--Add REFERRAL_HIST.CHANGE_DATE
        03/13/2019--TMB--Add APPT_MADE_DTTM, UPDATE_DATE
*****************************************************************************************************************************************/

SET NOCOUNT ON;
 
----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------

---BDD 12/7/2017 - let's toy with the parameters a bit to eliminate the possibility of parameter sniffing errors. 

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate
------------------------------------------------------------------------------------------------------



 SELECT appt.PAT_ENC_CSN_ID ,
        appt.PAT_ID ,
        appt.PAT_NAME ,
        CAST(appt.IDENTITY_ID AS VARCHAR(50)) AS IDENTITY_ID, --MRN    ---BDD 10/11/2018 cast for Epic upgrade
        appt.MYCHART_STATUS_C ,
        appt.MYCHART_STATUS_NAME ,
        appt.VIS_NEW_TO_SYS_YN ,
        appt.VIS_NEW_TO_DEP_YN ,
        appt.VIS_NEW_TO_PROV_YN ,

			--appt info
        appt.APPT_SERIAL_NUM ,
        appt.RESCHED_APPT_CSN_ID ,
        appt.PRC_ID ,
        appt.PRC_NAME ,
        appt.APPT_BLOCK_C ,
        appt.APPT_BLOCK_NAME ,
        appt.APPT_LENGTH ,
        appt.APPT_STATUS_C ,
        appt.APPT_STATUS_NAME ,
        appt.APPT_CONF_STAT_C ,
        appt.CANCEL_LEAD_HOURS ,
        appt.CANCEL_INITIATOR ,
        appt.COMPLETED_STATUS_YN ,
        appt.SAME_DAY_YN ,
        appt.SAME_DAY_CANC_YN ,
        appt.CANCEL_REASON_C ,
        appt.CANCEL_REASON_NAME ,
        appt.WALK_IN_YN ,
        appt.OVERBOOKED_YN ,
        appt.OVERRIDE_YN ,
        appt.UNAVAILABLE_TIME_YN ,
        appt.CHANGE_CNT ,
        appt.JOINT_APPT_YN ,
        appt.PHONE_REM_STAT_C ,
        appt.PHONE_REM_STAT_NAME ,

			--dates/times
        appt.CONTACT_DATE ,
        appt.APPT_MADE_DATE ,
        appt.APPT_CANC_DATE ,
        appt.APPT_CONF_DTTM ,
        appt.APPT_DTTM ,
		appt.SIGNIN_DTTM,
		appt.PAGED_DTTM,
		appt.BEGIN_CHECKIN_DTTM,
		appt.CHECKIN_DTTM,
		appt.ARVL_LIST_REMOVE_DTTM,
		appt.ROOMED_DTTM,
		appt.FIRST_ROOM_ASSIGN_DTTM,
		appt.NURSE_LEAVE_DTTM,
		appt.PHYS_ENTER_DTTM,
		appt.VISIT_END_DTTM,
		appt.CHECKOUT_DTTM,
        appt.TIME_TO_ROOM_MINUTES , --diff between check in time and roomed time (earliest of the ARVL_LIST_REMOVE_DTTM, ROOMED_DTTM, and FIRST_ROOM_ASSIGN_DTTM)
        appt.TIME_IN_ROOM_MINUTES , --diff between roomed time and appointment end time (earlier of the CHECKOUT_DTTM and VISIT_END_DTTM)
        appt.CYCLE_TIME_MINUTES ,		--diff between the check-in time and the appointment end time.

			--Provider/scheduler info
        appt.REFERRING_PROV_ID ,
        appt.REFERRING_PROV_NAME_WID ,
        appt.PROV_ID ,
        appt.PROV_NAME_WID ,
        appt.RPT_GRP_FIVE , --service line
        appt.APPT_ENTRY_USER_ID ,
        appt.APPT_ENTRY_USER_NAME_WID ,
        appt.PROV_NAME ,

			--Billing info 
        appt.PAYOR_ID ,
        appt.PAYOR_NAME ,
        appt.BENEFIT_PLAN_ID ,
        appt.BENEFIT_PLAN_NAME ,
        appt.FIN_CLASS_NAME ,
        appt.DO_NOT_BILL_INS_YN ,
        appt.SELF_PAY_VISIT_YN ,
        appt.HSP_ACCOUNT_ID ,
        appt.ACCOUNT_ID ,
        appt.COPAY_DUE ,
        appt.COPAY_COLLECTED ,
        appt.COPAY_USER_ID ,
        appt.COPAY_USER_NAME_WID ,
	
			 --fac-org info
        appt.DEPARTMENT_ID ,
        appt.DEPARTMENT_NAME ,
        appt.DEPT_ABBREVIATION ,
        appt.RPT_GRP_THIRTY , -- service line
        appt.RPT_GRP_SIX , -- pod
        appt.RPT_GRP_SEVEN , -- hub
        appt.DEPT_SPECIALTY_C ,
        appt.DEPT_SPECIALTY_NAME ,
        appt.CENTER_C ,
        appt.CENTER_NAME ,
        appt.LOC_ID ,
        appt.LOC_NAME ,
        appt.SERV_AREA_ID ,
		appt.APPT_STATUS_FLAG ,
		appt.VIS_NEW_TO_SPEC_YN ,
		appt.VIS_NEW_TO_SERV_AREA_YN ,
		appt.VIS_NEW_TO_LOC_YN ,
		appt.REFERRAL_ID ,
		appt.ENTRY_DATE ,
		appt.RFL_STATUS_NAME ,
		appt.RFL_TYPE_NAME ,
		appt.PROV_SPECIALTY_C ,
		appt.PROV_SPECIALTY_NAME ,
		appt.APPT_DT ,
		appt.ENC_TYPE_C ,
		appt.ENC_TYPE_TITLE ,
		appt.APPT_CONF_STAT_NAME ,
		appt.ZIP ,
		appt.SER_RPT_GRP_SIX ,
		appt.SER_RPT_GRP_EIGHT ,
		appt.F2F_Flag ,
		appt.APPT_CANC_DTTM ,
		appt.APPT_CANC_USER_ID ,
		appt.APPT_CANC_USER_NAME_WID ,
		appt.APPT_CONF_USER_ID ,
		appt.APPT_CONF_USER_NAME ,
		appt.CHANGE_DATE ,
		appt.APPT_MADE_DTTM , -- DATETIME
		CAST(GETDATE() AS DATETIME) AS UPDATE_DATE -- DATETIME
 FROM 
 (
/*  All scheduled appointments with patient, MyChart, encounter, department, insurance, and referral detail  */
          SELECT 

			--patient info
                    V_SCHED_APPT.PAT_ENC_CSN_ID ,
                    V_SCHED_APPT.PAT_ID ,
                    PATIENT.PAT_NAME ,
                    IDENTITY_ID.IDENTITY_ID , --MRN
                    PATIENT_MYC.MYCHART_STATUS_C ,
                    ZC_MYCHART_STATUS.NAME AS MYCHART_STATUS_NAME ,
                    PAT_ENC_4.VIS_NEW_TO_SYS_YN ,
                    PAT_ENC_4.VIS_NEW_TO_DEP_YN ,
                    PAT_ENC_4.VIS_NEW_TO_PROV_YN ,

			--appt info
                    V_SCHED_APPT.APPT_SERIAL_NUM ,
                    V_SCHED_APPT.RESCHED_APPT_CSN_ID ,
                    V_SCHED_APPT.PRC_ID ,
                    V_SCHED_APPT.PRC_NAME ,
                    V_SCHED_APPT.APPT_BLOCK_C ,
                    V_SCHED_APPT.APPT_BLOCK_NAME ,
                    V_SCHED_APPT.APPT_LENGTH ,
                    V_SCHED_APPT.APPT_STATUS_C ,
                    V_SCHED_APPT.APPT_STATUS_NAME ,
                    V_SCHED_APPT.APPT_CONF_STAT_C ,
                    V_SCHED_APPT.CANCEL_LEAD_HOURS ,
                    V_SCHED_APPT.CANCEL_INITIATOR ,
                    V_SCHED_APPT.COMPLETED_STATUS_YN ,
                    V_SCHED_APPT.SAME_DAY_YN ,
                    V_SCHED_APPT.SAME_DAY_CANC_YN ,
                    V_SCHED_APPT.CANCEL_REASON_C ,
                    V_SCHED_APPT.CANCEL_REASON_NAME ,
                    V_SCHED_APPT.WALK_IN_YN ,
                    V_SCHED_APPT.OVERBOOKED_YN ,
                    V_SCHED_APPT.OVERRIDE_YN ,
                    V_SCHED_APPT.UNAVAILABLE_TIME_YN ,
                    V_SCHED_APPT.CHANGE_CNT ,
                    V_SCHED_APPT.JOINT_APPT_YN ,
                    V_SCHED_APPT.PHONE_REM_STAT_C ,
                    V_SCHED_APPT.PHONE_REM_STAT_NAME ,

			--dates/times
                    V_SCHED_APPT.CONTACT_DATE ,
                    V_SCHED_APPT.APPT_MADE_DATE ,
                    V_SCHED_APPT.APPT_CANC_DATE ,
                    V_SCHED_APPT.APPT_CONF_DTTM ,
                    V_SCHED_APPT.APPT_DTTM ,
					V_SCHED_APPT.SIGNIN_DTTM,
					V_SCHED_APPT.PAGED_DTTM,
					V_SCHED_APPT.BEGIN_CHECKIN_DTTM,
					V_SCHED_APPT.CHECKIN_DTTM,
					V_SCHED_APPT.ARVL_LIST_REMOVE_DTTM,
					V_SCHED_APPT.ROOMED_DTTM,
					V_SCHED_APPT.FIRST_ROOM_ASSIGN_DTTM,
					V_SCHED_APPT.NURSE_LEAVE_DTTM,
					V_SCHED_APPT.PHYS_ENTER_DTTM,
					V_SCHED_APPT.VISIT_END_DTTM,
					V_SCHED_APPT.CHECKOUT_DTTM,
                    V_SCHED_APPT.TIME_TO_ROOM_MINUTES , --diff between check in time and roomed time (earliest of the ARVL_LIST_REMOVE_DTTM, ROOMED_DTTM, and FIRST_ROOM_ASSIGN_DTTM)
                    V_SCHED_APPT.TIME_IN_ROOM_MINUTES , --diff between roomed time and appointment end time (earlier of the CHECKOUT_DTTM and VISIT_END_DTTM)
                    V_SCHED_APPT.CYCLE_TIME_MINUTES ,	--diff between the check-in time and the appointment end time.

			--Provider/scheduler info
                    V_SCHED_APPT.REFERRING_PROV_ID ,
                    V_SCHED_APPT.REFERRING_PROV_NAME_WID ,
                    V_SCHED_APPT.PROV_ID ,
                    V_SCHED_APPT.PROV_NAME_WID ,
                    CLARITY_SER.RPT_GRP_FIVE , --service line
                    V_SCHED_APPT.APPT_ENTRY_USER_ID ,
                    V_SCHED_APPT.APPT_ENTRY_USER_NAME_WID ,
                    CLARITY_SER.PROV_NAME ,

			--Billing info 
                    V_COVERAGE_PAYOR_PLAN.PAYOR_ID ,
                    V_COVERAGE_PAYOR_PLAN.PAYOR_NAME ,
                    V_COVERAGE_PAYOR_PLAN.BENEFIT_PLAN_ID ,
                    V_COVERAGE_PAYOR_PLAN.BENEFIT_PLAN_NAME ,
                    V_COVERAGE_PAYOR_PLAN.FIN_CLASS_NAME ,
                    PAT_ENC_3.DO_NOT_BILL_INS_YN ,
                    PAT_ENC_3.SELF_PAY_VISIT_YN ,
                    V_SCHED_APPT.HSP_ACCOUNT_ID ,
                    V_SCHED_APPT.ACCOUNT_ID ,
                    V_SCHED_APPT.COPAY_DUE ,
                    V_SCHED_APPT.COPAY_COLLECTED ,
                    V_SCHED_APPT.COPAY_USER_ID ,
                    V_SCHED_APPT.COPAY_USER_NAME_WID ,
	
			 --fac-org info
                    V_SCHED_APPT.DEPARTMENT_ID ,
                    CLARITY_DEP.DEPARTMENT_NAME ,
                    CLARITY_DEP.DEPT_ABBREVIATION ,
                    CLARITY_DEP.RPT_GRP_THIRTY , -- service line
                    CLARITY_DEP.RPT_GRP_SIX , -- pod
                    CLARITY_DEP.RPT_GRP_SEVEN , -- hub
                    V_SCHED_APPT.DEPT_SPECIALTY_C ,
                    V_SCHED_APPT.DEPT_SPECIALTY_NAME ,
                    V_SCHED_APPT.CENTER_C ,
                    V_SCHED_APPT.CENTER_NAME ,
                    V_SCHED_APPT.LOC_ID ,
                    V_SCHED_APPT.LOC_NAME ,
                    V_SCHED_APPT.SERV_AREA_ID ,
	
			 --appt status flag
					CASE
					  WHEN ( V_SCHED_APPT.APPT_STATUS_C = 3 -- Canceled
                               AND V_SCHED_APPT.CANCEL_INITIATOR = 'PATIENT'
                               AND V_SCHED_APPT.CANCEL_LEAD_HOURS < 24) THEN 'Canceled Late'
					  ELSE V_SCHED_APPT.APPT_STATUS_NAME
					END AS APPT_STATUS_FLAG ,
					PAT_ENC_4.VIS_NEW_TO_SPEC_YN ,
					PAT_ENC_4.VIS_NEW_TO_SERV_AREA_YN ,
					PAT_ENC_4.VIS_NEW_TO_LOC_YN ,
					REFERRAL.REFERRAL_ID ,
					REFERRAL.ENTRY_DATE ,
					ZC_RFL_STATUS.NAME AS RFL_STATUS_NAME ,
					ZC_RFL_TYPE.NAME AS RFL_TYPE_NAME ,
					CLARITY_SER_SPEC.SPECIALTY_C AS PROV_SPECIALTY_C ,
					ZC_SPECIALTY.NAME AS PROV_SPECIALTY_NAME ,
					CAST(CAST(V_SCHED_APPT.APPT_DTTM AS DATE) AS DATETIME) AS APPT_DT ,
					PAT_ENC.ENC_TYPE_C ,
					ZC_DISP_ENC_TYPE.TITLE AS ENC_TYPE_TITLE ,
					V_SCHED_APPT.APPT_CONF_STAT_NAME ,
					CAST(SUBSTRING(PATIENT.ZIP, 1, 5) AS VARCHAR(5)) AS ZIP ,
					ZC_SER_RPT_GRP_6.NAME AS SER_RPT_GRP_SIX ,
					ZC_SER_RPT_GRP_8.NAME AS SER_RPT_GRP_EIGHT ,
					CASE
					  WHEN PAT_ENC.ENC_TYPE_C IN (				-- FACE TO FACE UVA DEFINED ENCOUNTER TYPES
			                                      '1001'			--Anti-coag visit
			                                     ,'50'			--Appointment
			                                     ,'213'			--Dentistry Visit
			                                     ,'2103500001'	--Home Visit
			                                     ,'3'			--Hospital Encounter
			                                     ,'108'			--Immunization
			                                     ,'1201'			--Initial Prenatal
			                                     ,'101'			--Office Visit
			                                     ,'2100700001'	--Office Visit / FC
			                                     ,'1003'			--Procedure visit
			                                     ,'1200'			--Routine Prenatal
			                                     ) THEN 1
                      ELSE 0
					END AS F2F_Flag ,
					V_SCHED_APPT.APPT_CANC_DTTM ,
					V_SCHED_APPT.APPT_CANC_USER_ID ,
					V_SCHED_APPT.APPT_CANC_USER_NAME_WID ,
					V_SCHED_APPT.APPT_CONF_USER_ID ,
					V_SCHED_APPT.APPT_CONF_USER_NAME ,
					REFERRAL_HIST.CHANGE_DATE ,
					V_SCHED_APPT.APPT_MADE_DTTM ,
                    'Rptg.uspLoad_Scheduled_Appointment' AS ETL_guid ,
					GETDATE() AS Load_Dte
          FROM      CLARITY.dbo.V_SCHED_APPT V_SCHED_APPT
                    INNER JOIN CLARITY.dbo.PATIENT PATIENT						
					  ON V_SCHED_APPT.PAT_ID = PATIENT.PAT_ID
                    INNER JOIN CLARITY.dbo.PAT_ENC AS PAT_ENC				
					  ON PAT_ENC.PAT_ENC_CSN_ID = V_SCHED_APPT.PAT_ENC_CSN_ID
                    INNER JOIN CLARITY.dbo.PAT_ENC_4 AS PAT_ENC_4				
					  ON PAT_ENC_4.PAT_ENC_CSN_ID = V_SCHED_APPT.PAT_ENC_CSN_ID
                    LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP CLARITY_DEP		
					  ON V_SCHED_APPT.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
                    LEFT OUTER JOIN CLARITY.dbo.PAT_ENC_3 PAT_ENC_3			
					  ON V_SCHED_APPT.PAT_ENC_CSN_ID = PAT_ENC_3.PAT_ENC_CSN
                    LEFT OUTER JOIN CLARITY.dbo.V_COVERAGE_PAYOR_PLAN V_COVERAGE_PAYOR_PLAN 
					  ON ( ( V_SCHED_APPT.CONTACT_DATE >= V_COVERAGE_PAYOR_PLAN.EFF_DATE )
                            AND
						   ( V_SCHED_APPT.CONTACT_DATE <= V_COVERAGE_PAYOR_PLAN.TERM_DATE )
                         )
                      AND ( V_SCHED_APPT.COVERAGE_ID = V_COVERAGE_PAYOR_PLAN.COVERAGE_ID )
                    LEFT OUTER JOIN CLARITY.dbo.PATIENT_MYC PATIENT_MYC		
					  ON V_SCHED_APPT.PAT_ID = PATIENT_MYC.PAT_ID
                    LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER	AS CLARITY_SER				
					  ON V_SCHED_APPT.PROV_ID = CLARITY_SER.PROV_ID
                    LEFT OUTER JOIN CLARITY.dbo.IDENTITY_ID	AS IDENTITY_ID			
					  ON IDENTITY_ID.PAT_ID = V_SCHED_APPT.PAT_ID AND IDENTITY_ID.IDENTITY_TYPE_ID = 14
					LEFT OUTER JOIN CLARITY.dbo.REFERRAL AS REFERRAL
					  ON REFERRAL.REFERRAL_ID = V_SCHED_APPT.REFERRAL_ID
					LEFT OUTER JOIN (SELECT PROV_ID, SPECIALTY_C
					                 FROM CLARITY.dbo.CLARITY_SER_SPEC
									 WHERE LINE = 1) AS CLARITY_SER_SPEC
					  ON CLARITY_SER_SPEC.PROV_ID = CLARITY_SER.PROV_ID
					LEFT OUTER JOIN (SELECT REFERRAL_ID, CHANGE_DATE
					                 FROM CLARITY.dbo.REFERRAL_HIST
									 WHERE CHANGE_TYPE_C = 1) AS REFERRAL_HIST
					  ON REFERRAL_HIST.REFERRAL_ID = REFERRAL.REFERRAL_ID
                    LEFT OUTER JOIN CLARITY.dbo.ZC_MYCHART_STATUS AS ZC_MYCHART_STATUS	
					  ON ZC_MYCHART_STATUS.MYCHART_STATUS_C = PATIENT_MYC.MYCHART_STATUS_C
                    LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_STATUS AS ZC_RFL_STATUS
                      ON ZC_RFL_STATUS.RFL_STATUS_C = REFERRAL.RFL_STATUS_C
                    LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_TYPE AS ZC_RFL_TYPE
                      ON ZC_RFL_TYPE.RFL_TYPE_C = REFERRAL.RFL_TYPE_C
					LEFT OUTER JOIN CLARITY.dbo.ZC_SPECIALTY AS ZC_SPECIALTY
					  ON ZC_SPECIALTY.SPECIALTY_C = CLARITY_SER_SPEC.SPECIALTY_C
					LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE AS ZC_DISP_ENC_TYPE
					  ON ZC_DISP_ENC_TYPE.DISP_ENC_TYPE_C = PAT_ENC.ENC_TYPE_C
					LEFT OUTER JOIN CLARITY.dbo.ZC_SER_RPT_GRP_6 AS ZC_SER_RPT_GRP_6
					  ON ZC_SER_RPT_GRP_6.RPT_GRP_SIX = CLARITY_SER.RPT_GRP_SIX
					LEFT OUTER JOIN CLARITY.dbo.ZC_SER_RPT_GRP_8 AS ZC_SER_RPT_GRP_8
					  ON ZC_SER_RPT_GRP_8.RPT_GRP_EIGHT = CLARITY_SER.RPT_GRP_EIGHT
          WHERE     1 = 1
                    AND V_SCHED_APPT.APPT_DTTM >= @locstartdate
                    AND V_SCHED_APPT.APPT_DTTM <  @locenddate
 ) appt

 ORDER BY PAT_ENC_CSN_ID

GO


