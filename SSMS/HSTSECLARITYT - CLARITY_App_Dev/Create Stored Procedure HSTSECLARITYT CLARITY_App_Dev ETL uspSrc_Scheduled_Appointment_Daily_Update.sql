USE [CLARITY_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [ETL].[uspSrc_Scheduled_Appointment_Daily_Update]
(
@startdate SMALLDATETIME=NULL,
@enddate SMALLDATETIME=NULL,
@lastmonthlyloaddtm SMALLDATETIME
)
AS
/****************************************************************************************************************************************
WHAT: Create procedure ETL.uspSrc_Scheduled_Appointment_Daily_Update
WHO : Tom Burgan
WHEN: 01/16/2019
WHY : Update staging table (HSTSDSSQLDMT DS_HSDM_App_Dev Stage.Scheduled_Appointment) containing Cadence scheduled appointment detail.
      To facilitate the reporting of current Ambulatory Optimization metrics.
----------------------------------------------------------------------------------------------------------------------------------------
INFO: 
      INPUTS:	HSTSECLARITY.CLARITY.dbo.F_SCHED_APPT
	            HSTSECLARITY.CLARITY.dbo.HSP_ACCOUNT
                HSTSECLARITY.CLARITY.dbo.ZC_APPT_STATUS
                HSTSECLARITY.CLARITY.dbo.PAT_ENC
                HSTSECLARITY.CLARITY.dbo.CLARITY_EMP
                HSTSECLARITY.CLARITY.dbo.CLARITY_DEP
                HSTSECLARITY.CLARITY.dbo.ZC_CENTER
                HSTSECLARITY.CLARITY.dbo.CLARITY_LOC
                HSTSECLARITY.CLARITY.dbo.CLARITY_SA
                HSTSECLARITY.CLARITY.dbo.CLARITY_SER
                HSTSECLARITY.CLARITY.dbo.CLARITY_PRC
                HSTSECLARITY.CLARITY.dbo.ZC_APPT_BLOCK
                HSTSECLARITY.CLARITY.dbo.ZC_CANCEL_REASON
                HSTSECLARITY.CLARITY.dbo.CLARITY_LWS
                HSTSECLARITY.CLARITY.dbo.ZC_APPT_CONF_STAT
                HSTSECLARITY.CLARITY.dbo.ZC_PHONE_REM_STAT
				HSTSECLARITY.CLARITY.dbo.PATIENT
                HSTSECLARITY.CLARITY.dbo.PAT_ENC_4
                HSTSECLARITY.CLARITY.dbo.PAT_ENC_3
                HSTSECLARITY.CLARITY.dbo.V_COVERAGE_PAYOR_PLAN
                HSTSECLARITY.CLARITY.dbo.PATIENT_MYC
                HSTSECLARITY.CLARITY.dbo.IDENTITY_ID
		        HSTSECLARITY.CLARITY.dbo.REFERRAL
		        HSTSECLARITY.CLARITY.dbo.CLARITY_SER_SPEC
		        HSTSECLARITY.CLARITY.dbo.REFERRAL_HIST
                HSTSECLARITY.CLARITY.dbo.ZC_MYCHART_STATUS
                HSTSECLARITY.CLARITY.dbo.ZC_RFL_STATUS
                HSTSECLARITY.CLARITY.dbo.ZC_RFL_TYPE
		        HSTSECLARITY.CLARITY.dbo.ZC_SPECIALTY
		        HSTSECLARITY.CLARITY.dbo.ZC_DISP_ENC_TYPE
		        HSTSECLARITY.CLARITY.dbo.ZC_SER_RPT_GRP_6
		        HSTSECLARITY.CLARITY.dbo.ZC_SER_RPT_GRP_8
                  
      OUTPUTS:  CLARITY_App.ETL.uspSrc_Scheduled_Appointment_Daily_Update
   
----------------------------------------------------------------------------------------------------------------------------------------
MODS: 	01/16/2019--TMB--Create stored procedure
*****************************************************************************************************************************************/

SET NOCOUNT ON;
 
----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------

---BDD 12/7/2017 - let's toy with the parameters a bit to eliminate the possibility of parameter sniffing errors. 

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME,
		@locloaddtm SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate
SET @locloaddtm   = @lastmonthlyloaddtm
------------------------------------------------------------------------------------------------------



 SELECT appt.PAT_ENC_CSN_ID ,
        appt.PAT_ID ,
        appt.PAT_NAME ,
        appt.IDENTITY_ID , --MRN
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
		appt.UPDATE_DATE , -- DATETIME
		appt.APPT_MADE_DTTM -- DATETIME
 FROM 
 (
/*  All scheduled appointments with patient, MyChart, encounter, department, insurance, and referral detail  */
          SELECT 

			--patient info
                    F_SCHED_APPT.PAT_ENC_CSN_ID ,
                    F_SCHED_APPT.PAT_ID ,
                    PATIENT.PAT_NAME ,
                    IDENTITY_ID.IDENTITY_ID , --MRN
                    PATIENT_MYC.MYCHART_STATUS_C ,
                    ZC_MYCHART_STATUS.NAME AS MYCHART_STATUS_NAME ,
                    PAT_ENC_4.VIS_NEW_TO_SYS_YN ,
                    PAT_ENC_4.VIS_NEW_TO_DEP_YN ,
                    PAT_ENC_4.VIS_NEW_TO_PROV_YN ,

			--appt info
                    F_SCHED_APPT.APPT_SERIAL_NUM ,
                    F_SCHED_APPT.RESCHED_APPT_CSN_ID ,
                    F_SCHED_APPT.PRC_ID ,
                    F_SCHED_APPT.PRC_NAME ,
                    F_SCHED_APPT.APPT_BLOCK_C ,
                    F_SCHED_APPT.APPT_BLOCK_NAME ,
                    F_SCHED_APPT.APPT_LENGTH ,
                    F_SCHED_APPT.APPT_STATUS_C ,
                    F_SCHED_APPT.APPT_STATUS_NAME ,
                    F_SCHED_APPT.APPT_CONF_STAT_C ,
                    F_SCHED_APPT.CANCEL_LEAD_HOURS ,
                    F_SCHED_APPT.CANCEL_INITIATOR ,
                    F_SCHED_APPT.COMPLETED_STATUS_YN ,
                    F_SCHED_APPT.SAME_DAY_YN ,
                    F_SCHED_APPT.SAME_DAY_CANC_YN ,
                    F_SCHED_APPT.CANCEL_REASON_C ,
                    F_SCHED_APPT.CANCEL_REASON_NAME ,
                    F_SCHED_APPT.WALK_IN_YN ,
                    F_SCHED_APPT.OVERBOOKED_YN ,
                    F_SCHED_APPT.OVERRIDE_YN ,
                    F_SCHED_APPT.UNAVAILABLE_TIME_YN ,
                    F_SCHED_APPT.CHANGE_CNT ,
                    F_SCHED_APPT.JOINT_APPT_YN ,
                    F_SCHED_APPT.PHONE_REM_STAT_C ,
                    --F_SCHED_APPT.PHONE_REM_STAT_NAME ,
                    ZC_PHONE_REM_STAT.NAME AS PHONE_REM_STAT_NAME ,

			--dates/times
                    F_SCHED_APPT.CONTACT_DATE ,
                    F_SCHED_APPT.APPT_MADE_DATE ,
                    F_SCHED_APPT.APPT_CANC_DATE ,
                    F_SCHED_APPT.APPT_CONF_DTTM ,
                    F_SCHED_APPT.APPT_DTTM ,
					F_SCHED_APPT.SIGNIN_DTTM,
					F_SCHED_APPT.PAGED_DTTM,
					F_SCHED_APPT.BEGIN_CHECKIN_DTTM,
					F_SCHED_APPT.CHECKIN_DTTM,
					F_SCHED_APPT.ARVL_LIST_REMOVE_DTTM,
					F_SCHED_APPT.ROOMED_DTTM,
					F_SCHED_APPT.FIRST_ROOM_ASSIGN_DTTM,
					F_SCHED_APPT.NURSE_LEAVE_DTTM,
					F_SCHED_APPT.PHYS_ENTER_DTTM,
					F_SCHED_APPT.VISIT_END_DTTM,
					F_SCHED_APPT.CHECKOUT_DTTM,
                    F_SCHED_APPT.TIME_TO_ROOM_MINUTES , --diff between check in time and roomed time (earliest of the ARVL_LIST_REMOVE_DTTM, ROOMED_DTTM, and FIRST_ROOM_ASSIGN_DTTM)
                    F_SCHED_APPT.TIME_IN_ROOM_MINUTES , --diff between roomed time and appointment end time (earlier of the CHECKOUT_DTTM and VISIT_END_DTTM)
                    F_SCHED_APPT.CYCLE_TIME_MINUTES ,	--diff between the check-in time and the appointment end time.

			--Provider/scheduler info
                    F_SCHED_APPT.REFERRING_PROV_ID ,
                    F_SCHED_APPT.REFERRING_PROV_NAME_WID ,
                    F_SCHED_APPT.PROV_ID ,
                    F_SCHED_APPT.PROV_NAME_WID ,
                    CLARITY_SER.RPT_GRP_FIVE , --service line
                    F_SCHED_APPT.APPT_ENTRY_USER_ID ,
                    F_SCHED_APPT.APPT_ENTRY_USER_NAME_WID ,
                    CLARITY_SER.PROV_NAME ,

			--Billing info 
                    V_COVERAGE_PAYOR_PLAN.PAYOR_ID ,
                    V_COVERAGE_PAYOR_PLAN.PAYOR_NAME ,
                    V_COVERAGE_PAYOR_PLAN.BENEFIT_PLAN_ID ,
                    V_COVERAGE_PAYOR_PLAN.BENEFIT_PLAN_NAME ,
                    V_COVERAGE_PAYOR_PLAN.FIN_CLASS_NAME ,
                    PAT_ENC_3.DO_NOT_BILL_INS_YN ,
                    PAT_ENC_3.SELF_PAY_VISIT_YN ,
                    F_SCHED_APPT.HSP_ACCOUNT_ID ,
                    F_SCHED_APPT.ACCOUNT_ID ,
                    F_SCHED_APPT.COPAY_DUE ,
                    F_SCHED_APPT.COPAY_COLLECTED ,
                    F_SCHED_APPT.COPAY_USER_ID ,
                    F_SCHED_APPT.COPAY_USER_NAME_WID ,
	
			 --fac-org info
                    F_SCHED_APPT.DEPARTMENT_ID ,
                    CLARITY_DEP.DEPARTMENT_NAME ,
                    CLARITY_DEP.DEPT_ABBREVIATION ,
                    CLARITY_DEP.RPT_GRP_THIRTY , -- service line
                    CLARITY_DEP.RPT_GRP_SIX , -- pod
                    CLARITY_DEP.RPT_GRP_SEVEN , -- hub
                    F_SCHED_APPT.DEPT_SPECIALTY_C ,
                    F_SCHED_APPT.DEPT_SPECIALTY_NAME ,
                    F_SCHED_APPT.CENTER_C ,
                    F_SCHED_APPT.CENTER_NAME ,
                    F_SCHED_APPT.LOC_ID ,
                    F_SCHED_APPT.LOC_NAME ,
                    F_SCHED_APPT.SERV_AREA_ID ,
	
			 --appt status flag
					CASE
					  WHEN ( F_SCHED_APPT.APPT_STATUS_C = 3 -- Canceled
                               AND F_SCHED_APPT.CANCEL_INITIATOR = 'PATIENT'
                               AND F_SCHED_APPT.CANCEL_LEAD_HOURS < 24) THEN 'Canceled Late'
					  ELSE F_SCHED_APPT.APPT_STATUS_NAME
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
					CAST(CAST(F_SCHED_APPT.APPT_DTTM AS DATE) AS DATETIME) AS APPT_DT ,
					PAT_ENC.ENC_TYPE_C ,
					ZC_DISP_ENC_TYPE.TITLE AS ENC_TYPE_TITLE ,
					F_SCHED_APPT.APPT_CONF_STAT_NAME ,
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
					F_SCHED_APPT.APPT_CANC_DTTM ,
					F_SCHED_APPT.APPT_CANC_USER_ID ,
					F_SCHED_APPT.APPT_CANC_USER_NAME_WID ,
					F_SCHED_APPT.APPT_CONF_USER_ID ,
					F_SCHED_APPT.APPT_CONF_USER_NAME ,
					REFERRAL_HIST.CHANGE_DATE ,
					F_SCHED_APPT.UPDATE_DATE ,
                    F_SCHED_APPT.APPT_MADE_DTTM ,
                    'Rptg.uspSrc_Scheduled_Appointment_Daily_Update' AS ETL_guid ,
					GETDATE() AS Load_Dte
          FROM
		  (    
                   SELECT
                             mart.PAT_ENC_CSN_ID
                           , mart.UPDATE_DATE	
                           , mart.CONTACT_DATE	
                           , mart.PAT_ID	
                           , mart.APPT_STATUS_C	
                           , CASE 
                                  WHEN mart.APPT_STATUS_C = 1 AND mart.SIGNIN_DTTM IS NOT NULL THEN 'Present'
                                  WHEN zcappt.NAME IS NOT NULL THEN zcappt.NAME
                                  ELSE 
                                      CASE
                                           WHEN zcappt.APPT_STATUS_C IS NULL THEN '*Unknown status'
                                           ELSE '*Unnamed status'
                                      END + ' [' + CONVERT(VARCHAR, mart.APPT_STATUS_C) + ']'
                             END AS APPT_STATUS_NAME
                           , mart.DEPARTMENT_ID	
                           , COALESCE(dep.DEPARTMENT_NAME,
                                      CASE
                                          WHEN mart.DEPARTMENT_ID IS NULL THEN '*Unspecified department'
                                          ELSE
                                              CASE 
                                                  WHEN dep.DEPARTMENT_ID IS NULL THEN '*Unknown department'
                                                  ELSE '*Unnamed department'
                                              END + ' [' + CONVERT(VARCHAR, mart.DEPARTMENT_ID) + ']'
                                      END
                             ) AS DEPARTMENT_NAME
                           , dep.specialty_dep_c AS DEPT_SPECIALTY_C
                           , CASE
                                 WHEN dep.department_id IS NULL THEN '*Unknown department'
                                 WHEN dep.specialty IS NULL THEN '*No specialty'
                                 ELSE dep.specialty
                             END AS DEPT_SPECIALTY_NAME
                           , dep.center_c AS CENTER_C
                           , COALESCE(zccenter.name,
                                      CASE
                                          WHEN dep.department_id IS NULL THEN '*Unknown department'
                                          WHEN dep.center_c IS NULL THEN '*No center'
                                          ELSE '*Unknown center [' + dep.center_c + ']'
                                      END) AS CENTER_NAME
                           , dep.rev_loc_id AS LOC_ID
                           , COALESCE(loc.loc_name,
                                      CASE
                                          WHEN dep.department_id IS NULL THEN '*Unknown department'
                                          WHEN dep.rev_loc_id IS NULL THEN '*No location'
                                          ELSE '*Unknown location [' + CAST(dep.rev_loc_id AS VARCHAR(18)) + ']'
                                      END) AS LOC_NAME
                           , dep.serv_area_id AS SERV_AREA_ID
                           , COALESCE(servarea.serv_area_name,
                                      CASE
                                          WHEN dep.department_id IS NULL THEN '*Unknown department'
                                          WHEN dep.serv_area_id IS NULL THEN '*No service area'
                                          ELSE '*Unknown service area [' + CAST(dep.serv_area_id AS VARCHAR(18)) + ']'
                                      END) AS SERV_AREA_NAME
                           , mart.PROV_ID	
                           , CASE
                               WHEN mart.PROV_ID IS NULL
                                   THEN '*Unspecified provider'
                               ELSE
                                   CASE
                                       WHEN apptprov.prov_name IS NOT NULL
                                           THEN apptprov.prov_name
                                       WHEN apptprov.prov_id IS NULL
                                           THEN '*Unknown provider'
                                       ELSE '*Unnamed provider'
                                   END + ' [' + mart.prov_id + ']'
                             END AS PROV_NAME_WID
                           , mart.PRC_ID	
                           , COALESCE(prc.PRC_NAME,
                                      CASE
                                           WHEN mart.PRC_ID IS NULL THEN '*Unspecified visit type'
                                           ELSE
                                               CASE 
                                                   WHEN prc.PRC_ID IS NULL THEN '*Unknown visit type'
                                                   ELSE '*Unnamed visit type'
                                               END + ' [' + mart.PRC_ID + ']'
                                      END
                             ) AS PRC_NAME
                           , mart.APPT_MADE_DTTM	
                           , mart.APPT_MADE_DATE	
                           , mart.SAME_DAY_YN	
                           , mart.APPT_ENTRY_USER_ID	
                           , CASE
                               WHEN mart.APPT_ENTRY_USER_ID IS NULL
                                   THEN '*Unspecified user'
                               ELSE
                                   CASE
                                       WHEN entryemp.USER_ID IS NULL 
                                           THEN '*Unknown user'
                                       WHEN entryemp.NAME IS NULL
                                           THEN '*Unnamed user'
                                       ELSE entryemp.NAME
                                   END + ' [' + mart.appt_entry_user_id + ']'
                             END AS APPT_ENTRY_USER_NAME_WID
                           , mart.APPT_BLOCK_C	
                           , CASE
                                 WHEN mart.APPT_BLOCK_C IS NULL THEN NULL
                                 ELSE COALESCE(zcblock.NAME,
                                               CASE
                                                   WHEN zcblock.appt_block_c IS NULL
                                                       THEN '*Unknown block'
                                                   ELSE '*Unnamed block'
                                               END + ' [' + CONVERT(VARCHAR, mart.appt_block_c) + ']'
                                              )
                             END AS APPT_BLOCK_NAME
                           , mart.APPT_LENGTH	
                           , mart.APPT_DTTM	
                           , mart.SIGNIN_DTTM	
                           , mart.PAGED_DTTM	
                           , mart.BEGIN_CHECKIN_DTTM	
                           , mart.CHECKIN_DTTM	
                           , mart.ARVL_LIST_REMOVE_DTTM	
                           , mart.ROOMED_DTTM	
                           , mart.FIRST_ROOM_ASSIGN_DTTM	
                           , mart.NURSE_LEAVE_DTTM	
                           , mart.PHYS_ENTER_DTTM	
                           , mart.VISIT_END_DTTM	
                           , mart.CHECKOUT_DTTM	
                           , DATEDIFF(
                               MINUTE
                               , mart.CHECKIN_DTTM
                               , (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
                                  FROM (VALUES(CASE WHEN mart.arvl_list_remove_dttm >= mart.checkin_dttm THEN mart.arvl_list_remove_dttm ELSE NULL END),
                                              (CASE WHEN mart.roomed_dttm >= mart.checkin_dttm THEN mart.roomed_dttm ELSE NULL END), 
                                              (CASE WHEN mart.first_room_assign_dttm >= mart.checkin_dttm THEN mart.first_room_assign_dttm ELSE NULL END)
                                       ) AS Roomed_Cols(candidate_dttm))
                             ) AS TIME_TO_ROOM_MINUTES
                           , DATEDIFF(
                               MINUTE
                               , (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
                                  FROM (VALUES(CASE WHEN mart.arvl_list_remove_dttm >= mart.checkin_dttm THEN mart.arvl_list_remove_dttm ELSE NULL END),
                                              (CASE WHEN mart.roomed_dttm >= mart.checkin_dttm THEN mart.roomed_dttm ELSE NULL END), 
                                              (CASE WHEN mart.first_room_assign_dttm >= mart.checkin_dttm THEN mart.first_room_assign_dttm ELSE NULL END)
                                       ) AS Roomed_Cols(candidate_dttm))
                               , (CASE WHEN mart.visit_end_dttm >= 
                                             (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
                                              FROM (VALUES(CASE WHEN mart.arvl_list_remove_dttm >= mart.checkin_dttm THEN mart.arvl_list_remove_dttm ELSE NULL END),
                                                          (CASE WHEN mart.roomed_dttm >= mart.checkin_dttm THEN mart.roomed_dttm ELSE NULL END), 
                                                          (CASE WHEN mart.first_room_assign_dttm >= mart.checkin_dttm THEN mart.first_room_assign_dttm ELSE NULL END)
                                                   ) AS Roomed_Cols(candidate_dttm))
                                             AND (mart.checkout_dttm IS NULL OR mart.visit_end_dttm <= mart.checkout_dttm)
                                             THEN mart.visit_end_dttm
                                       WHEN mart.checkout_dttm >= 
                                               (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
                                                FROM (VALUES(CASE WHEN mart.arvl_list_remove_dttm >= mart.checkin_dttm THEN mart.arvl_list_remove_dttm ELSE NULL END),
                                                            (CASE WHEN mart.roomed_dttm >= mart.checkin_dttm THEN mart.roomed_dttm ELSE NULL END), 
                                                            (CASE WHEN mart.first_room_assign_dttm >= mart.checkin_dttm THEN mart.first_room_assign_dttm ELSE NULL END)
                                                     ) AS Roomed_Cols(candidate_dttm))
                                                AND (mart.visit_end_dttm IS NULL OR mart.checkout_dttm < mart.visit_end_dttm)
                                               THEN mart.checkout_dttm
                                       ELSE NULL
                                 END)
                             ) AS TIME_IN_ROOM_MINUTES
                           , DATEDIFF(
                                 MINUTE
                               , mart.CHECKIN_DTTM
                               , CASE 
                                   WHEN mart.VISIT_END_DTTM >= mart.CHECKIN_DTTM
                                        AND (mart.CHECKOUT_DTTM IS NULL 
                                           OR mart.CHECKOUT_DTTM < mart.CHECKIN_DTTM
                                           OR mart.VISIT_END_DTTM <= mart.CHECKOUT_DTTM)
                                        THEN mart.VISIT_END_DTTM
                                   WHEN mart.CHECKOUT_DTTM >= mart.CHECKIN_DTTM
                                        THEN mart.CHECKOUT_DTTM
                                   ELSE NULL
                                 END) AS CYCLE_TIME_MINUTES
                           , mart.APPT_CANC_USER_ID	
                           , CASE
                               WHEN mart.APPT_CANC_USER_ID IS NULL THEN NULL
                               ELSE
                                   CASE
                                       WHEN cancemp.USER_ID IS NULL 
                                           THEN '*Unknown user'
                                       WHEN cancemp.NAME IS NULL
                                           THEN '*Unnamed user'
                                       ELSE cancemp.NAME
                                   END + ' [' + mart.appt_canc_user_id + ']'
                             END AS APPT_CANC_USER_NAME_WID
                           , mart.APPT_CANC_DTTM	
                           , mart.APPT_CANC_DATE	
                           , mart.CANCEL_REASON_C	
                           , CASE
                                 WHEN mart.CANCEL_REASON_C IS NULL THEN NULL
                                 ELSE COALESCE(canc.NAME,
                                               CASE
                                                   WHEN canc.cancel_reason_c IS NULL
                                                       THEN '*Unknown cancel reason'
                                                   ELSE '*Unnamed cancel reason'
                                               END + ' [' + CONVERT(VARCHAR, mart.cancel_reason_c) + ']'
                                           )
                             END AS CANCEL_REASON_NAME
                           , CASE
                                 WHEN mart.CANCEL_REASON_C IS NULL THEN NULL
                                 WHEN canc.CANCEL_REASON_C IS NULL THEN '*Unknown cancel reason [' + CONVERT(VARCHAR, mart.cancel_reason_c) + ']'
                                 WHEN (SELECT COUNT(PAT_INIT_CANC_C) FROM CLARITY.dbo.PAT_INIT_CANC 
                                       WHERE PAT_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PATIENT'
                                 WHEN (SELECT COUNT(PROV_INIT_CANC_C) FROM CLARITY.dbo.PROV_INIT_CANC 
                                       WHERE PROV_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PROVIDER'
                                 ELSE 'OTHER'
                             END AS CANCEL_INITIATOR
                           , mart.SAME_DAY_CANC_YN	
                           , mart.APPT_SERIAL_NUM	
                           , mart.RESCHED_APPT_CSN_ID	
                           , mart.REFERRAL_ID	
                           , mart.REFERRAL_REQ_YN	
                           , mart.REFERRING_PROV_ID	
                           , CASE
                               WHEN mart.REFERRING_PROV_ID IS NULL THEN NULL
                               ELSE
                                   CASE
                                       WHEN refprov.prov_name IS NOT NULL
                                           THEN refprov.prov_name
                                       WHEN refprov.prov_id IS NULL
                                           THEN '*Unknown provider'
                                       ELSE '*Unnamed provider'
                                   END + ' [' + mart.referring_prov_id + ']'
                             END AS REFERRING_PROV_NAME_WID
                           , mart.ACCOUNT_ID	
                           , COALESCE(mart.COVERAGE_ID, hsp.COVERAGE_ID) AS COVERAGE_ID
                           , mart.CHARGE_SLIP_NUMBER	
                           , mart.HSP_ACCOUNT_ID	
                           , mart.APPT_CONF_STAT_C	
                           , zcconf.NAME AS APPT_CONF_STAT_NAME
                           , mart.APPT_CONF_USER_ID	
                           , CASE
                                 WHEN mart.APPT_CONF_USER_ID IS NOT NULL 
                                   THEN 
                                       CASE
                                           WHEN confemp.USER_ID IS NULL THEN '*Unknown user'
                                           ELSE COALESCE(confemp.NAME, '*Unnamed user')
                                       END + ' [' + mart.APPT_CONF_USER_ID + ']'
                                   ELSE pe.APPT_CONF_PERS
                             END AS APPT_CONF_USER_NAME
                           , mart.APPT_CONF_DTTM	
                           , mart.SCHED_FROM_KIOSK_ID	
                           , CASE
                                 WHEN mart.SCHED_FROM_KIOSK_ID IS NULL THEN NULL
                                 ELSE COALESCE(schedlws.WORKSTATION_NAME,
                                               CASE
                                                   WHEN schedlws.WORKSTN_IDENTIFIER IS NULL THEN '*Unknown kiosk'
                                                   ELSE '*Unnamed kiosk [' + schedlws.WORKSTN_IDENTIFIER + ']'
                                               END
                                           )
                             END AS SCHED_FROM_KIOSK_NAME
                           , mart.CHECK_IN_KIOSK_ID	
                           , CASE
                                 WHEN mart.CHECK_IN_KIOSK_ID IS NULL THEN NULL
                                 ELSE COALESCE(chkinlws.WORKSTATION_NAME,
                                               CASE
                                                   WHEN chkinlws.WORKSTN_IDENTIFIER IS NULL THEN '*Unknown kiosk'
                                                   ELSE '*Unnamed kiosk [' + chkinlws.WORKSTN_IDENTIFIER + ']'
                                               END
                                           )
                             END AS CHECK_IN_KIOSK_NAME
                           , mart.CHECK_OUT_KIOSK_ID	
                           , CASE
                                 WHEN mart.CHECK_OUT_KIOSK_ID IS NULL THEN NULL
                                 ELSE COALESCE(chkoutlws.WORKSTATION_NAME,
                                               CASE
                                                   WHEN chkoutlws.WORKSTN_IDENTIFIER IS NULL THEN '*Unknown kiosk'
                                                   ELSE '*Unnamed kiosk [' + chkoutlws.WORKSTN_IDENTIFIER + ']'
                                               END
                                           )
                             END AS CHECK_OUT_KIOSK_NAME
                           , mart.IP_DOC_CONTACT_CSN	
                           , mart.WALK_IN_YN	
                           , mart.SEQUENTIAL_YN	
                           , mart.CNS_WARNING_OVERRIDDEN_YN	
                           , mart.OVERBOOKED_YN	
                           , mart.OVERRIDE_YN	
                           , mart.UNAVAILABLE_TIME_YN	
                           , mart.NUMBER_OF_CALLS	
                           , mart.CHANGE_CNT	
                           , mart.JOINT_APPT_YN	
                           , mart.CM_CT_OWNER_ID	
                           , mart.PHONE_REM_STAT_C	
                           , CASE WHEN mart.PHONE_REM_STAT_C IS NULL THEN NULL
                                  ELSE
                                      CASE
                                          WHEN zcrem.PHONE_REM_STAT_C IS NULL THEN '*Unknown phone reminder status'
                                          WHEN zcrem.NAME IS NULL THEN '*Unnamed phone reminder status [' + CONVERT(VARCHAR, zcrem.PHONE_REM_STAT_C) + ']'
				                          /* 09/19/18 UVA fix */
				                          ELSE zcrem.NAME
				                          /* 09/19/18 End fix */
                                      END 
                             END AS PHONE_REM_STAT_NAME
                           , mart.COPAY_DUE	
                           , mart.COPAY_COLLECTED	
                           , mart.COPAY_USER_ID	
                           , CASE WHEN mart.COPAY_USER_ID IS NULL THEN NULL
                                  ELSE
                                    CASE
                                       WHEN copayemp.USER_ID IS NULL 
                                           THEN '*Unknown user'
                                       WHEN copayemp.NAME IS NULL
                                           THEN '*Unnamed user'
                                       ELSE copayemp.NAME
                                    END + ' [' + mart.copay_user_id + ']'
                             END AS COPAY_USER_NAME_WID
                           , CASE WHEN mart.APPT_STATUS_C IN (2) THEN 'Y' ELSE 'N' END AS COMPLETED_STATUS_YN
                           , DATEPART(HOUR, mart.APPT_DTTM) AS HOUR_OF_DAY
                           , DATEDIFF(MINUTE, mart.APPT_CANC_DTTM, mart.APPT_DTTM) / 60 AS CANCEL_LEAD_HOURS
                   FROM
                       CLARITY.dbo.F_SCHED_APPT mart
                       LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT hsp ON mart.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
                       LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_STATUS zcappt ON mart.APPT_STATUS_C = zcappt.APPT_STATUS_C
                       LEFT OUTER JOIN CLARITY.dbo.PAT_ENC pe ON mart.PAT_ENC_CSN_ID = pe.PAT_ENC_CSN_ID
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP confemp ON mart.APPT_CONF_USER_ID = confemp.USER_ID
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep ON mart.DEPARTMENT_ID = dep.DEPARTMENT_ID
                       LEFT OUTER JOIN CLARITY.dbo.ZC_CENTER zccenter ON dep.center_c = zccenter.center_c
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_LOC loc ON dep.rev_loc_id = loc.loc_id
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_SA servarea ON dep.serv_area_id = servarea.serv_area_id
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER apptprov ON mart.PROV_ID = apptprov.PROV_ID
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_PRC prc ON mart.PRC_ID = prc.PRC_ID
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP entryemp ON mart.APPT_ENTRY_USER_ID = entryemp.USER_ID
                       LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_BLOCK zcblock ON mart.APPT_BLOCK_C = zcblock.APPT_BLOCK_C
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP cancemp ON mart.APPT_CANC_USER_ID = cancemp.USER_ID
                       LEFT OUTER JOIN CLARITY.dbo.ZC_CANCEL_REASON canc ON mart.CANCEL_REASON_C = canc.CANCEL_REASON_C
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER refprov ON mart.REFERRING_PROV_ID = refprov.PROV_ID
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_LWS schedlws ON mart.SCHED_FROM_KIOSK_ID = schedlws.workstation_id
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_LWS chkinlws ON mart.CHECK_IN_KIOSK_ID = chkinlws.workstation_id
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_LWS chkoutlws ON mart.CHECK_OUT_KIOSK_ID = chkoutlws.workstation_id
                       LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_CONF_STAT zcconf ON mart.APPT_CONF_STAT_C = zcconf.APPT_CONF_STAT_C
                       LEFT OUTER JOIN CLARITY.dbo.ZC_PHONE_REM_STAT zcrem ON mart.PHONE_REM_STAT_C = zcrem.PHONE_REM_STAT_C
                       LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP copayemp ON mart.COPAY_USER_ID = copayemp.USER_ID
                   WHERE
                   CAST(mart.APPT_DTTM AS DATE) >= @locstartdate
                   AND CAST(mart.APPT_DTTM AS DATE) <  @locenddate
                   AND ((mart.UPDATE_DATE >= @locloaddtm) OR (mart.APPT_MADE_DTTM >= @locloaddtm))) F_SCHED_APPT
          INNER JOIN CLARITY.dbo.PATIENT PATIENT						
			ON F_SCHED_APPT.PAT_ID = PATIENT.PAT_ID
          INNER JOIN CLARITY.dbo.PAT_ENC AS PAT_ENC				
			ON PAT_ENC.PAT_ENC_CSN_ID = F_SCHED_APPT.PAT_ENC_CSN_ID
          INNER JOIN CLARITY.dbo.PAT_ENC_4 AS PAT_ENC_4				
			ON PAT_ENC_4.PAT_ENC_CSN_ID = F_SCHED_APPT.PAT_ENC_CSN_ID
          LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP CLARITY_DEP		
			ON F_SCHED_APPT.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
          LEFT OUTER JOIN CLARITY.dbo.PAT_ENC_3 PAT_ENC_3			
			ON F_SCHED_APPT.PAT_ENC_CSN_ID = PAT_ENC_3.PAT_ENC_CSN
          LEFT OUTER JOIN CLARITY.dbo.V_COVERAGE_PAYOR_PLAN V_COVERAGE_PAYOR_PLAN 
		    ON ( ( F_SCHED_APPT.CONTACT_DATE >= V_COVERAGE_PAYOR_PLAN.EFF_DATE )
                  AND
				 ( F_SCHED_APPT.CONTACT_DATE <= V_COVERAGE_PAYOR_PLAN.TERM_DATE )
               )
               AND ( F_SCHED_APPT.COVERAGE_ID = V_COVERAGE_PAYOR_PLAN.COVERAGE_ID )
          LEFT OUTER JOIN CLARITY.dbo.PATIENT_MYC PATIENT_MYC		
			ON F_SCHED_APPT.PAT_ID = PATIENT_MYC.PAT_ID
          LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER	AS CLARITY_SER				
			ON F_SCHED_APPT.PROV_ID = CLARITY_SER.PROV_ID
          LEFT OUTER JOIN CLARITY.dbo.IDENTITY_ID	AS IDENTITY_ID			
			ON IDENTITY_ID.PAT_ID = F_SCHED_APPT.PAT_ID AND IDENTITY_ID.IDENTITY_TYPE_ID = 14
		  LEFT OUTER JOIN CLARITY.dbo.REFERRAL AS REFERRAL
			ON REFERRAL.REFERRAL_ID = F_SCHED_APPT.REFERRAL_ID
		  LEFT OUTER JOIN CLARITY.dbo.ZC_PHONE_REM_STAT AS ZC_PHONE_REM_STAT
			ON ZC_PHONE_REM_STAT.PHONE_REM_STAT_C = F_SCHED_APPT.PHONE_REM_STAT_C
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
 ) appt

 ORDER BY PAT_ENC_CSN_ID

GO


