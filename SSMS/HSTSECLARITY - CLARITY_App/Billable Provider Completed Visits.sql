USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate DATETIME
       ,@enddate DATETIME

SET @startdate = '7/1/2018 00:00 AM'
SET @enddate = '6/30/2019 00:00 AM'

/****************************************************************************************************************************************
WHAT: Create procedure etl.uspRefresh_Scheduled_Appointment
      originally ETL.uspLoad_Scheduled_Appointment
WHO : Tom Burgan
WHEN: 11/29/2017

New version : Bryan Dunn
WHEN		: 05/21/2019

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

        05/21/2019 -BDD New version to do daily refresh of updated schedule rows instead of trunc and replace of 2 years worth. 
						Stripped out V_SCHED_APPT into component tables, copied select assemblies from the view code 
						Changed some large derived table joins into regular joins
						Eliminated outer Select		       

*****************************************************************************************************************************************/

SET NOCOUNT ON;
 

DECLARE @currdate DATETIME,
        @rpenddate DATETIME,
        --@loclastupdate DATETIME,
		@locstartdate DATETIME,
		@locenddate DATETIME

SET @currdate = CAST(CAST(GETDATE() AS DATE) AS DATETIME) 
-------------------------------------------------------------------------------
 ---go back a week for safety in case of null pass
IF @startdate IS NULL 
   SET @startdate = DATEADD(dd,-7,@currdate)

   SET @rpenddate=CASE WHEN DATEPART(mm, @currdate)<7 
	                   THEN CAST('07/01/'+CAST(DATEPART(yy, @currdate) AS CHAR(4)) AS SMALLDATETIME)
                       ELSE CAST('07/01/'+CAST(DATEPART(yy, @currdate)+1 AS CHAR(4)) AS SMALLDATETIME)
                  END; 


 --temp set for testing
--SET @loclastupdate = @startdate
SET @locstartdate = @startdate
SET @locenddate = @enddate

------------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#schedappt ') IS NOT NULL
DROP TABLE #schedappt

IF OBJECT_ID('tempdb..#clinicmetric ') IS NOT NULL
DROP TABLE #clinicmetric

/*  All scheduled appointments with patient, MyChart, encounter, department, insurance, and referral detail  */
          SELECT 

			--patient info
                    F_SCHED_APPT.PAT_ENC_CSN_ID , 
                    F_SCHED_APPT.PAT_ID ,
                    PATIENT.PAT_NAME ,
                    CAST(IDENTITY_ID.IDENTITY_ID AS VARCHAR(50)) AS IDENTITY_ID , --MRN
                    PATIENT_MYC.MYCHART_STATUS_C ,
                    ZC_MYCHART_STATUS.NAME AS MYCHART_STATUS_NAME ,
                    PAT_ENC_4.VIS_NEW_TO_SYS_YN ,
                    PAT_ENC_4.VIS_NEW_TO_DEP_YN ,
                    PAT_ENC_4.VIS_NEW_TO_PROV_YN ,

			--appt info
                    F_SCHED_APPT.APPT_SERIAL_NUM ,
                    F_SCHED_APPT.RESCHED_APPT_CSN_ID ,
                    F_SCHED_APPT.PRC_ID ,

                    COALESCE(prc.PRC_NAME,CASE
                                           WHEN F_SCHED_APPT.PRC_ID IS NULL THEN '*Unspecified visit type'
                                           ELSE
                                            CASE 
                                              WHEN prc.PRC_ID IS NULL THEN '*Unknown visit type'
                                              ELSE '*Unnamed visit type'
                                            END + ' [' + F_SCHED_APPT.PRC_ID + ']' END 
							) AS PRC_NAME,

                    F_SCHED_APPT.APPT_BLOCK_C ,

                    CASE
                     WHEN F_SCHED_APPT.APPT_BLOCK_C IS NULL THEN NULL
                       ELSE COALESCE(zcblock.NAME,
                                                  CASE
                                                   WHEN zcblock.appt_block_c IS NULL
                                                    THEN '*Unknown block'
                                                   ELSE '*Unnamed block'
                                                   END + ' [' + CONVERT(VARCHAR(254), F_SCHED_APPT.appt_block_c) + ']'
                                    )
                    END    AS APPT_BLOCK_NAME ,

                    F_SCHED_APPT.APPT_LENGTH ,
                    F_SCHED_APPT.APPT_STATUS_C ,

                    CASE 
                        WHEN F_SCHED_APPT.APPT_STATUS_C = 1 AND F_SCHED_APPT.SIGNIN_DTTM IS NOT NULL THEN 'Present'
                        WHEN zcappt.NAME IS NOT NULL THEN zcappt.NAME
                        ELSE 
                           CASE
                              WHEN zcappt.APPT_STATUS_C IS NULL THEN '*Unknown status'
                              ELSE '*Unnamed status'
                              END + ' [' + CONVERT(VARCHAR(254), F_SCHED_APPT.APPT_STATUS_C) + ']'
                    END AS APPT_STATUS_NAME,

                    F_SCHED_APPT.APPT_CONF_STAT_C ,
                    DATEDIFF(MINUTE, F_SCHED_APPT.APPT_CANC_DTTM, F_SCHED_APPT.APPT_DTTM) / 60 AS CANCEL_LEAD_HOURS ,

                    CASE
					  WHEN F_SCHED_APPT.CANCEL_REASON_C IS NULL THEN NULL
					  WHEN canc.CANCEL_REASON_C IS NULL THEN '*Unknown cancel reason [' + CONVERT(VARCHAR(55), F_SCHED_APPT.cancel_reason_c) + ']'
					  WHEN (SELECT COUNT(PAT_INIT_CANC_C) FROM CLARITY.dbo.PAT_INIT_CANC 
							WHERE PAT_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PATIENT'
					  WHEN (SELECT COUNT(PROV_INIT_CANC_C) FROM CLARITY.dbo.PROV_INIT_CANC 
							WHERE PROV_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PROVIDER'
					  ELSE 'OTHER'
				    END AS CANCEL_INITIATOR ,

                    CASE WHEN F_SCHED_APPT.APPT_STATUS_C IN (2) THEN 'Y' ELSE 'N' END AS COMPLETED_STATUS_YN ,

                    F_SCHED_APPT.SAME_DAY_YN ,
                    F_SCHED_APPT.SAME_DAY_CANC_YN ,
                    F_SCHED_APPT.CANCEL_REASON_C ,
                    CASE
                      WHEN F_SCHED_APPT.CANCEL_REASON_C IS NULL 
					       THEN NULL
							ELSE COALESCE(canc.NAME,
	   	  								  CASE
							                WHEN canc.cancel_reason_c IS NULL
								              THEN '*Unknown cancel reason'
							                  ELSE '*Unnamed cancel reason'
						                   END + ' [' + CONVERT(VARCHAR(254), F_SCHED_APPT.cancel_reason_c) + ']'
					                      )  
					END AS CANCEL_REASON_NAME ,

                    F_SCHED_APPT.WALK_IN_YN ,
                    F_SCHED_APPT.OVERBOOKED_YN ,
                    F_SCHED_APPT.OVERRIDE_YN ,
                    F_SCHED_APPT.UNAVAILABLE_TIME_YN ,
                    F_SCHED_APPT.CHANGE_CNT ,
                    F_SCHED_APPT.JOINT_APPT_YN ,
                    F_SCHED_APPT.PHONE_REM_STAT_C ,

                    CASE WHEN F_SCHED_APPT.PHONE_REM_STAT_C IS NULL 
					       THEN NULL
						   ELSE
							   CASE
								   WHEN zcrem.PHONE_REM_STAT_C IS NULL THEN '*Unknown phone reminder status'
								   WHEN zcrem.NAME IS NULL THEN '*Unnamed phone reminder status [' + CONVERT(VARCHAR(254), zcrem.PHONE_REM_STAT_C) + ']'
				   					/* 09/28/18 bug fix start */
								   ELSE zcrem.NAME
								   /* 09/28/18 bug fix end */
                               END 
                    END AS PHONE_REM_STAT_NAME ,

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

			 DATEDIFF(
					MINUTE
					, F_SCHED_APPT.CHECKIN_DTTM
					, (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
					   FROM (VALUES(CASE WHEN F_SCHED_APPT.arvl_list_remove_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.arvl_list_remove_dttm ELSE NULL END),
									(CASE WHEN F_SCHED_APPT.roomed_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.roomed_dttm ELSE NULL END), 
									(CASE WHEN F_SCHED_APPT.first_room_assign_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.first_room_assign_dttm ELSE NULL END)
									) AS Roomed_Cols(candidate_dttm))
				  ) AS TIME_TO_ROOM_MINUTES,
				 DATEDIFF(
					MINUTE
					, (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
					   FROM (VALUES(CASE WHEN F_SCHED_APPT.arvl_list_remove_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.arvl_list_remove_dttm ELSE NULL END),
								   (CASE WHEN F_SCHED_APPT.roomed_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.roomed_dttm ELSE NULL END), 
								   (CASE WHEN F_SCHED_APPT.first_room_assign_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.first_room_assign_dttm ELSE NULL END)
							) AS Roomed_Cols(candidate_dttm))
					, (CASE WHEN F_SCHED_APPT.visit_end_dttm >= 
								  (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
								   FROM (VALUES(CASE WHEN F_SCHED_APPT.arvl_list_remove_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.arvl_list_remove_dttm ELSE NULL END),
											   (CASE WHEN F_SCHED_APPT.roomed_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.roomed_dttm ELSE NULL END), 
											   (CASE WHEN F_SCHED_APPT.first_room_assign_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.first_room_assign_dttm ELSE NULL END)
										 ) AS Roomed_Cols(candidate_dttm))
								  AND (F_SCHED_APPT.checkout_dttm IS NULL OR F_SCHED_APPT.visit_end_dttm <= F_SCHED_APPT.checkout_dttm)
								  THEN F_SCHED_APPT.visit_end_dttm
							WHEN F_SCHED_APPT.checkout_dttm >= 
									(SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
									 FROM (VALUES(CASE WHEN F_SCHED_APPT.arvl_list_remove_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.arvl_list_remove_dttm ELSE NULL END),
												 (CASE WHEN F_SCHED_APPT.roomed_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.roomed_dttm ELSE NULL END), 
												 (CASE WHEN F_SCHED_APPT.first_room_assign_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.first_room_assign_dttm ELSE NULL END)
										   ) AS Roomed_Cols(candidate_dttm))
									 AND (F_SCHED_APPT.visit_end_dttm IS NULL OR F_SCHED_APPT.checkout_dttm < F_SCHED_APPT.visit_end_dttm)
									THEN F_SCHED_APPT.checkout_dttm
							ELSE NULL
					  END)
				  ) AS TIME_IN_ROOM_MINUTES,
				 DATEDIFF(
					  MINUTE
					, F_SCHED_APPT.CHECKIN_DTTM
					, CASE 
						WHEN F_SCHED_APPT.VISIT_END_DTTM >= F_SCHED_APPT.CHECKIN_DTTM
							 AND (F_SCHED_APPT.CHECKOUT_DTTM IS NULL 
								OR F_SCHED_APPT.CHECKOUT_DTTM < F_SCHED_APPT.CHECKIN_DTTM
								OR F_SCHED_APPT.VISIT_END_DTTM <= F_SCHED_APPT.CHECKOUT_DTTM)
							 THEN F_SCHED_APPT.VISIT_END_DTTM
						WHEN F_SCHED_APPT.CHECKOUT_DTTM >= F_SCHED_APPT.CHECKIN_DTTM
							 THEN F_SCHED_APPT.CHECKOUT_DTTM
						ELSE NULL
					  END) AS CYCLE_TIME_MINUTES,

			--Provider/scheduler info
                    F_SCHED_APPT.REFERRING_PROV_ID ,

                    CASE
						WHEN F_SCHED_APPT.REFERRING_PROV_ID IS NULL THEN NULL
						ELSE
							CASE
								WHEN refprov.prov_name IS NOT NULL
									THEN refprov.prov_name
								WHEN refprov.prov_id IS NULL
									THEN '*Unknown provider'
								ELSE '*Unnamed provider'
							END + ' [' + F_SCHED_APPT.referring_prov_id + ']'
						END AS REFERRING_PROV_NAME_WID,

                    F_SCHED_APPT.PROV_ID ,
                    CASE
						WHEN F_SCHED_APPT.PROV_ID IS NULL
							THEN '*Unspecified provider'
						ELSE
							CASE
								WHEN CLARITY_SER.prov_name IS NOT NULL
									THEN CLARITY_SER.prov_name
								WHEN CLARITY_SER.prov_id IS NULL
									THEN '*Unknown provider'
								ELSE '*Unnamed provider'
							END + ' [' + F_SCHED_APPT.prov_id + ']'
				     END AS PROV_NAME_WID,

                    CLARITY_SER.RPT_GRP_FIVE , --service line
                    F_SCHED_APPT.APPT_ENTRY_USER_ID ,

                    CASE
						WHEN F_SCHED_APPT.APPT_ENTRY_USER_ID IS NULL
							THEN '*Unspecified user'
						ELSE
							CASE
								WHEN entryemp.USER_ID IS NULL 
									THEN '*Unknown user'
								WHEN entryemp.NAME IS NULL
									THEN '*Unnamed user'
								ELSE entryemp.NAME
							END + ' [' + F_SCHED_APPT.appt_entry_user_id + ']'
                    END AS APPT_ENTRY_USER_NAME_WID ,

                    CLARITY_SER.PROV_NAME ,
					CLARITY_SER.PROV_TYPE ,
					CLARITY_SER.STAFF_RESOURCE ,

			--Billing info 
                    V_COVERAGE_PAYOR_PLAN.PAYOR_ID ,
                    V_COVERAGE_PAYOR_PLAN.PAYOR_NAME ,
                    V_COVERAGE_PAYOR_PLAN.BENEFIT_PLAN_ID ,
                    CAST(V_COVERAGE_PAYOR_PLAN.BENEFIT_PLAN_NAME AS VARCHAR(75)) AS BENEFIT_PLAN_NAME ,
                    V_COVERAGE_PAYOR_PLAN.FIN_CLASS_NAME ,
                    PAT_ENC_3.DO_NOT_BILL_INS_YN ,
                    PAT_ENC_3.SELF_PAY_VISIT_YN ,

                    F_SCHED_APPT.HSP_ACCOUNT_ID ,
                    F_SCHED_APPT.ACCOUNT_ID ,
                    F_SCHED_APPT.COPAY_DUE ,
                    F_SCHED_APPT.COPAY_COLLECTED ,
                    F_SCHED_APPT.COPAY_USER_ID ,

                    CASE WHEN F_SCHED_APPT.COPAY_USER_ID IS NULL 
					   THEN NULL
					   ELSE
						 CASE
							WHEN copayemp.USER_ID IS NULL 
								THEN '*Unknown user'
							WHEN copayemp.NAME IS NULL
								THEN '*Unnamed user'
							ELSE copayemp.NAME
						END + ' [' + F_SCHED_APPT.copay_user_id + ']'
                    END AS COPAY_USER_NAME_WID ,
	
			 --fac-org info
                    F_SCHED_APPT.DEPARTMENT_ID ,
                    CLARITY_DEP.DEPARTMENT_NAME ,
                    CLARITY_DEP.DEPT_ABBREVIATION ,
                    CLARITY_DEP.RPT_GRP_THIRTY , -- service line
                    CLARITY_DEP.RPT_GRP_SIX , -- pod
                    CLARITY_DEP.RPT_GRP_SEVEN , -- hub

                    CLARITY_DEP.SPECIALTY_DEP_C AS DEPT_SPECIALTY_C ,
                    CASE
					  WHEN CLARITY_DEP.department_id IS NULL THEN '*Unknown department'
					  WHEN CLARITY_DEP.specialty IS NULL THEN '*No specialty'
					  ELSE CLARITY_DEP.specialty
				    END AS DEPT_SPECIALTY_NAME ,

                    CLARITY_DEP.CENTER_C ,
                    COALESCE(zccenter.name,
									   CASE
										   WHEN CLARITY_DEP.department_id IS NULL THEN '*Unknown department'
										   WHEN CLARITY_DEP.center_c IS NULL THEN '*No center'
										   ELSE '*Unknown center [' + CLARITY_DEP.center_c + ']'
									   END
						    ) AS CENTER_NAME ,

                    LOC.LOC_ID ,
                    COALESCE(loc.loc_name,
                                          CASE
                                            WHEN CLARITY_DEP.department_id IS NULL THEN '*Unknown department'
                                            WHEN CLARITY_DEP.rev_loc_id IS NULL THEN '*No location'
                                            ELSE '*Unknown location [' + CAST(CLARITY_DEP.rev_loc_id AS VARCHAR(18)) + ']'
                                          END
							) AS LOC_NAME ,

                    CLARITY_DEP.SERV_AREA_ID ,
	
			 --appt status flag
					CASE
					  WHEN ( F_SCHED_APPT.APPT_STATUS_C = 3 -- Canceled
                               AND (CASE
					                  WHEN F_SCHED_APPT.CANCEL_REASON_C IS NULL THEN NULL
					                  WHEN canc.CANCEL_REASON_C IS NULL THEN '*Unknown cancel reason [' + CONVERT(VARCHAR(254), F_SCHED_APPT.cancel_reason_c) + ']'
					                  WHEN (SELECT COUNT(PAT_INIT_CANC_C) FROM CLARITY.dbo.PAT_INIT_CANC 
							                  WHERE PAT_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PATIENT'
					                  WHEN (SELECT COUNT(PROV_INIT_CANC_C) FROM CLARITY.dbo.PROV_INIT_CANC 
							                  WHERE PROV_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PROVIDER'
					                  ELSE 'OTHER'
				                    END
								   ) = 'PATIENT'
                               AND DATEDIFF(MINUTE, F_SCHED_APPT.APPT_CANC_DTTM, F_SCHED_APPT.APPT_DTTM) / 60 < 24) THEN 'Canceled Late'
					  ELSE CASE 
                               WHEN F_SCHED_APPT.APPT_STATUS_C = 1 AND F_SCHED_APPT.SIGNIN_DTTM IS NOT NULL THEN 'Present'
                               WHEN zcappt.NAME IS NOT NULL THEN zcappt.NAME
                               ELSE 
                                   CASE
                                     WHEN zcappt.APPT_STATUS_C IS NULL THEN '*Unknown status'
                                     ELSE '*Unnamed status'
                                   END + ' [' + CONVERT(VARCHAR(254), F_SCHED_APPT.APPT_STATUS_C) + ']'
                           END

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

					zcconf.name AS APPT_CONF_STAT_NAME ,

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

					CASE
						WHEN F_SCHED_APPT.APPT_CANC_USER_ID IS NULL THEN NULL
						ELSE
							CASE
								WHEN cancemp.USER_ID IS NULL 
									THEN '*Unknown user'
								WHEN cancemp.NAME IS NULL
									THEN '*Unnamed user'
								ELSE cancemp.NAME
							END + ' [' + F_SCHED_APPT.appt_canc_user_id + ']'
					END AS APPT_CANC_USER_NAME_WID ,

					F_SCHED_APPT.APPT_CONF_USER_ID ,

					CASE
						  WHEN F_SCHED_APPT.APPT_CONF_USER_ID IS NOT NULL 
							THEN 
								CASE
									WHEN confemp.USER_ID IS NULL THEN '*Unknown user'
									ELSE COALESCE(confemp.NAME, '*Unnamed user')
								END + ' [' + F_SCHED_APPT.APPT_CONF_USER_ID + ']'
						  ELSE PAT_ENC.APPT_CONF_PERS
					END AS APPT_CONF_USER_NAME ,

					REFERRAL_HIST.CHANGE_DATE ,
					F_SCHED_APPT.APPT_MADE_DTTM ,
                    'RefreshScheduledAppointment' AS ETL_guid ,
					CAST(GETDATE() AS DATETIME) AS UPDATE_DATE,

					PROV_ATTR_INFO_OT.BILL_PROV_YN

		  INTO #schedappt

          FROM      CLARITY.dbo.F_SCHED_APPT AS F_SCHED_APPT

		    --        INNER JOIN (SELECT DISTINCT CAST(PK_DELIM_STRING AS NUMERIC(18,0)) AS PAT_ENC_CSN_ID    ---BDD 05/21/2019
      --                              FROM CLARITY.dbo.CR_STAT_ALTER 
      --                           WHERE table_name = 'F_SCHED_APPT' 
      --                             AND UPDATE_DT >= @locLastupdate
      --                         ) AS crsa 
						--ON F_SCHED_APPT.PAT_ENC_CSN_ID = crsa.PAT_ENC_CSN_ID


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

--------------------
--- BDD 5/20/2019 changed joins to eliminate large derived tables
--					LEFT OUTER JOIN (SELECT PROV_ID, SPECIALTY_C                  ---41,792 rows in this derived set
--					                 FROM CLARITY.dbo.CLARITY_SER_SPEC
--									 WHERE LINE = 1) AS CLARITY_SER_SPEC
--					  ON CLARITY_SER_SPEC.PROV_ID = CLARITY_SER.PROV_ID
                    LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER_SPEC AS CLARITY_SER_SPEC
					  ON CLARITY_SER_SPEC.PROV_ID = CLARITY_SER.PROV_ID
					    AND CLARITY_SER_SPEC.LINE = 1
--					LEFT OUTER JOIN (SELECT REFERRAL_ID, CHANGE_DATE              ----4,478,194 rows in this derived set
--					                 FROM CLARITY.dbo.REFERRAL_HIST
--									 WHERE CHANGE_TYPE_C = 1) AS REFERRAL_HIST
--					  ON REFERRAL_HIST.REFERRAL_ID = REFERRAL.REFERRAL_ID
                    LEFT OUTER JOIN CLARITY.dbo.REFERRAL_HIST AS REFERRAL_HIST
					  ON REFERRAL_HIST.REFERRAL_ID = REFERRAL.REFERRAL_ID   
					    AND REFERRAL_HIST.CHANGE_TYPE_C  = 1
-------------------

					LEFT OUTER JOIN CLARITY.dbo.PROV_ATTR_INFO_OT AS PROV_ATTR_INFO_OT
					  ON PROV_ATTR_INFO_OT.PROV_ATTR_ID = CLARITY_SER.PROV_ATTR_ID
					    AND PROV_ATTR_INFO_OT.BILL_PROV_YN = 'Y'
						AND F_SCHED_APPT.CONTACT_DATE BETWEEN PROV_ATTR_INFO_OT.CONTACT_DATE AND COALESCE(PROV_ATTR_INFO_OT.CONTACT_TO_DATE,@rpenddate)

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

 ----joins added to eliminate the V_SCHED_APPT view
                    LEFT OUTER JOIN CLARITY.dbo.CLARITY_PRC AS prc 
					  ON F_SCHED_APPT.PRC_ID = prc.PRC_ID
                    LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_BLOCK AS zcblock
					  ON F_SCHED_APPT.APPT_BLOCK_C = zcblock.APPT_BLOCK_C
					LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_STATUS AS zcappt
					  ON F_SCHED_APPT.APPT_STATUS_C = zcappt.APPT_STATUS_C
					LEFT OUTER JOIN CLARITY.dbo.CLARITY_LOC AS loc 
					  ON CLARITY_DEP.rev_loc_id = loc.loc_id
                    LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS refprov 
					  ON F_SCHED_APPT.REFERRING_PROV_ID = refprov.PROV_ID
					LEFT OUTER JOIN CLARITY.dbo.ZC_CANCEL_REASON AS canc
					  ON F_SCHED_APPT.CANCEL_REASON_C = canc.CANCEL_REASON_C
					LEFT OUTER JOIN CLARITY.dbo.ZC_PHONE_REM_STAT AS zcrem
					  ON F_SCHED_APPT.PHONE_REM_STAT_C = zcrem.PHONE_REM_STAT_C
					LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP AS entryemp
					  ON F_SCHED_APPT.APPT_ENTRY_USER_ID = entryemp.USER_ID
                    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP AS copayemp
					  ON F_SCHED_APPT.COPAY_USER_ID = copayemp.USER_ID
                    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP AS cancemp
					  ON F_SCHED_APPT.APPT_CANC_USER_ID = cancemp.USER_ID
                    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP AS confemp
					  ON F_SCHED_APPT.APPT_CONF_USER_ID = confemp.USER_ID

					LEFT OUTER JOIN CLARITY.dbo.ZC_CENTER AS zccenter
					  ON CLARITY_DEP.CENTER_C = zccenter .CENTER_C
					LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_CONF_STAT AS zcconf
					  ON F_SCHED_APPT.APPT_CONF_STAT_C = zcconf.APPT_CONF_STAT_C


----filters are handled above in the join to CR_STAT_ALTER
       WHERE     1 = 1
                 AND F_SCHED_APPT.APPT_DTTM >= @locstartdate
                 AND F_SCHED_APPT.APPT_DTTM <  @locenddate
				 AND F_SCHED_APPT.APPT_STATUS_C = 2	
				    
	      --ORDER BY F_SCHED_APPT.PAT_ENC_CSN_ID

--SELECT *
--SELECT PROV_NAME
--     , PROV_ID
--	 , PROV_TYPE
--	 , STAFF_RESOURCE
--	 , BILL_PROV_YN
--SELECT MAX(PROV_NAME) AS PROV_NAME
--     , PROV_ID
--	 , DEPARTMENT_NAME
--	 , MAX(PROV_TYPE) AS PROV_TYPE
--	 , MAX(STAFF_RESOURCE) AS STAFF_RESOURCE
--	 , SUM(CASE WHEN BILL_PROV_YN = 'Y' THEN 1 ELSE 0 END) AS [Billable]
--	 , SUM(CASE WHEN BILL_PROV_YN IS NULL THEN 1 ELSE 0 END) AS [Not Billable]
--FROM #schedappt
--GROUP BY PROV_ID, DEPARTMENT_NAME			   
--	      --ORDER BY PAT_ENC_CSN_ID				   
--	      ORDER BY PROV_ID

        SELECT DISTINCT
            CAST(NULL AS VARCHAR(150)) AS event_category,
            main.epic_pod AS pod_id,
            main.epic_hub AS hub_id,
            main.epic_department_id,
            main.person_id,
            main.practice_group_id,
            main.practice_group_name,
            main.provider_id,
            main.provider_name,
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
                    --CAST(NULL AS SMALLINT)
                    CAST(0 AS SMALLINT)
            END AS appt_event_No_Show,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled Late' ))
                ) THEN
                    1
                ELSE
                    --CAST(NULL AS SMALLINT)
                    CAST(0 AS SMALLINT)
            END AS appt_event_Canceled_Late,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled' ))
                ) THEN
                    1
                ELSE
                    --CAST(NULL AS SMALLINT)
                    CAST(0 AS SMALLINT)
            END AS appt_event_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Scheduled' ))
                ) THEN
                    1
                ELSE
                    --CAST(NULL AS SMALLINT)
                    CAST(0 AS SMALLINT)
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
                    --CAST(NULL AS SMALLINT)
                    CAST(0 AS SMALLINT)
            END AS appt_event_Provider_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C IN ( 2 ))
                ) THEN
                    1
                ELSE
                    --CAST(NULL AS SMALLINT)
                    CAST(0 AS SMALLINT)
            END AS appt_event_Completed,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C IN ( 6 ))
                ) THEN
                    1
                ELSE
                    --CAST(NULL AS SMALLINT)
                    CAST(0 AS SMALLINT)
            END AS appt_event_Arrived,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.VIS_NEW_TO_SPEC_YN = 'Y')
                ) THEN
                    1
                ELSE
                    --CAST(NULL AS SMALLINT)
                    CAST(0 AS SMALLINT)
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
			main.APPT_SERIAL_NUM,
			main.RESCHED_APPT_CSN_ID,
			main.BILL_PROV_YN

		INTO #clinicmetric

        FROM
        ( --main
            SELECT appts.RPT_GRP_THIRTY AS epic_service_line,
                   appts.RPT_GRP_SIX AS epic_pod,
                   appts.RPT_GRP_SEVEN AS epic_hub,
                   appts.DEPARTMENT_ID AS epic_department_id,
                   CAST(appts.IDENTITY_ID AS INT) AS person_id,
                   CAST(NULL AS INT) AS practice_group_id,
                   CAST(NULL AS VARCHAR(150)) AS practice_group_name,
                   appts.PROV_ID AS provider_id,
                   appts.PROV_NAME AS provider_name,
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
				   appts.PHONE_REM_STAT_NAME,
				   appts.CHANGE_DATE,
				   appts.APPT_MADE_DTTM,

				   appts.APPT_SERIAL_NUM,
				   appts.RESCHED_APPT_CSN_ID,

				   appts.BILL_PROV_YN

            FROM #schedappt AS appts

                -- -------------------------------------
                -- Excluded departments--
                -- -------------------------------------
                LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
				    ON excl.DEPARTMENT_ID = appts.DEPARTMENT_ID
            WHERE appts.APPT_DT >= @locstartdate
			AND excl.DEPARTMENT_ID IS NULL

        ) AS main

		SELECT *
		FROM #clinicmetric
		--WHERE appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45
		WHERE appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)
		ORDER BY BILL_PROV_YN DESC

GO


