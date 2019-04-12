USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--EXEC [ETL].[uspSrc_AmbOpt_HARVerif]

ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_HARVerif] 
(@startdate SMALLDATETIME = NULL, 
 @enddate SMALLDATETIME = NULL
)
AS
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_HARVerif
--WHO : Mali Amarasinghe
--WHEN: 05/16/2018
--WHY : for Ambulatory Optimization project
--			Calculate HAR-Verification for all outpatient visits 
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:
--              CLARITY_App.dbo.Dim_Date
--				CLARITY.dbo.PAT_ENC					
--				CLARITY.dbo.HSP_ACCOUNT_3					
--				CLARITY.dbo.HSP_ACCOUNT
--              CLARITY.dbo.F_SCHED_APPT
--              CLARITY.dbo.ZC_CANCEL_REASON
--				CLARITY.dbo.CLARITY_DEP
--              CLARITY.dbo.ZC_DEP_RPT_GRP_6
--              CLARITY.dbo.ZC_DEP_RPT_GRP_7						
--				CLARITY.dbo.PATIENT
--              CLARITY.dbo.COVERAGE			
--				CLARITY.dbo.PATIENT_MYC
--				CLARITY.dbo.CLARITY_SER
--				CLARITY.dbo.IDENTITY_ID
--              CLARITY.dbo.IDENTITY_SER_ID
--				CLARITY.dbo.ZC_MYCHART_STATUS
--              CLARITY.dbo.ZC_SEX
--              CLARITY.dbo.VERIFICATION
--              CLARITY.dbo.COVERAGE_MEM_LIST
--              CLARITY.dbo.VERIF_STATUS_HX
--              CLARITY_App.Rptg.vwRef_MDM_Location_Master
--              CLARITY.dbo.CLARITY_EPP
--              CLARITY.dbo.CLARITY_EPM
--				CLARITY.dbo.CLARITY_EPP_2
--              CLARITY.dbo.CLARITY_EPM_2
--              CLARITY.dbo.CLARITY_EMP
--              CLARITY.dbo.ZC_GUAR_VERIF_STAT
--				CLARITY_App.dbo.Dim_Physcn
--              CLARITY_App.Rptg.Big6_Transplant_Datamart
--              CLARITY_App.Stage.AmbOpt_Excluded_Department
--              CLARITY_App.Rptg.vwRef_Crosswalk_HSEntity_Prov
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_HARVerif]
--					
/*
			pre-registration Rate =Pre_registration/ Total encounters

			E-Verified Rate = E_Verified/Ins_Verification

			Override Case Rate =Override_Case_Rate/RTE_Enabled
				
		*/
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         05/16/2018 -Mali - create stored procedure
--		   09/17/2018 -Mali	-	Remove 'New'  Verification status as a verified status 
--			11/16/2018 - Mali	-- Add No show and Left without seen patients 
--			12/13/2018 -Mali	-- replace PAT_ENC with V_SCHED_APPT 
--								-- PAN_ENC only have Appt cancel Date, this create a disparity when calculating Appointment cancel lead time. 
--								--remove filter for Outpatient per request from KF 
--								--Use CANCEL_INITIATOR value to identify patient-initiated cancellations
--         12/17/2018 - Tom     -- restore PAT_ENC as the source for encounter data
--                              -- join to F_SCHED_APPT for determining cancellation initiator and cancel lead time values
--         01/07/2019 - Tom		-- updated logic that assigns latest ins verification date to an encounter
--                              -- include late provider-initiated cancellations in the denominator
--         01/16/2019 - Tom     -- filter excluded departments
--         01/31/2019 - Tom     -- add code to set values for wrapper columns "peds" and "transplant"
--         03/26/2019 - Tom     -- exclude appointments created within 5 days of the appointment date
--                              -- remove parameter for days between HAR Verification date and appointment date
--                              -- add column APPT_MADE_DTTM, calculated column [HAR Verification DATEDIFF]
--                              -- add new standard columns
--         03/28/2019 - BDD     ---cast various columns as proper data type for portal tables, replaced spaces in output column name with _
--         03/28/2019 - BDD     ---removed w_ from new column names to match other portal processes. corrected mdm column names
--         03/28/2019 - BDD     ---replaced select * with column list
--         04/02/2019 - Tom     -- edited logic to join encounter to Rptg.vwRef_Crosswalk_HSEntity_Prov using PROV_ID
--         04/04/2019 - TMB     -- added columns CANCEL_INITIATOR, CANCEL_LEAD_HOURS, Cancel_Lead_Days,
--                                  appt_event_Canceled, appt_event_Canceled_Late, appt_event_Provider_Canceled
--         04/09/2019 - TMB     -- add columns STAFF_RESOURCE_C, STAFF_RESOURCE, PROVIDER_TYPE_C, PROV_TYPE, BUSINESS_UNIT
--************************************************************************************************************************

SET NOCOUNT ON;

--DECLARE @startdate SMALLDATETIME = NULL, @enddate SMALLDATETIME = NULL, @dt SMALLINT = NULL
----get default Balanced Scorecard date range
IF  @startdate IS NULL
AND @enddate IS NULL
    EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT
                                                 ,@enddate OUTPUT;

DECLARE @locstartdate SMALLDATETIME
       ,@locenddate   SMALLDATETIME
       ,@locdt        INT;
SET @locstartdate = @startdate;
SET @locenddate = @enddate;
SET @locdt = 5; -- 3/26/2019 Tom Default days between HAR Verification date and appointment date for scorecard
-------------------------------------------------------------------------------

SELECT DISTINCT
    --flags	

                    CAST('HAR_Verification' AS VARCHAR(50))                                                                  AS event_type

                   ,CAST(CASE
                             WHEN harvrf.VERIF_STATUS_C IN (   '1'  --verified
                                                            --  ,'2'  --New verification records --removed 9/17/2018 Mali
                                                              ,'6'  --e-verified
                                                              ,'8'  --E-verified- Additional coverage
                                                              ,'12' --verifed by phone
                                                              ,'13' --verified by website
                                                           )
                             AND  DATEDIFF(DAY, harvrf.LAST_STAT_CHNG_DTTM, appt.APPT_TIME) >= @locdt
                             THEN 1
                             ELSE 0
                         END AS INT)                                                                                         AS [HAR Verification]

                   ,CAST(CASE
                             WHEN vrxhx.VERIF_STATUS_HX_C IN ( '1', '12', '13', '6', '8' )				--value '2' removed 9/17/2018 Mali
                             AND  DATEDIFF(DAY, vrxhx.VERIF_DATE_HX_DTTM, appt.APPT_TIME) > @locdt
                             THEN 1
                             ELSE 0
                         END AS INT)                                                                                         AS [Ins Verification]

                   ,CAST(CASE
                             WHEN vrxhx.VERIF_STATUS_HX_C = '6' --e-verified
                             AND  DATEDIFF(DAY, vrxhx.VERIF_DATE_HX_DTTM, appt.APPT_TIME) > @locdt
                             THEN 1
                             ELSE 0
                         END AS INT)                                                                                         AS [E-Verified]
                   ,CAST(CASE
                             WHEN cvg.PLAN_ID IS NOT NULL
                             AND
                                  (
                                      COALESCE(epp2.USE_ELCT_VERIF_YN, 'N') = 'Y' -- RTE enabled at benefit plan level
                                OR    COALESCE(epm2.USE_ELCT_VERIF_YN, 'N') = 'Y'
                                  ) -- RTE enabled at payor level
                             THEN 1
                             ELSE 0
                         END AS INT)                                                                                         AS [RTE Enabled]
                   ,CAST(CASE
                             WHEN cvg.PLAN_ID IS NOT NULL
                             AND  (COALESCE(epp2.USE_ELCT_VERIF_YN, 'N') = 'Y' OR COALESCE(epm2.USE_ELCT_VERIF_YN, 'N') = 'Y')
                             AND  vrxhx.VERIF_STATUS_HX_C <> '6' --Not E-Verified
                             THEN 1
                             ELSE 0
                         END AS INT)                                                                                         AS [RTE Override case Rate]

                                                                                                                                                 --patient info
                   ,appt.PAT_ENC_CSN_ID
                   ,appt.PAT_ID                                                                                              AS PAT_id
                   ,PATIENT.PAT_NAME                                                                                         AS person_name
                   ,CAST(idx.IDENTITY_ID AS VARCHAR(50))                                                                     AS person_id        --MRN ---BDD 10/11/2018 cast for epic upgrade
                   ,mych.MYCHART_STATUS_C
                   ,mych.NAME                                                                                                AS MYCHART_STATUS_NAME
                   ,CAST(PATIENT.BIRTH_DATE AS DATETIME)                                                                     AS person_birth_date
                   ,CAST(sx.NAME AS VARCHAR(255))                                                                            AS person_gender
                   ,CAST(NULL AS INT)                                                                                        AS sk_Dim_Pt
                   ,CAST(CASE WHEN FLOOR((CAST(dmdt.day_date AS INTEGER)
								                 - CAST(CAST(PATIENT.BIRTH_DATE AS DATETIME) AS INTEGER))
								                / 365.25) < 18 THEN 1
				              ELSE 0
			        END AS SMALLINT)                                                                                         AS peds
                   ,CAST(CASE WHEN tx.pat_enc_csn_id IS NOT NULL THEN 1
				              ELSE 0
			             END AS SMALLINT)                                                                                    AS transplant

                                                                                                                                                 --appt info
                   ,appt.APPT_STATUS_C
                   ,appt.APPT_CONF_STAT_C
                   ,appt.CANCEL_REASON_C

                 ----BDD 08/27/2018 added per Mali
				   ,apptuser.SYSTEM_LOGIN																					AS Appt_Entry_User
				   ,harvrxuser.SYSTEM_LOGIN																					AS HAR_Verif_User
				   ,harvrf.VERIF_STATUS_C																					AS HAR_Verif_Status_C
				   ,vrxsts.NAME																								AS HAR_Verif_Status
                 ------

                                                                                                                                                 --dates/times
                   ,CAST(LEFT(DATENAME(MM, dmdt.day_date), 3) + ' ' + CAST(DAY(dmdt.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
                   ,CAST(CAST(dmdt.day_date AS DATE) AS SMALLDATETIME)                                                       AS report_date
                   ,CAST(appt.APPT_TIME AS DATETIME)                                                                         AS event_date
                   ,CAST(appt.APPT_CANC_DTTM AS DATETIME)																     AS APPT_CANCEL_DATE
                   ,CAST(harvrf.LAST_STAT_CHNG_DTTM AS DATETIME)                                                             AS HAR_Verified_Date
                   ,CAST(vrxhx.VERIF_DATE_HX_DTTM AS DATETIME)                                                               AS INS_Verified_Date
                   ,dmdt.fmonth_num
                   ,dmdt.fmonth_name
                   ,dmdt.Fyear_num
                   ,dmdt.FYear_name
                   ,CAST(NULL AS VARCHAR(150))                                                                               AS event_category



                                                                                                                                                 --Provider/scheduler info

                   ,appt.VISIT_PROV_ID                                                                                       AS provier_id
                   ,ser.PROV_NAME                                                                                            AS provider_Name
                   ,CAST(ser.RPT_GRP_FIVE AS VARCHAR(150))                                                                   AS prov_serviceline --service line
                   ,CAST(NULL AS INT)                                                                                        AS practice_group_id
                   ,CAST(NULL AS VARCHAR(150))                                                                               AS practice_group_name


                                                                                                                                                 --Billing info 
                   ,cvg.PAYOR_ID
                   ,epm.PAYOR_NAME
                   ,cvg.PLAN_ID
                   ,CAST(epp.BENEFIT_PLAN_NAME AS VARCHAR(75)) AS BENEFIT_PLAN_NAME
                   ,appt.ACCT_FIN_CLASS_C
                   ,appt.HSP_ACCOUNT_ID
                   ,appt.COVERAGE_ID


                                                                                                                                                 --fac-org info
                   ,mdm.epic_department_id
                   ,mdm.EPIC_DEPT_NAME AS epic_department_name
                   ,mdm.EPIC_EXT_NAME  AS epic_department_name_external
                   ,CAST(dep.RPT_GRP_SIX AS VARCHAR(55))                                                                     AS pod_id
                   ,CAST(pod.NAME AS VARCHAR(100))                                                                           AS pod_name         -- pod
                   ,CAST(dep.RPT_GRP_SEVEN AS VARCHAR(55))                                                                   AS hub_id           -- hub
                   ,CAST(hub.NAME AS VARCHAR(100))                                                                           AS hub_name
                   ,mdm.service_line_id                                                                                      AS service_line_id
                   ,mdm.service_line                                                                                         AS service_line
                   ,mdm.sub_service_line_id                                                                                  AS sub_service_line_id
                   ,mdm.sub_service_line                                                                                     AS sub_service_line
                   ,mdm.opnl_service_id                                                                                      AS opnl_service_id
                   ,mdm.opnl_service_name                                                                                    AS opnl_service_name
                   ,mdm.corp_service_line_id                                                                                 AS corp_service_line_id
                   ,mdm.corp_service_line                                                                                    AS corp_service_line_name
                   ,mdm.hs_area_id                                                                                           AS hs_area_id
                   ,mdm.hs_area_name                                                                                         AS hs_area_name

				   ,DATEDIFF(DAY, harvrf.LAST_STAT_CHNG_DTTM, appt.APPT_TIME)                                                AS HAR_Verification_DATEDIFF -- INTEGER
				   ,appt.APPT_MADE_DTTM
				   ,dp.sk_Dim_Physcn
				   ,NULL                                                                                                     AS som_group_id
				   ,NULL                                                                                                     AS som_group_name
				   ,mdm.LOC_ID                                                                                               AS rev_location_id
				   ,mdm.REV_LOC_NAME                                                                                         AS rev_location

				   ,uwd.Clrt_Financial_Division                                                                              AS financial_division_id
				   ,uwd.Clrt_Financial_Division_Name																		 AS financial_division_name
				   ,uwd.Clrt_Financial_SubDivision                                                                           AS financial_sub_division_id
				   ,uwd.Clrt_Financial_SubDivision_Name																		 AS financial_sub_division_name

--				   ,uwd.SOM_DEPT_ID                                                                                          AS som_department_id
				   ,CAST(uwd.SOM_Department_ID AS INT)  AS som_department_id    ---bdd 3/29/2019 temp until ref table built
				   ,CAST(uwd.SOM_Department AS VARCHAR(150))																 AS som_department_name
				   ,CAST(uwd.SOM_Division_ID AS INT)																		 AS som_division_id
				   ,CAST(uwd.SOM_Division_Name AS VARCHAR(150))                                                              AS som_division_name

				   ,CAST(appt.CANCEL_INITIATOR AS VARCHAR(55))                                                               AS CANCEL_INITIATOR
				   ,CAST(appt.CANCEL_LEAD_HOURS AS INTEGER)                                                                  AS CANCEL_LEAD_HOURS
				   ,appt.Cancel_Lead_Days
				   ,CASE WHEN appt.APPT_STATUS_C = 3 AND appt.CANCEL_INITIATOR = 'PATIENT' AND appt.CANCEL_LEAD_HOURS < 24.0 THEN 0
				         WHEN appt.APPT_STATUS_C = 3 THEN 1
						 ELSE 0
					END AS appt_event_Canceled
				   ,CASE WHEN appt.APPT_STATUS_C = 3 AND appt.CANCEL_INITIATOR = 'PATIENT' AND appt.CANCEL_LEAD_HOURS < 24.0 THEN 1
						 ELSE 0
					END AS appt_event_Canceled_Late
				   ,CASE WHEN appt.APPT_STATUS_C = 3 AND appt.CANCEL_INITIATOR = 'PROVIDER' THEN 1
						 ELSE 0
					END AS appt_event_Provider_Canceled
					
	               ,ser.STAFF_RESOURCE_C -- INTEGER
	               ,ser.STAFF_RESOURCE -- VARCHAR(20)
	               ,ser.PROVIDER_TYPE_C -- VARCHAR(66)
	               ,ser.PROV_TYPE -- VARCHAR(66)
				   ,mdm.BUSINESS_UNIT -- VARCHAR(20)

FROM                CLARITY_App.dbo.Dim_Date                           AS dmdt
    LEFT OUTER JOIN
                    (
                        SELECT         enc.PAT_ID,
                                       enc.PAT_ENC_DATE_REAL,
                                       enc.PAT_ENC_CSN_ID,
                                       enc.CONTACT_DATE,
                                       enc.ENC_TYPE_C,
                                       enc.ENC_TYPE_TITLE,
                                       enc.AGE,
                                       enc.PCP_PROV_ID,
                                       enc.FIN_CLASS_C,
                                       enc.VISIT_PROV_ID,
                                       enc.VISIT_PROV_TITLE,
                                       enc.DEPARTMENT_ID,
                                       enc.BP_SYSTOLIC,
                                       enc.BP_DIASTOLIC,
                                       enc.TEMPERATURE,
                                       enc.PULSE,
                                       enc.WEIGHT,
                                       enc.HEIGHT,
                                       enc.RESPIRATIONS,
                                       enc.LMP_DATE,
                                       enc.LMP_OTHER_C,
                                       enc.HEAD_CIRCUMFERENCE,
                                       enc.ENC_CLOSED_YN,
                                       enc.ENC_CLOSED_USER_ID,
                                       enc.ENC_CLOSE_DATE,
                                       enc.LOS_PRIME_PROC_ID,
                                       enc.LOS_PROC_CODE,
                                       enc.LOS_MODIFIER1_ID,
                                       enc.LOS_MODIFIER2_ID,
                                       enc.LOS_MODIFIER3_ID,
                                       enc.LOS_MODIFIER4_ID,
                                       enc.CHKIN_INDICATOR_C,
                                       enc.CHKIN_INDICATOR_DT,
                                       enc.APPT_STATUS_C,
                                       enc.APPT_BLOCK_C,
                                       enc.APPT_TIME,
                                       enc.APPT_LENGTH,
                                       enc.APPT_MADE_DATE,
                                       enc.APPT_PRC_ID,
                                       enc.CHECKIN_TIME,
                                       enc.CHECKOUT_TIME,
                                       enc.ARVL_LST_DL_TIME,
                                       enc.ARVL_LST_DL_USR_ID,
                                       enc.APPT_ENTRY_USER_ID,
                                       enc.APPT_CANC_USER_ID,
                                       enc.APPT_CANCEL_DATE,
                                       enc.CHECKIN_USER_ID,
                                       enc.CANCEL_REASON_C,
                                       enc.APPT_SERIAL_NO,
                                       enc.HOSP_ADMSN_TIME,
                                       enc.HOSP_DISCHRG_TIME,
                                       enc.HOSP_ADMSN_TYPE_C,
                                       enc.NONCVRED_SERVICE_YN,
                                       enc.REFERRAL_REQ_YN,
                                       enc.REFERRAL_ID,
                                       enc.ACCOUNT_ID,
                                       enc.COVERAGE_ID,
                                       enc.AR_EPISODE_ID,
                                       enc.CLAIM_ID,
                                       enc.PRIMARY_LOC_ID,
                                       enc.CHARGE_SLIP_NUMBER,
                                       enc.VISIT_EPM_ID,
                                       enc.VISIT_EPP_ID,
                                       enc.VISIT_FC,
                                       enc.COPAY_DUE,
                                       enc.COPAY_COLLECTED,
                                       enc.COPAY_SOURCE_C,
                                       enc.COPAY_TYPE_C,
                                       enc.COPAY_REF_NUM,
                                       enc.COPAY_PMT_EXPL_C,
                                       enc.UPDATE_DATE,
                                       enc.SERV_AREA_ID,
                                       enc.HSP_ACCOUNT_ID,
                                       enc.ADM_FOR_SURG_YN,
                                       enc.SURGICAL_SVC_C,
                                       enc.INPATIENT_DATA_ID,
                                       enc.IP_EPISODE_ID,
                                       enc.APPT_QNR_ANS_ID,
                                       enc.ATTND_PROV_ID,
                                       enc.ORDERING_PROV_TEXT,
                                       enc.ES_ORDER_STATUS_C,
                                       enc.EXTERNAL_VISIT_ID,
                                       enc.CONTACT_COMMENT,
                                       enc.OUTGOING_CALL_YN,
                                       enc.DATA_ENTRY_PERSON,
                                       enc.IS_WALK_IN_YN,
                                       enc.CM_CT_OWNER_ID,
                                       enc.REFERRAL_SOURCE_ID,
                                       enc.SIGN_IN_TIME,
                                       enc.SIGN_IN_USER_ID,
                                       enc.APPT_TARGET_DATE,
                                       enc.WC_TPL_VISIT_C,
                                       enc.ROUTE_SUM_PRNT_YN,
                                       enc.CONSENT_TYPE_C,
                                       enc.PHONE_REM_STAT_C,
                                       enc.APPT_CONF_STAT_C,
                                       enc.APPT_CONF_PERS,
                                       enc.APPT_CONF_INST,
                                       enc.CANCEL_REASON_CMT,
                                       enc.ORDERING_PROV_ID,
                                       enc.BMI,
                                       enc.BSA,
                                       enc.AVS_PRINT_TM,
                                       enc.AVS_FIRST_USER_ID,
                                       enc.ENC_MED_FRZ_RSN_C,
                                       enc.WC_TPL_VISIT_CMT,
                                       enc.HOSP_LICENSE_C,
                                       enc.ACCREDITATION_C,
                                       enc.CERTIFICATION_C,
                                       enc.ENTITY_C,
                                       enc.EFFECTIVE_DATE_DT,
                                       enc.DISCHARGE_DATE_DT,
                                       enc.EFFECTIVE_DEPT_ID,
                                       enc.TOBACCO_USE_VRFY_YN,
                                       enc.PHON_CALL_YN,
                                       enc.PHON_NUM_APPT,
                                       enc.ENC_CLOSE_TIME,
                                       enc.COPAY_PD_THRU,
                                       enc.INTERPRETER_NEED_YN,
                                       enc.VST_SPECIAL_NEEDS_C,
                                       enc.INTRP_ASSIGNMENT_C,
                                       enc.ASGND_INTERP_TYPE_C,
                                       enc.INTERPRETER_VEND_C,
                                       enc.INTERPRETER_NAME,
                                       enc.CHECK_IN_KIOSK_ID,
                                       enc.BENEFIT_PACKAGE_ID,
                                       enc.BENEFIT_COMP_ID,
                                       enc.BEN_ADJ_TABLE_ID,
                                       enc.BEN_ADJ_FORMULA_ID,
                                       enc.BEN_ENG_SP_AMT,
                                       enc.BEN_ADJ_COPAY_AMT,
                                       enc.BEN_ADJ_METHOD_C,
                                       enc.DOWNTIME_CSN,
                                       enc.ENTRY_TIME,
                                       enc.ENC_CREATE_USER_ID,
                                       enc.ENC_INSTANT,
                                       enc.ED_ARRIVAL_KIOSK_ID,
                                       enc.EFFECTIVE_DATE_DTTM,
                                       enc.CALCULATED_ENC_STAT_C
                                      ,har.HAR_VERIFICATION_ID
                                      ,acct.ACCT_FIN_CLASS_C
									  ,sched.APPT_CANC_DTTM
									  ,sched.APPT_MADE_DTTM

				                       ----BDD 04/01/2019 added calcd column to help eliminate function from final where clause
    				                  ,DATEDIFF(DAY, sched.APPT_MADE_DTTM, enc.APPT_TIME) AS Appt_Made_Days

									  ,CASE									--4/4/2019 -Tom B Create cancel initiator								
                                         WHEN sched.CANCEL_REASON_C IS NULL THEN NULL
                                         WHEN canc.CANCEL_REASON_C IS NULL THEN '*Unknown cancel reason [' + CONVERT(VARCHAR, sched.cancel_reason_c) + ']'
                                         WHEN (SELECT COUNT(PAT_INIT_CANC_C) FROM CLARITY.dbo.PAT_INIT_CANC 
                                               WHERE PAT_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PATIENT'
                                         WHEN (SELECT COUNT(PROV_INIT_CANC_C) FROM CLARITY.dbo.PROV_INIT_CANC 
                                               WHERE PROV_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PROVIDER'
                                         ELSE 'OTHER'
                                       END AS CANCEL_INITIATOR
									  ,CAST((DATEDIFF(MINUTE, sched.APPT_CANC_DTTM, sched.APPT_DTTM) / 60) AS NUMERIC(8,2)) AS CANCEL_LEAD_HOURS --4/4/2019 -Tom B Cancel Lead Hours
									  ,DATEDIFF(DAY, CAST(sched.APPT_CANC_DTTM AS DATE), CAST(sched.APPT_DTTM AS DATE)) AS Cancel_Lead_Days --4/4/2019 -Tom B Cancel Lead Days

                        FROM           CLARITY.dbo.PAT_ENC       AS enc					--12/17/2018 -Tom B Replaced V_SCHED_APPT with PAT_ENC
                            INNER JOIN CLARITY.dbo.HSP_ACCOUNT_3 AS har ON har.HSP_ACCOUNT_ID = enc.HSP_ACCOUNT_ID
                            INNER JOIN CLARITY.dbo.HSP_ACCOUNT   AS acct ON har.HSP_ACCOUNT_ID = acct.HSP_ACCOUNT_ID
							INNER JOIN CLARITY.dbo.F_SCHED_APPT  AS sched ON sched.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID				-- 12/17/2018 -Tom B Added join to use fact table for					
                            LEFT OUTER JOIN CLARITY.dbo.ZC_CANCEL_REASON canc ON sched.CANCEL_REASON_C = canc.CANCEL_REASON_C		--             cancel initiator and lead time

                        WHERE          1 = 1
                        -- AND            acct.ACCT_BASECLS_HA_C = '2' --outpatient				--12/13/2018 -Mali remove filter for Outpatient per request from KF ****
                        AND            enc.APPT_TIME >= @locstartdate
                        AND            enc.APPT_TIME < @locenddate
                    )                                                  AS appt ON CAST(appt.APPT_TIME AS DATE) = CAST(dmdt.day_date AS DATE)

    LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP                            AS dep ON dep.DEPARTMENT_ID = appt.DEPARTMENT_ID

    LEFT OUTER JOIN CLARITY.dbo.ZC_DEP_RPT_GRP_6                       AS pod ON pod.RPT_GRP_SIX = dep.RPT_GRP_SIX

    LEFT OUTER JOIN CLARITY.dbo.ZC_DEP_RPT_GRP_7                       AS hub ON hub.RPT_GRP_SEVEN = dep.RPT_GRP_SEVEN

    LEFT OUTER JOIN CLARITY.dbo.PATIENT                                AS PATIENT ON appt.PAT_ID = PATIENT.PAT_ID

    LEFT OUTER JOIN CLARITY.dbo.COVERAGE                               AS cvg ON  ((appt.CONTACT_DATE >= cvg.CVG_EFF_DT) AND (appt.CONTACT_DATE <= COALESCE(cvg.CVG_TERM_DT,GETDATE())))
                                                                              AND (appt.COVERAGE_ID = cvg.COVERAGE_ID)

    LEFT OUTER JOIN CLARITY.dbo.PATIENT_MYC                            AS PATIENT_MYC ON appt.PAT_ID = PATIENT_MYC.PAT_ID

    LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER                            AS ser ON appt.VISIT_PROV_ID = ser.PROV_ID

    LEFT OUTER JOIN CLARITY.dbo.IDENTITY_ID                            AS idx ON  idx.PAT_ID = appt.PAT_ID
                                                                              AND idx.IDENTITY_TYPE_ID = 14

    LEFT OUTER JOIN CLARITY.dbo.IDENTITY_SER_ID                        AS isi ON  isi.PROV_ID = ser.PROV_ID -- 03/26/2019 -Tom B Add to extract IDENTITY_ID for join to Dim_Physcn
                                                                              AND isi.IDENTITY_TYPE_ID = 6 -- IDNumber
                                                                              AND TRY_CONVERT(INT,isi.IDENTITY_ID) IS NOT NULL -- exclude IDs with alpha characters

    LEFT OUTER JOIN CLARITY.dbo.ZC_MYCHART_STATUS                      AS mych ON mych.MYCHART_STATUS_C = PATIENT_MYC.MYCHART_STATUS_C

	LEFT OUTER JOIN CLARITY.dbo.ZC_SEX                                 AS sx ON sx.RCPT_MEM_SEX_C = PATIENT.SEX_C

    LEFT OUTER JOIN CLARITY.dbo.VERIFICATION                           AS harvrf ON appt.HAR_VERIFICATION_ID = harvrf.RECORD_ID

    --LEFT OUTER JOIN CLARITY.dbo.COVERAGE_MEM_LIST                      AS memlst ON memlst.COVERAGE_ID = appt.COVERAGE_ID -- 1/7/2019 -Tom B remove join

    --------------------------------
    --last time insurance was verified before the appointment																	

    ---BDD 6/8/2018 Changed aliases inside the derived table so that they aren't the same as aliases in the outer query

    LEFT OUTER JOIN
                    (
                        SELECT              vrxhx2.RECORD_ID
						                   ,vrxhx2.VERIF_DATE_HX_DTTM
										   ,vrxhx2.VERIF_STATUS_HX_C
                                           ,appt2.PAT_ENC_CSN_ID
										   ,appt2.COVERAGE_ID
                                           ,ROW_NUMBER() OVER (PARTITION BY appt2.PAT_ENC_CSN_ID, appt2.COVERAGE_ID ORDER BY vrxhx2.VERIF_DATE_HX_DTTM DESC) AS Rw --1/7/2019 -Tom B added CSN to partitioning, sort by verification date
                        FROM                CLARITY.dbo.PAT_ENC           AS appt2
                            LEFT OUTER JOIN CLARITY.dbo.COVERAGE_MEM_LIST AS memlst2 ON memlst2.COVERAGE_ID = appt2.COVERAGE_ID

                            INNER JOIN
							          (
									     SELECT RECORD_ID
										       ,VERIF_DATE_HX_DTTM
											   ,VERIF_STATUS_HX_C
										 FROM CLARITY.dbo.VERIF_STATUS_HX
									  ) vrxhx2 ON  vrxhx2.RECORD_ID = memlst2.MEM_VERIFICATION_ID
                        WHERE               1 = 1
                        AND                 appt2.APPT_TIME >= @locstartdate
                        AND                 appt2.APPT_TIME < @locenddate
						AND                 vrxhx2.VERIF_DATE_HX_DTTM <= appt2.APPT_TIME
                    )                                                  AS vrxhx ON  vrxhx.PAT_ENC_CSN_ID = appt.PAT_ENC_CSN_ID --1/7/2019 -Tom B link latest verification to encounter
                                                                                --AND vrxhx.RECORD_ID = memlst.MEM_VERIFICATION_ID -- 1/7/2019 -Tom B removed respective join
                                                                                AND vrxhx.Rw = '1' --last updated within the date range
    ----------------------------	

    ---BDD 6/8/2018 changed below to use the distinct view
    LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_MDM_Location_Master         AS mdm ON mdm.EPIC_DEPARTMENT_ID = appt.DEPARTMENT_ID --03/26/2019 -Tom B Uncomment, use to get LOC_ID and REV_LOC_NAME

    --LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm ON mdm.epic_department_id = appt.DEPARTMENT_ID --03/26/2019 -Tom B Comment out
	
    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EPP                            AS epp ON epp.BENEFIT_PLAN_ID = cvg.PLAN_ID

    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EPM                            AS epm ON epm.PAYOR_ID = cvg.PAYOR_ID

    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EPP_2                          AS epp2 ON epp.BENEFIT_PLAN_ID = epp2.BENEFIT_PLAN_ID

    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EPM_2                          AS epm2 ON epm.PAYOR_ID = epm2.PAYOR_ID

-----BDD 08/27/2018 added per Mali
	LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP								AS apptuser	ON appt.APPT_ENTRY_USER_ID=apptuser.USER_ID
	
	LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP								AS harvrxuser	ON appt.APPT_ENTRY_USER_ID=harvrxuser.USER_ID

	LEFT OUTER JOIN CLARITY.dbo.ZC_GUAR_VERIF_STAT						AS vrxsts		ON harvrf.VERIF_STATUS_C=vrxsts.GUAR_VERIF_STAT_C
					   
	LEFT OUTER JOIN dbo.Dim_Physcn                                      AS dp ON isi.IDENTITY_ID = dp.IDNumber -- 4/2/2019 -Tom B Used to extract sk_Dim_Physcn value
                                                                              AND dp.current_flag = 1

------

-- Identify transplant encounter

  LEFT OUTER JOIN (SELECT DISTINCT
					  btd.pat_enc_csn_id
					 ,btd.Event_Transplanted AS 'transplant_surgery_dt'
					 ,btd.hosp_admsn_time AS 'Adm_Dtm'
					FROM
					  CLARITY_App.Rptg.Big6_Transplant_Datamart btd
					INNER JOIN CLARITY.dbo.PAT_ENC enc ON btd.pat_enc_csn_id = enc.PAT_ENC_CSN_ID
					WHERE
					  (btd.TX_Episode_Phase = 'transplanted'
					   AND btd.TX_Stat_Dt >= @locstartdate
					   AND btd.TX_Stat_Dt < @locenddate)
					  AND btd.TX_GroupedPhaseStatus = 'TX-ADMIT') tx ON tx.pat_enc_csn_id = appt.PAT_ENC_CSN_ID

                -- -------------------------------------
                -- Excluded departments--
                -- -------------------------------------
    LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department                    AS excl ON excl.DEPARTMENT_ID = appt.DEPARTMENT_ID

	LEFT OUTER JOIN -- 03/26/2019 -Tom B Add to extract standard columns from vwRef_Crosswalk_HSEntity_Prov
	                (
					    SELECT				PROV_ID --4/2/2019 -Tom B Utilize PROV_ID value added to view
						                   ,SOMSeq
										   ,Clrt_Financial_Division
										   ,Clrt_Financial_Division_Name
										   ,Clrt_Financial_SubDivision
										   ,Clrt_Financial_SubDivision_Name
										   ,SOM_DEPT_ID
										   ,wd_Dept_Code
										   ,wd_Department_Name
										   ,wd_Is_Primary_Job
										   ,SOM_Department_ID
										   ,SOM_Department
										   ,SOM_Division_ID
										   ,SOM_Division_Name
					    FROM                (
												SELECT
													hse.PROV_ID, --4/2/2019 -Tom B Utilize PROV_ID value added to view
													ROW_NUMBER() OVER (PARTITION BY hse.PROV_ID ORDER BY hse.cw_Legacy_src_system) AS [SOMSeq], --4/2/2019 -Tom B Use PROV_ID to partition
             										Clrt_Financial_Division = CASE WHEN ISNUMERIC(hse.Clrt_Financial_Division) = 0 THEN CAST(NULL AS INT) ELSE CAST(hse.Clrt_Financial_Division AS INT) END,
			    									Clrt_Financial_Division_Name = CASE WHEN hse.Clrt_Financial_Division_Name = 'na' THEN CAST(NULL AS VARCHAR(150)) ELSE CAST (hse.Clrt_Financial_Division AS VARCHAR(150)) END,
													Clrt_Financial_SubDivision = CASE WHEN ISNUMERIC(hse.Clrt_Financial_SubDivision) = 0 THEN CAST(NULL AS INT) ELSE CAST(hse.Clrt_Financial_SubDivision AS INT) END, 
													Clrt_Financial_SubDivision_Name = CASE WHEN hse.Clrt_Financial_SubDivision_Name = 'na' THEN CAST(NULL AS VARCHAR(150)) ELSE CAST(hse.Clrt_Financial_SubDivision_Name AS VARCHAR(150)) END,
													hse.SOM_DEPT_ID,
													hse.wd_Dept_Code,
													hse.wd_Department_Name,
													hse.wd_Is_Primary_Job,
													som.SOM_Department_ID,
													som.SOM_Department,
													som.SOM_Division_ID,
													som.SOM_Division_Name
												FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS hse
												   LEFT OUTER JOIN (SELECT DISTINCT SOM_Department_ID,
																					SOM_Department,
																					SOM_Division_ID,
																					SOM_Division_Name
																	   FROM Rptg.vwRef_SOM_Hierarchy
																   ) AS som
																  ON hse.wd_department_name = som.SOM_Division_Name
												WHERE ISNULL(hse.wd_Is_Primary_Job,1) = 1

											) wd
					) AS uwd ON uwd.PROV_ID = appt.VISIT_PROV_ID --4/2/2019 -Tom B Join to encounter provider id
					  	     AND uwd.SOMSeq = 1

WHERE               1 = 1

----BDD 4/1/2019 changed below to eliminate function in Where clause
---AND                 DATEDIFF(DAY, appt.APPT_MADE_DTTM, appt.APPT_TIME) >= @locdt -- 3/26/2019 Tom Exclude appointments created within 5 days of appointment date
AND                 appt.Appt_Made_Days >= @locdt
-----
AND                 dmdt.day_date >= @locstartdate
AND                 dmdt.day_date < @locenddate

AND                 excl.DEPARTMENT_ID IS NULL

AND                 (appt.APPT_STATUS_C <> 3 -- 4/4/2019 Tom Filter scheduled appointment records
                     OR
					 (appt.APPT_STATUS_C = 3
					  AND
					  ((appt.CANCEL_INITIATOR = 'PATIENT' AND appt.CANCEL_LEAD_HOURS < 24.0)
					   OR
					   (appt.CANCEL_INITIATOR = 'PROVIDER' AND appt.Cancel_Lead_Days <= 45)
					  )
					 )
					)

ORDER BY            event_date;
GO


