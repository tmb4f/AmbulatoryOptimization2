USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL, 
        @enddate SMALLDATETIME = NULL--,
        --@dt SMALLINT = NULL

--SET @startdate = '2/3/2019 00:00 AM'
--SET @enddate = '2/9/2019 11:59 PM'
SET @startdate = '7/1/2018 00:00 AM'
SET @enddate = '6/30/2019 11:59 PM'

--EXEC [ETL].[uspSrc_AmbOpt_HARVerif]

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_HARVerif] 
--(@startdate SMALLDATETIME = NULL, 
-- @enddate SMALLDATETIME = NULL, 
-- @dt SMALLINT = NULL
--)
--AS
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
--              CLARITY.dbo.V_SCHED_APPT appt
--				CLARITY.dbo.CLARITY_DEP dep							
--				CLARITY.dbo.PATIENT PATIENT										   
--				CLARITY.dbo.PAT_ENC_4 enc4									
--				CLARITY.dbo.PAT_ENC_3 enc3				
--				CLARITY.dbo.V_COVERAGE_PAYOR_PLAN Vcvg				
--				CLARITY.dbo.PATIENT_MYC 
--				CLARITY.dbo.CLARITY_SER
--				CLARITY.dbo.IDENTITY_ID
--				CLARITY.dbo.ZC_MYCHART_STATUS mych						
--				CLARITY.dbo.HSP_ACCOUNT_3 har					
--				CLARITY.dbo.HSP_ACCOUNT hsp	
--				CLARITY.dbo.VERIFICATION harvrf
--				CLARITY.dbo.VERIFICATION insvrf
--				CLARITY.dbo.VERIF_ENC_CVGS	 cvgvrf
--				CLARITY_App.Rptg.vwRef_MDM_Location_Master mdm
--				CLARITY.dbo.CLARITY_EPP_2 epp
--				CLARITY.dbo.PAT_ENC
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
--         03/14/2019 - Tom     -- exclude appointments created within 5 days of the appointment date
--                              -- remove parameter for days between HAR Verification date and appointment date
--                              -- add column APPT_MADE_DTTM, calculated column [HAR Verification DATEDIFF]
--                              -- add new standard columns
--************************************************************************************************************************

SET NOCOUNT ON;

--DECLARE @startdate SMALLDATETIME = NULL, @enddate SMALLDATETIME = NULL, @dt SMALLINT = NULL
----get default Balanced Scorecard date range
IF  @startdate IS NULL
AND @enddate IS NULL
    EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT
                                                 ,@enddate OUTPUT;

--IF @dt IS NULL
--    SET @dt = 5; -- set default  to 5 (number of days expected to complete pre-reg before appointment date)


DECLARE @locstartdate SMALLDATETIME
       ,@locenddate   SMALLDATETIME
       ,@locdt        INT;
SET @locstartdate = @startdate;
SET @locenddate = @enddate;
SET @locdt = 5; -- 3/14/2019 Tom Default days between HAR Verification date and appointment date for scorecard
-------------------------------------------------------------------------------

if OBJECT_ID('tempdb..#HAR') is not NULL
DROP TABLE #HAR

if OBJECT_ID('tempdb..#HAR2') is not NULL
DROP TABLE #HAR2

if OBJECT_ID('tempdb..#HAR3') is not NULL
DROP TABLE #HAR3

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
				   --,wd.dim_Physcn_PROV_ID
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

                   ,appt.VISIT_PROV_ID                                                                                       AS provider_id
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
                   --,mdm.epic_department_name
                   --,mdm.epic_department_name_external
                   ,mdm.EPIC_DEPT_NAME                                                                                       AS epic_department_name
                   ,mdm.EPIC_EXT_NAME                                                                                        AS epic_department_name_external
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
				   ,DATEDIFF(DAY, harvrf.LAST_STAT_CHNG_DTTM, appt.APPT_TIME)                                                AS [HAR Verification DATEDIFF] -- INTEGER
				   ,appt.APPT_MADE_DTTM -- DATETIME
				   --,NULL                                                                                                     AS w_som_group_id
				   --,NULL                                                                                                     AS w_som_group_name
				   ,mdm.LOC_ID                                                                                               AS w_rev_location_id
				   ,mdm.REV_LOC_NAME                                                                                         AS w_rev_name
				   --,wd.Clrt_Financial_Division                                                                               AS w_financial_division_id
				   --,wd.Clrt_Financial_Division_Name                                                                          AS w_financial_division_name
				   --,wd.Clrt_Financial_SubDivision                                                                            AS w_financial_sub_division_id
				   --,wd.Clrt_Financial_SubDivision_Name                                                                       AS w_financial_sub_division_name
				   --,wd.SOM_DEPT_ID                                                                                           AS w_som_department_id
				   --,NULL                                                                                                     AS w_som_department_name
				   --,wd.wd_Dept_Code                                                                                          AS w_som_division_id
				   --,wd.wd_Department_Name                                                                                    AS w_som_division_name
				   --,wd.wd_Is_Primary_Job
				   ,CAST(isi.IDENTITY_ID AS INTEGER) AS IDENTITY_ID

INTO                #HAR

FROM                CLARITY_App.dbo.Dim_Date                           AS dmdt
    LEFT OUTER JOIN
                    (
                        SELECT         enc.*
                                      ,har.HAR_VERIFICATION_ID
                                      ,acct.ACCT_FIN_CLASS_C
									  ,sched.APPT_CANC_DTTM
									  ,sched.APPT_MADE_DTTM
                        FROM           CLARITY.dbo.PAT_ENC       AS enc					--12/17/2018 -Tom B Replaced V_SCHED_APPT with PAT_ENC
                            INNER JOIN CLARITY.dbo.HSP_ACCOUNT_3 AS har ON har.HSP_ACCOUNT_ID = enc.HSP_ACCOUNT_ID
                            INNER JOIN CLARITY.dbo.HSP_ACCOUNT   AS acct ON har.HSP_ACCOUNT_ID = acct.HSP_ACCOUNT_ID
							INNER JOIN CLARITY.dbo.F_SCHED_APPT  AS sched ON sched.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID				-- 12/17/2018 -Tom B Added join to use fact table for					
                            LEFT OUTER JOIN CLARITY.dbo.ZC_CANCEL_REASON canc ON sched.CANCEL_REASON_C = canc.CANCEL_REASON_C		--             cancel initiator and lead time

                        WHERE          1 = 1
                        AND
                                       (
                                           enc.APPT_STATUS_C NOT IN ('3')			----12/13/2018  Add to replicate Tom B. Query     replaced >IN ( '6', '2', '1','4','5' ) --arrived/Completed/Scheduled/No show/Left without seen  
                                     OR
                                      (
                                          enc.APPT_STATUS_C = '3' --if cancelled, patient-initiated
										AND CASE									--12/17/2018 -Tom B Cancel initiator filter								
                                              WHEN sched.CANCEL_REASON_C IS NULL THEN NULL
                                              WHEN canc.CANCEL_REASON_C IS NULL THEN '*Unknown cancel reason [' + CONVERT(VARCHAR, sched.cancel_reason_c) + ']'
                                              WHEN (SELECT COUNT(PAT_INIT_CANC_C) FROM CLARITY.dbo.PAT_INIT_CANC 
                                                    WHERE PAT_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PATIENT'
                                              WHEN (SELECT COUNT(PROV_INIT_CANC_C) FROM CLARITY.dbo.PROV_INIT_CANC 
                                                    WHERE PROV_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PROVIDER'
                                              ELSE 'OTHER'
                                            END = 'PATIENT'
										AND CAST((DATEDIFF(MINUTE, sched.APPT_CANC_DTTM, sched.APPT_DTTM) / 60) AS NUMERIC(8,2)) < 24.0 --12/17/2018 -Tom B Cancel lead time filter

                                      )
                                     OR
                                      (
                                          enc.APPT_STATUS_C = '3' --if cancelled, provider-initiated -- 01/07/2019 -Tom B Add to replicate logic used to compute denominator for No Show and Bump Rate metrics
										AND CASE						
                                              WHEN sched.CANCEL_REASON_C IS NULL THEN NULL
                                              WHEN canc.CANCEL_REASON_C IS NULL THEN '*Unknown cancel reason [' + CONVERT(VARCHAR, sched.cancel_reason_c) + ']'
                                              WHEN (SELECT COUNT(PAT_INIT_CANC_C) FROM CLARITY.dbo.PAT_INIT_CANC 
                                                    WHERE PAT_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PATIENT'
                                              WHEN (SELECT COUNT(PROV_INIT_CANC_C) FROM CLARITY.dbo.PROV_INIT_CANC 
                                                    WHERE PROV_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 THEN 'PROVIDER'
                                              ELSE 'OTHER'
                                            END = 'PROVIDER'
										AND DATEDIFF(DAY, CAST(sched.APPT_CANC_DTTM AS DATE), CAST(sched.APPT_DTTM AS DATE)) <= 45

                                      )
                                       )
                       -- AND            acct.ACCT_BASECLS_HA_C = '2' --outpatient				--12/13/2018 -Mali remove filter for Outpatient per request from KF ****
                        AND            enc.APPT_TIME >= @locstartdate
                        AND            enc.APPT_TIME < @locenddate
						--AND            enc.VISIT_PROV_ID IN ('91274','93744')
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

    LEFT OUTER JOIN CLARITY.dbo.IDENTITY_SER_ID                        AS isi ON  isi.PROV_ID = ser.PROV_ID -- 03/15/2019 -Tom B Add to extract IDENTITY_ID for join to Dim_Physcn
                                                                              AND isi.IDENTITY_TYPE_ID = 6
                                                                              AND TRY_CONVERT(INT,isi.IDENTITY_ID) IS NOT NULL

    LEFT OUTER JOIN CLARITY.dbo.ZC_MYCHART_STATUS                      AS mych ON mych.MYCHART_STATUS_C = PATIENT_MYC.MYCHART_STATUS_C

	LEFT OUTER JOIN CLARITY.dbo.ZC_SEX                                 AS sx ON sx.RCPT_MEM_SEX_C = PATIENT.SEX_C

    LEFT OUTER JOIN CLARITY.dbo.VERIFICATION                           AS harvrf ON appt.HAR_VERIFICATION_ID = harvrf.RECORD_ID

    --LEFT OUTER JOIN CLARITY.dbo.COVERAGE_MEM_LIST                      AS memlst ON memlst.COVERAGE_ID = appt.COVERAGE_ID -- 01/07/2019 -Tom B remove join

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
                                           ,ROW_NUMBER() OVER (PARTITION BY appt2.PAT_ENC_CSN_ID, appt2.COVERAGE_ID ORDER BY vrxhx2.VERIF_DATE_HX_DTTM DESC) AS Rw --01/07/2019 -Tom B added CSN to partitioning, sort by verification date
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
                    )                                                  AS vrxhx ON  vrxhx.PAT_ENC_CSN_ID = appt.PAT_ENC_CSN_ID -- 01/07/2019 -Tom B link latest verification to encounter
                                                                                --AND vrxhx.RECORD_ID = memlst.MEM_VERIFICATION_ID -- 01/07/2019 -Tom B removed respective join
                                                                                AND vrxhx.Rw = '1' --last updated within the date range
    ----------------------------	

    ---BDD 6/8/2018 changed below to use the distinct view
    LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_MDM_Location_Master mdm				ON mdm.EPIC_DEPARTMENT_ID = appt.DEPARTMENT_ID

    --LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm ON mdm.epic_department_id = appt.DEPARTMENT_ID
	
    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EPP                            AS epp ON epp.BENEFIT_PLAN_ID = cvg.PLAN_ID

    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EPM                            AS epm ON epm.PAYOR_ID = cvg.PAYOR_ID

    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EPP_2                          AS epp2 ON epp.BENEFIT_PLAN_ID = epp2.BENEFIT_PLAN_ID

    LEFT OUTER JOIN CLARITY.dbo.CLARITY_EPM_2                          AS epm2 ON epm.PAYOR_ID = epm2.PAYOR_ID

-----BDD 08/27/2018 added per Mali
	LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP								AS apptuser	ON appt.APPT_ENTRY_USER_ID=apptuser.USER_ID
	
	LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP								AS harvrxuser	ON appt.APPT_ENTRY_USER_ID=harvrxuser.USER_ID

	LEFT OUTER JOIN CLARITY.dbo.ZC_GUAR_VERIF_STAT						AS vrxsts		ON harvrf.VERIF_STATUS_C=vrxsts.GUAR_VERIF_STAT_C
					   
	--LEFT OUTER JOIN dbo.Dim_Physcn                                      AS dp ON isi.IDENTITY_ID = dp.IDNumber -- 03/15/2019 -Tom B Add to extract key for join to vwRef_Crosswalk_HSEntity_Prov
 --                                                                             AND dp.current_flag = 1

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

	--LEFT OUTER JOIN (SELECT DISTINCT
	--                    dim_Physcn_PROV_ID
	--				   ,Clrt_Financial_Division
	--				   ,Clrt_Financial_Division_Name
	--				   ,Clrt_Financial_SubDivision
	--				   ,Clrt_Financial_SubDivision_Name
	--				   ,SOM_DEPT_ID
	--				   ,wd_Dept_Code
	--				   ,wd_Department_Name
	--				   ,wd_Is_Primary_Job
	--				 FROM
	--				   Rptg.vwRef_Crosswalk_HSEntity_Prov
	--				 WHERE
	--				   ISNULL(wd_Is_Primary_Job,1) = 1
	--				   AND Som_DEPT_ID IS NOT NULL) AS wd ON wd.dim_Physcn_PROV_ID = CAST(isi.IDENTITY_ID AS INTEGER)

WHERE               1 = 1

AND                 DATEDIFF(DAY, appt.APPT_MADE_DTTM, appt.APPT_TIME) >= @locdt -- 3/14/2019 Tom Exclude appointments created within 5 days of appointment date
AND                 dmdt.day_date >= @locstartdate
AND                 dmdt.day_date < @locenddate
AND                 excl.DEPARTMENT_ID IS NULL
AND                 mdm.LOC_ID <> '10376'
--AND                 appt.VISIT_PROV_ID = '61341'

--ORDER BY            event_date;

SELECT *
FROM #HAR
--WHERE provider_id IN ('61341','91274','93744')
ORDER BY PAT_ENC_CSN_ID
--ORDER BY provider_id, PAT_ENC_CSN_ID

SELECT har.PAT_ENC_CSN_ID
      ,har.w_rev_name
	  ,wd.dim_Physcn_PROV_ID
	  --,wd.Clrt_Financial_Division
	  --,wd.Clrt_Financial_Division_Name
	  --,wd.Clrt_Financial_SubDivision
	  --,wd.Clrt_Financial_SubDivision_Name
	  ,wd.SOM_DEPT_ID
	  ,wd.wd_Dept_Code
	  ,wd.wd_Department_Name
	  ,wd.wd_Is_Primary_Job
--SELECT *
     --, ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY w_som_division_id) AS [EncSeq]
     , ROW_NUMBER() OVER (PARTITION BY har.PAT_ENC_CSN_ID ORDER BY har.w_rev_name) AS [EncSeq]
INTO #HAR2
FROM #HAR har
	LEFT OUTER JOIN (SELECT DISTINCT
	                    dim_Physcn_PROV_ID
					   --,Clrt_Financial_Division
					   --,Clrt_Financial_Division_Name
					   --,Clrt_Financial_SubDivision
					   --,Clrt_Financial_SubDivision_Name
					   ,SOM_DEPT_ID
					   ,wd_Dept_Code
					   ,wd_Department_Name
					   ,wd_Is_Primary_Job
					 FROM
					   Rptg.vwRef_Crosswalk_HSEntity_Prov
					 WHERE
					   ISNULL(wd_Is_Primary_Job,1) = 1
					   AND Som_DEPT_ID IS NOT NULL
					   --Som_DEPT_ID IS NOT NULL
					) AS wd ON wd.dim_Physcn_PROV_ID = har.IDENTITY_ID

SELECT *
FROM #HAR2
--ORDER BY PAT_ENC_CSN_ID
--        --, w_som_division_id
--        , w_rev_name
--		, EncSeq
ORDER BY dim_Physcn_PROV_ID
       , PAT_ENC_CSN_ID
       --, w_som_division_id
       , w_rev_name
	   , EncSeq

SELECT DISTINCT PAT_ENC_CSN_ID
INTO #HAR3
FROM #HAR2
WHERE [EncSeq] > 1

SELECT *
FROM #HAR3
ORDER BY PAT_ENC_CSN_ID
/*
SELECT har.*
FROM #HAR har
INNER JOIN #HAR3 har3
ON har.PAT_ENC_CSN_ID = har3.PAT_ENC_CSN_ID
ORDER BY            har.dim_Physcn_PROV_ID, PAT_ENC_CSN_ID;
*/
GO


