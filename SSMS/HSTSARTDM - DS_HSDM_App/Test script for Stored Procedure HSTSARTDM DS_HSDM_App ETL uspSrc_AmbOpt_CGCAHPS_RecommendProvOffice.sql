USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME
       ,@enddate SMALLDATETIME

--SET @startdate = NULL
--SET @enddate = NULL

SET @startdate = '2/1/2019 00:00'
SET @enddate = '2/28/2019 23:59'

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_CGCAHPS_RecommendProvOffice]
--    (
--     @startdate SMALLDATETIME=NULL
--    ,@enddate SMALLDATETIME=NULL
--    )
--AS 

/**********************************************************************************************************************
WHAT: Ambulatory Optimization Reporting:  CGCAHPS Recommend Provider Office
WHO : Tom Burgan
WHEN: 4/4/2018
WHY : Press Ganey CGCAHPS results for survey question:
      "Would you recommend this provider's office to your family and friends?"
-----------------------------------------------------------------------------------------------------------------------
INFO:                  
      INPUTS:   DS_HSDW_Prod.Rptg.vwDim_Date
	            DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				DS_HSDW_Prod.Rptg.vwDim_PG_Question
				DS_HSDW_Prod.Rptg.vwFact_Pt_Acct
				DS_HSDW_Prod.Rptg.vwDim_Patient
				DS_HSDW_Prod.Rptg.vwDim_Physcn
				DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
				DS_HSDW_Prod.dbo.Dim_Clrt_DEPt
				DS_HSDW_Prod.rptg.vwRef_MDM_location_master
                
      OUTPUTS:  ETL.uspSrc_AmbOpt_CGCAHPS_RecommendProvOffice
--------------------------------------------------------------------------------------------------------------------------
MODS: 	
      04/04/2018 - TMB - create stored procedure
	  04/27/2018 - TMB - change vwDim_PG_Question criteria for responses to 805 (CG_26CL); add sk_Dim_Physcn to extract
	  05/17/2018 - TMB - use sk_Phys_Atn value in vwFact_Pt_Acct to identify attending provider; add logic to handle 0's in
	                     sk_Phys_Atn values
	  07/16/2018 - TMB - exclude departments
      04/08/2019 - TMB - add BUSINESS_UNIT, Prov_Typ, Staff_Resource, and the new standard portal columns
**************************************************************************************************************************************************************/
   
    SET NOCOUNT ON; 

---------------------------------------------------
 ----get default Balanced Scorecard date range
 IF @startdate IS NULL AND @enddate IS NULL
    EXEC etl.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT
----------------------------------------------------

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate

if OBJECT_ID('tempdb..#RptgTemp') is not NULL
DROP TABLE #RptgTemp 

    SELECT DISTINCT
            CAST('Outpatient-CGCAHPS' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.SEX AS person_gender
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,pm.service_line_id
           ,pm.service_line
           ,pm.sub_service_line_id
           ,pm.sub_service_line
           ,pm.opnl_service_id
           ,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   ,pm.corp_service_line_id
		   ,pm.corp_service_line
		   ,pm.provider_id
		   ,pm.provider_name
		   ,pm.practice_group_id
		   ,pm.practice_group_name
		   ,pm.sk_Dim_Pt
           ,pm.sk_Fact_Pt_Acct
           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
           ,CAST(NULL AS VARCHAR(66)) AS pod_id
		   ,pm.pod_name
           ,CAST(NULL AS VARCHAR(66)) AS hub_id
		   ,pm.hub_name
           ,CAST(pm.EPIC_DEPARTMENT_ID AS NUMERIC(18, 0)) AS epic_department_id
           ,epic_department_name = pm.EPIC_DEPT_NAME
           ,epic_department_name_external = pm.EPIC_EXT_NAME
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.sk_Dim_Physcn
	INTO #RptgTemp
    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
                   -- MDM
				,mdm.service_line_id
				,mdm.service_line
				,mdm.sub_service_line_id
				,mdm.sub_service_line
				,mdm.opnl_service_id
				,mdm.opnl_service_name
				,mdm.corp_service_line_id
				,mdm.corp_service_line
				,mdm.hs_area_id
				,mdm.hs_area_name
				,mdm.practice_group_id
				,mdm.practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,mdm.epic_department_name_external
		        ,loc_master.PFA_POD AS pod_name
		        ,loc_master.HUB AS hub_name
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				,pat.BirthDate AS BIRTH_DATE
				,pat.SEX
				,resp.Load_Dtm
				--,resp.sk_Dim_Physcn
				,dp.sk_Dim_Physcn
				,loc_master.BUSINESS_UNIT
		FROM    DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
		LEFT OUTER JOIN (SELECT sk_Dim_Physcn
		                 FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
						 WHERE current_flag = 1) AS dp
				ON fpa.sk_Phys_Atn=dp.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE dp.[sk_Dim_Physcn] WHEN '-1' THEN '-999' WHEN '0' THEN '-999' ELSE dp.sk_Dim_Physcn END =prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '784' -- Age question for Outpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		--LEFT OUTER JOIN DS_HSDW_Prod.rptg.vwRef_MDM_location_master loc_master
		--		ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
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
        ) AS loc_master
                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('805') -- Would you recommend this provider's office to your family and friends?
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				AND excl.DEPARTMENT_ID IS NULL
	) AS pm
ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    --ORDER BY rec.day_date;

	SELECT *
	FROM #RptgTemp

    --ORDER BY event_date;
    --ORDER BY provider_name
	   --    , event_date;
    ORDER BY sk_Dim_Physcn;

GO

