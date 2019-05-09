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
				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
                DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
                DS_HSDM_App.Stage.AmbOpt_Excluded_Department
                DS_HSDM_App.Rptg.vwRef_Crosswalk_HSEntity_Prov
                DS_HSDM_App.Rptg.vwRef_SOM_Hierarchy
                DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt
                DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt
                
      OUTPUTS:  ETL.uspSrc_AmbOpt_CGCAHPS_RecommendProvOffice
--------------------------------------------------------------------------------------------------------------------------
MODS: 	
      04/04/2018 - TMB - create stored procedure
	  04/27/2018 - TMB - change vwDim_PG_Question criteria for responses to 805 (CG_26CL); add sk_Dim_Physcn to extract
	  05/17/2018 - TMB - use sk_Phys_Atn value in vwFact_Pt_Acct to identify attending provider; add logic to handle 0's in
	                     sk_Phys_Atn values
	  07/16/2018 - TMB - exclude departments
      04/08/2019 - TMB - add BUSINESS_UNIT, Prov_Typ, Staff_Resource, and the new standard portal columns
      05/08/2019 - TMB - add logic for updated/new views Rptg.vwRef_Crosswalk_HSEntity_Prov and Rptg.vwRef_SOM_Hierarchy
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
           ,pm.pod_id
		   ,pm.pod_name
           ,pm.hub_id
		   ,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.sk_Dim_Physcn
		   ,pm.BUSINESS_UNIT
		   ,pm.Prov_Typ
		   ,pm.Staff_Resource
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,pm.som_department_name
		   ,pm.som_division_id
		   ,pm.som_division_name
		   ,pm.som_division_5 -- VARCHAR(150)
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
				,loc_master.POD_ID AS pod_id
		        ,loc_master.PFA_POD AS pod_name
				,loc_master.HUB_ID AS hub_id
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
				,prov.Prov_Typ
				,prov.Staff_Resource
				--,NULL AS som_group_id
				--,NULL AS som_group_name
				,loc_master.LOC_ID AS rev_location_id
				,loc_master.REV_LOC_NAME AS rev_location
				--,uwd.Clrt_Financial_Division AS financial_division_id
				--,uwd.Clrt_Financial_Division_Name AS financial_division_name
				--,uwd.Clrt_Financial_SubDivision AS financial_sub_division_id
				--,uwd.Clrt_Financial_SubDivision_Name financial_sub_division_name
				--,CAST(uwd.SOM_Department_ID AS INT) AS som_department_id
				--,CAST(uwd.SOM_Department AS VARCHAR(150)) AS som_department_name
				--,CAST(uwd.SOM_Division_ID AS INT) AS som_division_id
				--,CAST(uwd.SOM_Division_Name AS VARCHAR(150)) AS som_division_name
         		,CASE WHEN ISNUMERIC(cwlk.Clrt_Financial_Division) = 0 THEN CAST(NULL AS INT) ELSE CAST(cwlk.Clrt_Financial_Division AS INT) END AS financial_division_id
				,CAST(cwlk.Clrt_Financial_Division_Name AS VARCHAR(150)) AS financial_division_name
				,CASE WHEN ISNUMERIC(cwlk.Clrt_Financial_SubDivision) = 0 THEN CAST(NULL AS INT) ELSE CAST(cwlk.Clrt_Financial_SubDivision AS INT) END AS financial_sub_division_id
				,CAST(cwlk.Clrt_Financial_SubDivision_Name AS VARCHAR(150)) AS financial_sub_division_name
				,som.SOM_Group_ID AS som_group_id
				,CAST(som.SOM_group AS VARCHAR(150)) AS som_group_name
				,som.SOM_department_id AS som_department_id
				,CAST(som.SOM_department AS VARCHAR(150)) AS som_department_name
				,som.SOM_division_id AS som_division_id
				,CAST(som.SOM_division_name AS VARCHAR(150)) AS som_division_name
				,CAST(som.SOM_division_5 AS VARCHAR(150)) AS som_division_5
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
				   POD_ID,
                   PFA_POD,
				   HUB_ID,
                   HUB,
			       BUSINESS_UNIT,
				   LOC_ID,
				   REV_LOC_NAME
            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
        ) AS loc_master
                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
  --      LEFT OUTER JOIN
	 --   (
		--	SELECT DISTINCT
		--		   sk_Dim_Physcn,
		--		   dim_Physcn_PROV_ID,
		--		   SOMSeq,
		--		   Clrt_Financial_Division,
		--		   Clrt_Financial_Division_Name,
		--		   Clrt_Financial_SubDivision,
		--		   Clrt_Financial_SubDivision_Name,
		--		   wd_Dept_Code,
		--		   wd_Department_Name,
		--		   wd.Som_Department_ID,
		--		   wd.SOM_Department,
		--		   wd.SOM_Division_ID,
		--		   wd.SOM_Division_Name
		--	FROM
		--	(
		--		SELECT hse.sk_Dim_Physcn,
		--			   hse.dim_Physcn_PROV_ID,
		--			   ROW_NUMBER() OVER (PARTITION BY hse.sk_Dim_Physcn ORDER BY hse.cw_Legacy_src_system) AS [SOMSeq],
  --           		   Clrt_Financial_Division = CASE WHEN ISNUMERIC(hse.Clrt_Financial_Division) = 0 THEN CAST(NULL AS INT) ELSE CAST(hse.Clrt_Financial_Division AS INT) END,
		--	    	   Clrt_Financial_Division_Name = CASE WHEN hse.Clrt_Financial_Division_Name = 'na' THEN CAST(NULL AS VARCHAR(150)) ELSE CAST (hse.Clrt_Financial_Division_Name AS VARCHAR(150)) END,
		--			   Clrt_Financial_SubDivision = CASE WHEN ISNUMERIC(hse.Clrt_Financial_SubDivision) = 0 THEN CAST(NULL AS INT) ELSE CAST(hse.Clrt_Financial_SubDivision AS INT) END, 
		--			   Clrt_Financial_SubDivision_Name = CASE WHEN hse.Clrt_Financial_SubDivision_Name = 'na' THEN CAST(NULL AS VARCHAR(150)) ELSE CAST(hse.Clrt_Financial_SubDivision_Name AS VARCHAR(150)) END,
		--			   hse.SOM_DEPT_ID,
		--		       hse.wd_Dept_Code,
		--			   hse.wd_Department_Name,
		--			   som.SOM_Department_ID,
		--			   som.SOM_Department,
		--			   som.SOM_Division_ID,
		--			   som.SOM_Division_Name
		--		FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS hse
		--	    LEFT OUTER JOIN
		--		(
		--		    SELECT DISTINCT
		--			       SOM_Department_ID,
		--				   SOM_Department,
		--				   SOM_Division_ID,
		--				   SOM_Division_Name
		--			FROM Rptg.vwRef_SOM_Hierarchy
		--		) AS som
		--				ON hse.wd_department_name = som.SOM_Division_Name
		--	    WHERE ISNULL(wd_Is_Primary_Job,1) = 1
		--	) wd
		--) AS uwd ON uwd.sk_Dim_Physcn = dp.sk_Dim_Physcn
		--		    AND uwd.SOMSeq = 1

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
        LEFT OUTER JOIN Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
		        ON cwlk.sk_Dim_Physcn = dp.sk_Dim_Physcn
                   AND cwlk.wd_Is_Primary_Job = 1
                   AND cwlk.wd_Is_Position_Active = 1
        LEFT OUTER JOIN Rptg.vwRef_SOM_Hierarchy AS som
			    ON cwlk.wd_Dept_Code=som.SOM_division_5
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


