USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Department_CallServiceLevel]
    (
     @startdate SMALLDATETIME = NULL
    ,@enddate SMALLDATETIME = NULL
    )
AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Department_CallServiceLevel
--WHO : Tom Burgan
--WHEN: 3/28/19
--WHY : Report ACC call system call service level rates.
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:
--              DS_HSDW_App.ETL.usp_Get_Dash_Dates_BalancedScorecard
--              DS_HSDW_Prod.Rptg.vwDim_Date
--              DS_HSDW_App.CallCenter.Department_ACC_Workgroup_Mapping
--              DS_HSDW_Prod.CallCenter.ACC_PhoneData_QueueSummary
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Department_CallServiceLevel]
-- 
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--       03/28/2019 - TMB - create stored procedure
--       04/26/2019 - TMB - populate values for hs_area_id and hs_area_name
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate
-------------------------------------------------------------------------------
---declare and set some variables to improve the query performance
DECLARE @bod SMALLDATETIME,                       ----to prevent reuse of getdate() to get beginning of day in where clauses
        @currdate SMALLDATETIME                   ----to prevent reuse of getdate() in where clauses
SET @currdate = CAST(GETDATE() AS SMALLDATETIME)
SET @bod = CAST(CAST(@currdate AS DATE) AS SMALLDATETIME)

----------------------------------------------------------

SELECT date_dim.day_date
      ,date_dim.fmonth_num
      ,date_dim.Fyear_num
      ,date_dim.FYear_name
INTO #datetable
FROM DS_HSDW_Prod.Rptg.vwDim_Date AS date_dim
WHERE date_dim.day_date >= @locstartdate
AND date_dim.day_date < @locenddate
ORDER BY date_dim.day_date                 ---BDD 5/14/2018 added order by to reduce data reording in subsequent cluster index step

  -- Create index for temp table #datetable
  CREATE UNIQUE CLUSTERED INDEX IX_datetable ON #datetable ([day_date])

SELECT DISTINCT
       Workgroup
	 , EpicDepartment
INTO #depmapping
FROM DS_HSDW_App.CallCenter.Department_ACC_Workgroup_Mapping
ORDER BY Workgroup

  -- Create index for temp table #depmapping
  CREATE UNIQUE CLUSTERED INDEX IX_depmapping ON #depmapping ([Workgroup],[EpicDepartment])

------------------------------------------------------------------------------------------

SELECT [cName]
      ,CAST([cReportGroup] AS VARCHAR(20)) AS ReportGroup
	  ,dep.EpicDepartment AS Department_Id
      ,CAST([I3TimeStampGMT] AS SMALLDATETIME) AS day_date
      ,[nEnteredAcd]
      ,[nAbandonedAcd]
      ,[nGrabbedAcd]
      ,[nLocalDisconnectAcd]
      ,[nAlertedAcd]
      ,[nAnsweredAcd]
      ,[nAcdSvcLvl]
      ,[nAnsweredAcdSvcLvl]
      ,[nAnsweredAcdSvcLvl1]
      ,[nAnsweredAcdSvcLvl2]
      ,[nAnsweredAcdSvcLvl3]
      ,[nAnsweredAcdSvcLvl4]
      ,[nAnsweredAcdSvcLvl5]
      ,[nAnsweredAcdSvcLvl6]
      ,[nAbandonAcdSvcLvl]
      ,[nAbandonAcdSvcLvl1]
      ,[nAbandonAcdSvcLvl2]
      ,[nAbandonAcdSvcLvl3]
      ,[nAbandonAcdSvcLvl4]
      ,[nAbandonAcdSvcLvl5]
      ,[nAbandonAcdSvcLvl6]
      ,[tAnsweredAcd]
      ,[tAbandonedAcd]
      ,[tTalkAcd]
      ,[tTalkCompleteAcd]
      ,[nHoldAcd]
      ,[tHoldAcd]
      ,[nTransferedAcd]
      ,[nNotAnsweredAcd]
      ,[tAlertedAcd]
      ,[nDisconnectAcd]
      ,[tAgentTalk]
      ,[nServiceLevel]
  INTO #acc
  FROM DS_HSDW_Prod.[CallCenter].[ACC_PhoneData_QueueSummary] accqs
  INNER JOIN #depmapping dep
  ON dep.Workgroup = accqs.cName

SELECT
       acc.day_date
      ,acc.Department_Id AS epic_department_id
	  ,dep.Clrt_DEPt_Nme AS epic_department_name
	  ,dep.Clrt_DEPt_Ext_Nme AS epic_department_name_external
	  ,acc.ReportGroup
	  ,acc.cName
      ,acc.nEnteredAcd
	  ,acc.nAbandonedAcd
	  ,acc.nAnsweredAcd
      ,acc.nAnsweredAcdSvcLvl1 + acc.nAnsweredAcdSvcLvl2 + acc.nAnsweredAcdSvcLvl3 AS nAnsweredAcdSvcLvl_20
  INTO #accsum
  FROM #acc acc
  LEFT OUTER JOIN (SELECT DISTINCT
                          DEPARTMENT_ID
						, Clrt_DEPt_Nme
						, Clrt_DEPt_Ext_Nme
                   FROM DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt) dep ON dep.DEPARTMENT_ID = acc.Department_Id

  -- Create index for temp table #accsum

  CREATE NONCLUSTERED INDEX IX_accsum ON #accsum ([day_date], [epic_department_id], [epic_department_name], [epic_department_name_external], [cName], [ReportGroup])

SELECT DISTINCT
       epic_department_id
	 , epic_department_name
	 , epic_department_name_external
	 , cName
	 , ReportGroup
INTO #allacc
FROM #accsum

  -- Create index for temp table #allacc

  CREATE NONCLUSTERED INDEX IX_allacc ON #allacc ([epic_department_id], [epic_department_name], [epic_department_name_external], [cName], [ReportGroup])

SELECT acc.epic_department_id
     , acc.epic_department_name
	 , acc.epic_department_name_external
	 , acc.cName
	 , acc.ReportGroup
     , dt.day_date
	 , dt.fmonth_num
	 , dt.Fyear_num
	 , dt.FYear_name
INTO #accdatetable
FROM #allacc acc
CROSS JOIN #datetable dt

  -- Create index for temp table #accdatetable

  CREATE NONCLUSTERED INDEX IX_accdatetable ON #accdatetable ([day_date], [epic_department_id], [epic_department_name], [epic_department_name_external], [cName], [ReportGroup])

-----------------------------------------------------------------------------------------------------------

---BDD 7/27/2018 added insert to stage. Assumes truncate is handled in the SSIS package
INSERT INTO DS_HSDM_App.TabRptg.Dash_AmbOpt_CallServiceLevel_Department_Tiles
           ([event_type]
           ,[event_count]
           ,[event_date]
           ,[event_category]
           ,[pod_id]
           ,[pod_name]
           ,[hub_id]
           ,[hub_name]
           ,[epic_department_id]
           ,[epic_department_name]
           ,[epic_department_name_external]
           ,[fmonth_num]
           ,[Fyear_num]
           ,[FYear_name]
           ,[report_period]
           ,[report_date]
           ,[peds]
           ,[transplant]
           ,[sk_Dim_pt]
           ,[sk_Fact_Pt_Acct]
           ,[sk_Fact_Pt_Enc_Clrt]
           ,[person_birth_date]
           ,[person_gender]
           ,[person_id]
           ,[person_name]
           ,[practice_group_id]
           ,[practice_group_name]
           ,[provider_id]
           ,[provider_name]
           ,[service_line_id]
           ,[service_line]
           ,[sub_service_line_id]
           ,[sub_service_line]
           ,[opnl_service_id]
           ,[opnl_service_name]
           ,[corp_service_line_id]
           ,[corp_service_line_name]
           ,[hs_area_id]
           ,[hs_area_name]
           ,[nEnteredAcd]
           ,[nAbandonedAcd]
           ,[nAnsweredAcd]
           ,[nAnsweredAcdSvcLvl_20]
		   ,[ReportGroup]
		   )

    SELECT	DISTINCT
            CAST('Call Service Level' AS VARCHAR(50)) AS event_type
           ,CASE WHEN main.day_date IS NOT NULL THEN 1 ELSE 0 END AS event_count
           ,date_dim.day_date AS event_date
           ,CAST(date_dim.cName AS VARCHAR(150)) AS event_category
		   ,CAST(NULL AS INTEGER) AS pod_id
		   ,CAST(NULL AS VARCHAR(100)) AS pod_name
		   ,CAST(NULL AS VARCHAR(66)) AS hub_id
		   ,CAST(NULL AS VARCHAR(100)) AS hub_name
		   ,CAST(date_dim.epic_department_id AS NUMERIC(18,0)) AS epic_department_id
		   ,CAST(date_dim.epic_department_name AS VARCHAR(254)) AS epic_department_name
		   ,CAST(date_dim.epic_department_name_external AS VARCHAR(254)) AS epic_department_name_external
           ,date_dim.fmonth_num
           ,date_dim.Fyear_num
           ,date_dim.FYear_name
           ,CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date
		   ,CAST(NULL AS SMALLINT) AS peds
		   ,CAST(NULL AS SMALLINT) AS transplant
		   ,CAST(NULL AS INTEGER) AS sk_Dim_pt
		   ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Acct
		   ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
		   ,CAST(NULL AS DATE) AS person_birth_date
		   ,CAST(NULL AS VARCHAR(254)) AS person_gender
		   ,CAST(NULL AS INTEGER) AS person_id
		   ,CAST(NULL AS VARCHAR(200)) AS person_name
		   ,CAST(NULL AS INT) AS practice_group_id
		   ,CAST(NULL AS VARCHAR(150)) AS practice_group_name
		   ,CAST(NULL AS VARCHAR(18)) AS provider_id
		   ,CAST(NULL AS VARCHAR(200)) AS provider_name
			-- MDM
		   ,CAST(NULL AS SMALLINT) AS service_line_id
		   ,CAST(NULL AS VARCHAR(150)) AS service_line
		   ,CAST(NULL AS SMALLINT) AS sub_service_line_id
		   ,CAST(NULL AS VARCHAR(150)) AS sub_service_line
		   ,CAST(NULL AS SMALLINT) AS opnl_service_id
		   ,CAST(NULL AS VARCHAR(150)) AS opnl_service_name
		   ,CAST(NULL AS SMALLINT) AS corp_service_line_id
		   ,CAST(NULL AS VARCHAR(150)) AS corp_service_line
		   ,mdm.hs_area_id
		   ,mdm.hs_area_name
		   
		   ,CASE WHEN main.nEnteredAcd IS NULL THEN 0 ELSE main.nEnteredAcd END AS nEnteredAcd
		   ,CASE WHEN main.nAbandonedAcd IS NULL THEN 0 ELSE main.nAbandonedAcd END AS nAbandonedAcd
		   ,CASE WHEN main.nAnsweredAcd IS NULL THEN 0 ELSE main.nAnsweredAcd END AS nAnsweredAcd
		   ,CASE WHEN main.nAnsweredAcdSvcLvl_20 IS NULL THEN 0 ELSE main.nAnsweredAcdSvcLvl_20 END AS nAnsweredAcdSvcLvl_20
		   ,date_dim.ReportGroup

        FROM
            #accdatetable AS date_dim
        LEFT OUTER JOIN (
                         --main

							SELECT
									acc.epic_department_id
								   ,acc.epic_department_name
								   ,acc.epic_department_name_external
								   ,acc.cName
								   ,acc.ReportGroup

							--Select
								   ,acc.day_date
							       ,SUM(acc.nEnteredAcd) AS nEnteredAcd
							       ,SUM(acc.nAbandonedAcd) AS nAbandonedAcd
							       ,SUM(acc.nAnsweredAcd) AS nAnsweredAcd
								   ,SUM(acc.nAnsweredAcdSvcLvl_20) AS nAnsweredAcdSvcLvl_20
								FROM
									#accsum acc
								GROUP BY acc.epic_department_id
								       , acc.epic_department_name
									   , acc.epic_department_name_external
									   , acc.cName
									   , acc.ReportGroup
									   , acc.day_date
	         ) main
        ON  ((date_dim.day_date = main.day_date)
		     AND (date_dim.epic_department_id = main.epic_department_id)
		     AND (date_dim.epic_department_name = main.epic_department_name)
		     AND (date_dim.epic_department_name_external = main.epic_department_name_external)
			 AND (date_dim.cName = main.cName)
			 AND (date_dim.ReportGroup = main.ReportGroup))
        LEFT OUTER JOIN #depmapping pod
        ON date_dim.cName = pod.Workgroup
		LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc mdm
		ON date_dim.epic_department_id = mdm.epic_department_id
        WHERE
            date_dim.day_date >= @locstartdate
            AND date_dim.day_date < @locenddate
		ORDER BY CASE WHEN main.day_date IS NOT NULL THEN 1 ELSE 0 END DESC
		       , CAST(date_dim.epic_department_id AS NUMERIC(18,0)) 
			   , CAST(date_dim.epic_department_name AS VARCHAR(254))
			   , CAST(date_dim.cName AS VARCHAR(150))
			   , date_dim.ReportGroup
			   , date_dim.day_date

GO


