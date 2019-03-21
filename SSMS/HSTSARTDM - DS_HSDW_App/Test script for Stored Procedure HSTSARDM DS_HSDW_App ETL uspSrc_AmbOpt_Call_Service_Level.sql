USE [DS_HSDW_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate SMALLDATETIME = NULL

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Call_Service_Level]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Call_Service_Level
--WHO : Tom Burgan
--WHEN: 11/7/18
--WHY : Report ACC call system call servie level rates.
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:
--              DS_HSDW_App.ETL.usp_Get_Dash_Dates_BalancedScorecard
--              DS_HSDW_Prod.Rptg.vwDim_Date
--              DS_HSDW_Prod.CallCenter.ACC_PhoneData_QueueSummary
--              DS_HSDW_App.CallCenter.ACC_PhoneData_Pod_Mapping
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Call_Service_Level]
-- 
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--       11/07/2018 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC DS_HSDM_App.ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 

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

if OBJECT_ID('tempdb..#datetable') is not NULL
DROP TABLE #datetable

if OBJECT_ID('tempdb..#podmapping') is not NULL
DROP TABLE #podmapping

if OBJECT_ID('tempdb..#acc') is not NULL
DROP TABLE #acc

if OBJECT_ID('tempdb..#accsum') is not NULL
DROP TABLE #accsum

if OBJECT_ID('tempdb..#allacc') is not NULL
DROP TABLE #allacc

if OBJECT_ID('tempdb..#accdatetable') is not NULL
DROP TABLE #accdatetable

if OBJECT_ID('tempdb..#AmbOpt_Dash_CallAnswerRate') is not NULL
DROP TABLE #AmbOpt_Dash_CallAnswerRate

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
	 , Pod
	 , OperationalOwner
INTO #podmapping
FROM [CallCenter].[ACC_Workgroup_Pod_Mapping]
ORDER BY Workgroup

  -- Create index for temp table #podmapping
  CREATE UNIQUE CLUSTERED INDEX IX_podmapping ON #podmapping ([Workgroup])

------------------------------------------------------------------------------------------

SELECT [cName]
      ,CAST([cReportGroup] AS VARCHAR(20)) AS ReportGroup
      ,CASE
		 WHEN pod.[Pod] IS NULL THEN 'Non-Pod'
		 ELSE pod.[Pod]
	   END AS Pod
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
  LEFT OUTER JOIN #podmapping pod
  ON pod.Workgroup = accqs.cName

  SELECT *
  FROM #acc
  ORDER BY Pod
         , day_date
/*
SELECT
       acc.day_date
      ,CASE
	     WHEN ref.POD_ID IS NULL THEN -1
		 ELSE ref.POD_ID
	   END AS pod_id
	  ,acc.Pod AS pod_name
	  ,acc.ReportGroup
	  ,acc.cName
      ,acc.nEnteredAcd
	  ,acc.nAbandonedAcd
	  ,acc.nAnsweredAcd
      ,acc.nAnsweredAcdSvcLvl1 + acc.nAnsweredAcdSvcLvl2 + acc.nAnsweredAcdSvcLvl3 AS nAnsweredAcdSvcLvl_20
  INTO #accsum
  FROM #acc acc
  LEFT OUTER JOIN (SELECT DISTINCT
                          POD_ID
						, PFA_POD
                   FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
				   WHERE PFA_POD IS NOT NULL) ref ON ref.PFA_POD = acc.Pod

  -- Create index for temp table #accsum

  CREATE NONCLUSTERED INDEX IX_accsum ON #accsum ([day_date], [pod_id], [pod_name], [cName], [ReportGroup])

SELECT DISTINCT
       pod_id
	 , pod_name
	 , cName
	 , ReportGroup
INTO #allacc
FROM #accsum

  -- Create index for temp table #allacc

  CREATE NONCLUSTERED INDEX IX_allacc ON #allacc ([pod_id], [pod_name], [cName], [ReportGroup])

SELECT acc.pod_id
     , acc.pod_name
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

  CREATE NONCLUSTERED INDEX IX_accdatetable ON #accdatetable ([day_date], [pod_id], [pod_name], [cName], [ReportGroup])

-----------------------------------------------------------------------------------------------------------
---BDD 7/27/2018 added insert to stage. Assumes truncate is handled in the SSIS package
INSERT INTO DS_HSDM_App.Stage.AmbOpt_Dash_CallServiceLevel
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
           ,[corp_service_line]
           ,[hs_area_id]
           ,[hs_area_name]
           ,[nEnteredAcd]
           ,[nAbandonedAcd]
           ,[nAnsweredAcd]
           ,[nAnsweredAcdSvcLvl_20]
		   ,[ReportGroup]
		   ,[OperationalOwner]
		   )
    SELECT	DISTINCT
            CAST('Call Service Level' AS VARCHAR(50)) AS event_type
           ,CASE WHEN main.day_date IS NOT NULL THEN 1 ELSE 0 END AS event_count
           ,date_dim.day_date AS event_date
           ,CAST(date_dim.cName AS VARCHAR(150)) AS event_category
		   ,CASE WHEN date_dim.pod_id = -1 THEN NULL ELSE date_dim.pod_id END AS pod_id
		   ,CASE WHEN date_dim.pod_name = 'Unknown' THEN CAST(NULL AS VARCHAR(100)) ELSE CAST(date_dim.pod_name AS VARCHAR(100)) END AS pod_name
		   ,CAST(NULL AS VARCHAR(66)) AS hub_id
		   ,CAST(NULL AS VARCHAR(100)) AS hub_name
		   ,CAST(NULL AS NUMERIC(18,0)) AS epic_department_id
		   ,CAST(NULL AS VARCHAR(254)) AS epic_department_name
		   ,CAST(NULL AS VARCHAR(254)) AS epic_department_name_external
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
		   ,CAST(NULL AS SMALLINT) AS hs_area_id
		   ,CAST(NULL AS VARCHAR(150)) AS hs_area_name
		   
		   ,CASE WHEN main.nEnteredAcd IS NULL THEN 0 ELSE main.nEnteredAcd END AS nEnteredAcd
		   ,CASE WHEN main.nAbandonedAcd IS NULL THEN 0 ELSE main.nAbandonedAcd END AS nAbandonedAcd
		   ,CASE WHEN main.nAnsweredAcd IS NULL THEN 0 ELSE main.nAnsweredAcd END AS nAnsweredAcd
		   ,CASE WHEN main.nAnsweredAcdSvcLvl_20 IS NULL THEN 0 ELSE main.nAnsweredAcdSvcLvl_20 END AS nAnsweredAcdSvcLvl_20
		   ,date_dim.ReportGroup
		   ,pod.OperationalOwner

        FROM
            #accdatetable AS date_dim
        LEFT OUTER JOIN (
                         --main

							SELECT
									acc.pod_id
								   ,acc.pod_name
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
								GROUP BY acc.pod_id
								       , acc.pod_name
									   , acc.cName
									   , acc.ReportGroup
									   , acc.day_date
	         ) main
        ON  ((date_dim.day_date = main.day_date)
		     AND (date_dim.pod_id = main.pod_id)
		     AND (date_dim.pod_name = main.pod_name)
			 AND (date_dim.cName = main.cName)
			 AND (date_dim.ReportGroup = main.ReportGroup))
        LEFT OUTER JOIN #podmapping pod
        ON date_dim.cName = pod.Workgroup
        WHERE
            date_dim.day_date >= @locstartdate
            AND date_dim.day_date < @locenddate
		ORDER BY CASE WHEN main.day_date IS NOT NULL THEN 1 ELSE 0 END DESC
		       , CASE WHEN date_dim.pod_id = -1 THEN NULL ELSE date_dim.pod_id END 
			   , CASE WHEN date_dim.pod_name = 'Unknown' THEN CAST(NULL AS VARCHAR(100)) ELSE CAST(date_dim.pod_name AS VARCHAR(100)) END
			   , CAST(date_dim.cName AS VARCHAR(150))
			   , date_dim.ReportGroup
			   , date_dim.day_date
*/
GO


