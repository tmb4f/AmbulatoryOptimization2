USE DS_HSDM_App

IF OBJECT_ID('tempdb..#CallServiceLevel ') IS NOT NULL
DROP TABLE #CallServiceLevel

SELECT rptg.*
INTO #CallServiceLevel
FROM
(
SELECT [pod_name] AS Pod
      ,[event_category] AS ACC_Workgroup
      ,SUM([nAnsweredAcd]) AS Answered
      ,SUM([nAnsweredAcdSvcLvl_20]) AS AnsweredSvcLvl20
	  ,CAST(CAST(SUM([nAnsweredAcdSvcLvl_20]) AS NUMERIC(8,2)) / CAST(SUM([nAnsweredAcd]) AS NUMERIC(8,2)) * 100.0 AS NUMERIC(5,2)) AS SvcLvlRate
	  ,MAX(SUBSTRING(pod_name,1,1)) AS sortall
  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_CallServiceLevel_Pod_Tiles]
  --WHERE event_date >= '1/1/2019'
  WHERE event_date >= '7/1/2019'
  AND event_date <= '7/31/2019'
  AND event_count = 1
  AND pod_name <> 'Non-Pod'
  GROUP BY pod_name
         , event_category
  HAVING SUM([nAnsweredAcd]) > 0
--UNION ALL
--SELECT [pod_name] AS Pod
--      ,[event_category] AS ACC_Workgroup
--      ,SUM([nAnsweredAcd]) AS Answered
--      ,SUM([nAnsweredAcdSvcLvl_20]) AS AnsweredSvcLvl20
--	  ,CAST(CAST(SUM([nAnsweredAcdSvcLvl_20]) AS NUMERIC(8,2)) / CAST(SUM([nAnsweredAcd]) AS NUMERIC(8,2)) * 100.0 AS NUMERIC(5,2)) AS SvcLvlRate
--	  ,MAX('Z') AS sortall
--  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_CallServiceLevel_Pod_Tiles]
--  --WHERE event_date >= '1/1/2019'
--  WHERE event_date >= '7/1/2019'
--  AND event_date <= '7/31/2019'
--  AND event_count = 1
--  AND pod_name = 'Non-Pod'
--  GROUP BY pod_name
--         , event_category
--  HAVING SUM([nAnsweredAcd]) > 0
  ) rptg

  --SELECT *
  --FROM #CallServiceLevel
  ----ORDER BY pod_name
  ----       , event_category
  --ORDER BY sortall
  --       , ACC_Workgroup

--SELECT SUM([nAnsweredAcd]) AS Answered
--      ,SUM([nAnsweredAcdSvcLvl_20]) AS AnsweredSvcLvl20
--	  ,CAST(SUM([nAnsweredAcdSvcLvl_20]) AS NUMERIC(13,2)) / CAST(SUM([nAnsweredAcd]) AS NUMERIC(13,2)) * 100.0 AS SvcLvlRate
--  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_CallServiceLevel_Pod_Tiles]
--  WHERE event_date >= '7/1/2018'
--  AND event_date <= '6/30/2019'
--  AND event_count = 1

SELECT SUM(Answered) AS Answered
      ,SUM(AnsweredSvcLvl20) AS AnsweredSvcLvl20
	  ,CAST(SUM(AnsweredSvcLvl20) AS NUMERIC(13,2)) / CAST(SUM(Answered) AS NUMERIC(13,2)) * 100.0 AS SvcLvlRate
  FROM #CallServiceLevel
  --WHERE sortall <> 'Z'