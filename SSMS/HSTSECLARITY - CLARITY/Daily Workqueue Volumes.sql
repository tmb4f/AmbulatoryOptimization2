USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate SMALLDATETIME = NULL

--CREATE PROCEDURE [ETL].[uspSrc_AmbOpt_Workqueue_Volume]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Workqueue_Volume
--WHO : Tom Burgan
--WHEN: 1/16/18
--WHY : Report workqueue volumes for items in patient, referrals, and scheduled orders workqueues.
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	
--              CLARITY.dbo.REFERRAL_WQ_USR_HX
--              CLARITY.dbo.REFERRAL_WQ_ITEMS
--              CLARITY..REFERRAL_WQ
--              Rptg.vwDim_Date
--              CLARITY.dbo.V_ZC_SCHED_WQ_TAB
--              CLARITY.dbo.SCHED_ORDERS_HX sactivity
--              CLARITY.dbo.SCHED_ORDERS_WQ_ITEMS
--              CLARITY..SCHED_ORDERS_WQ
--              CLARITY.dbo.V_ZC_SCHED_WQ_TAB
--              CLARITY.dbo.PAT_WQ_ITEMS
--              CLARITY.dbo.PAT_WQ
--              CLARITY.dbo.ZC_OWNING_AREA_2
--              CLARITY.dbo.ZC_DEP_RPT_GRP_6
--              Rptg.vwRef_MDM_Location_Master
--              CLARITY.dbo.ZC_DEP_RPT_GRP_7
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Workqueue_Volume]
-- 
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         1/16/2018 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;

	--SET @startdate = '5/1/2017 00:00 AM'

	--SET @enddate = '4/25/2018 00:00 AM'

	SET @startdate = '11/1/2018 00:00 AM'

	SET @enddate = '2/12/2019 00:00 AM'
 
 
-------------------------------------------------------------------------------

--SELECT @startdate, @enddate

if OBJECT_ID('tempdb..#datetable') is not NULL
DROP TABLE #datetable

if OBJECT_ID('tempdb..#refwqhx') is not NULL
DROP TABLE #refwqhx

if OBJECT_ID('tempdb..#refcross') is not NULL
DROP TABLE #refcross

if OBJECT_ID('tempdb..#rptdate') is not NULL
DROP TABLE #rptdate

if OBJECT_ID('tempdb..#refwqsum') is not NULL
DROP TABLE #refwqsum

if OBJECT_ID('tempdb..#schwqhx') is not NULL
DROP TABLE #schwqhx

if OBJECT_ID('tempdb..#schcross') is not NULL
DROP TABLE #schcross

if OBJECT_ID('tempdb..#schwqhx2') is not NULL
DROP TABLE #schwqhx2

if OBJECT_ID('tempdb..#rptdate2') is not NULL
DROP TABLE #rptdate2

if OBJECT_ID('tempdb..#schwqsum') is not NULL
DROP TABLE #schwqsum

if OBJECT_ID('tempdb..#patwqhx') is not NULL
DROP TABLE #patwqhx

if OBJECT_ID('tempdb..#patcross') is not NULL
DROP TABLE #patcross

if OBJECT_ID('tempdb..#rptdate3') is not NULL
DROP TABLE #rptdate3

if OBJECT_ID('tempdb..#patwqsum') is not NULL
DROP TABLE #patwqsum

if OBJECT_ID('tempdb..#allwqsum') is not NULL
DROP TABLE #allwqsum

SELECT date_dim.day_date
INTO #datetable
FROM Rptg.vwDim_Date AS date_dim
WHERE date_dim.day_date >= @startdate
AND date_dim.day_date < @enddate

  -- Create index for temp table #datetable

  CREATE UNIQUE CLUSTERED INDEX IX_datetable ON #datetable ([day_date])

SELECT  *
INTO #refwqhx
FROM    (

SELECT				ractivity.LINE,
					ractivity.ITEM_ID,
				    entry_date = MAX(case when HISTORY_ACTIVITY_C IN (1,2) then START_INSTANT_DTTM end) OVER (PARTITION BY ractivity.ITEM_ID ORDER BY ractivity.ITEM_ID),
			        release_date = MAX(case when HISTORY_ACTIVITY_C IN (3) then START_INSTANT_DTTM end) OVER (PARTITION BY ractivity.ITEM_ID ORDER BY ractivity.ITEM_ID),
					last_action = LAST_VALUE(ractivity.HISTORY_ACTIVITY_C) OVER (PARTITION BY ractivity.ITEM_ID ORDER BY ractivity.ITEM_ID) ,
				    last_tab= LAST_VALUE(ractivity.TAB_NUMBER_C)OVER (PARTITION BY ractivity.ITEM_ID ORDER BY ractivity.ITEM_ID) ,
					ritems.WORKQUEUE_ID ,
					rwq.WORKQUEUE_NAME ,
					rwq.OWNING_AREA_C ,
					ritems.TAB_STATUS_C ,
					rwq.SUPERVISOR_ID

FROM clarity..REFERRAL_WQ_USR_HX ractivity
INNER JOIN clarity..REFERRAL_WQ_ITEMS ritems ON ractivity.item_id=ritems.ITEM_ID-- AND ractivity.LINE = 1
INNER JOIN CLARITY..REFERRAL_WQ rwq ON rwq.WORKQUEUE_ID = ritems.WORKQUEUE_ID
WHERE 1=1
AND ractivity.START_INSTANT_DTTM < @enddate
--AND rwq.WORKQUEUE_NAME LIKE '%unsched%'
AND rwq.QUEUE_ACTIVE_YN = 'Y' -- active wq only
--AND ritems.ITEM_ID = '49035392'
--AND ritems.ITEM_ID = '49040814'
--AND ritems.ITEM_ID IN ('49035392','49040814')
) items

  -- Create index for temp table #refwqhx

  CREATE UNIQUE CLUSTERED INDEX IX_refwqhx ON #refwqhx ([entry_date], [release_date], [WORKQUEUE_ID], [ITEM_ID], [LINE])

--SELECT *
--FROM #refwqhx
--WHERE LINE = 1
--ORDER BY WORKQUEUE_ID
--        ,ITEM_ID

SELECT              date_dim.day_date ,
					refwqhx.ITEM_ID ,
                    refwqhx.entry_date ,
					refwqhx.release_date ,
					refwqhx.last_action ,
					refwqhx.last_tab ,
                    refwqhx.WORKQUEUE_ID ,
                    refwqhx.WORKQUEUE_NAME ,
					refwqhx.OWNING_AREA_C ,
					refwqhx.SUPERVISOR_ID-- ,
					--CASE 
					--	WHEN 1=1
					--	AND ((refwqhx.last_action <> 3) and (refwqhx.RELEASE_DATE IS NULL OR refwqhx.RELEASE_DATE > = date_dim.day_date)  AND (refwqhx.ENTRY_DATE < = date_dim.day_date))
					--	OR ((refwqhx.last_action = 3 AND refwqhx.release_date >= date_dim.day_date) AND (refwqhx.ENTRY_DATE < = date_dim.day_date ))
					--		THEN 1
					--END ct_items
INTO #refcross
FROM #datetable date_dim
CROSS JOIN (SELECT ITEM_ID
                 , entry_date
			     , release_date
				 , last_action
				 , last_tab
				 , WORKQUEUE_ID
				 , WORKQUEUE_NAME
				 , OWNING_AREA_C
				 , TAB_STATUS_C
				 , SUPERVISOR_ID
            FROM #refwqhx
			WHERE LINE = 1) refwqhx
WHERE  (CAST(refwqhx.entry_date AS SMALLDATETIME) < CAST(GETDATE() AS SMALLDATETIME)
AND CAST(refwqhx.entry_date AS SMALLDATETIME) < date_dim.day_date
AND ((refwqhx.release_date IS NULL)
	 OR (CAST(refwqhx.release_date AS SMALLDATETIME) > date_dim.day_date)))

  -- Create index for temp table #refcross

  CREATE UNIQUE CLUSTERED INDEX IX_refcross ON #refcross ([last_action], [release_date], [entry_date], [day_date], [WORKQUEUE_ID], [ITEM_ID])

SELECT              rpt.day_date ,
					rpt.ITEM_ID ,
                    rpt.entry_date ,
					rpt.release_date ,
                    rpt.WORKQUEUE_ID ,
                    rpt.WORKQUEUE_NAME ,
					rpt.OWNING_AREA_C ,
					rpt.SUPERVISOR_ID ,
					CASE 
						WHEN 1=1
						AND ((rpt.last_action <> 3) and (rpt.RELEASE_DATE IS NULL OR rpt.RELEASE_DATE > = day_date)  AND (rpt.ENTRY_DATE < = day_date))
						OR ((rpt.last_action = 3 AND rpt.release_date >= day_date) AND (rpt.ENTRY_DATE < = day_date ))
							THEN 1
						ELSE 0
					END AS ct_items
INTO #rptdate
FROM #refcross rpt

  -- Create index for temp table #rptdate

  CREATE UNIQUE CLUSTERED INDEX IX_rptdate ON #rptdate ([day_date], [OWNING_AREA_C], [release_date], [entry_date], [WORKQUEUE_ID], [ITEM_ID])

--SELECT *
--FROM #rptdate
--ORDER BY ITEM_ID
--       , day_date
----ORDER BY day_date
----       , ITEM_ID

SELECT  ref_items.day_date ,
ref_items.OWNING_AREA_C ,
ref_items.WORKQUEUE_ID ,
ref_items.WORKQUEUE_NAME ,
ct_items = SUM(ct_items),

header = 'UNSCHED REF WQ'

INTO #refwqsum
FROM  #rptdate ref_items

GROUP BY ref_items.day_date ,
         ref_items.OWNING_AREA_C ,
         ref_items.WORKQUEUE_ID ,
         ref_items.WORKQUEUE_NAME

  -- Create index for temp table #refwqsum

  CREATE UNIQUE CLUSTERED INDEX IX_refwqsum ON #refwqsum ([day_date], [OWNING_AREA_C], [WORKQUEUE_ID])

--SELECT *
--FROM #refwqsum
--ORDER BY day_date
--       , OWNING_AREA_C

SELECT  *
INTO #schwqhx
FROM    (

SELECT				sactivity.LINE,
					sactivity.ITEM_ID,
					entry_date = MAX(case when ACTIVITY_C IN (1,2) then START_DTTM end) OVER (PARTITION BY sactivity.ITEM_ID ORDER BY sactivity.ITEM_ID) ,
					release_date = MAX(case when ACTIVITY_C IN (3) then START_DTTM end) OVER (PARTITION BY sactivity.ITEM_ID ORDER BY sactivity.ITEM_ID) ,
					last_action = LAST_VALUE(sactivity.ACTIVITY_C) OVER (PARTITION BY sactivity.ITEM_ID ORDER BY sactivity.ITEM_ID) ,
					last_tab= LAST_VALUE(sactivity.TAB_STATUS_C) OVER (PARTITION BY sactivity.ITEM_ID ORDER BY sactivity.ITEM_ID) ,
				    sitems.WORKQUEUE_ID ,
					swq.WORKQUEUE_NAME ,
					swq.OWNING_AREA_C ,
					sitems.TAB_STATUS_C ,
					swq.SUPERVISOR_ID

FROM clarity..SCHED_ORDERS_HX sactivity
INNER JOIN clarity..SCHED_ORDERS_WQ_ITEMS sitems ON sactivity.item_id=sitems.ITEM_ID
INNER JOIN CLARITY..SCHED_ORDERS_WQ swq ON swq.WORKQUEUE_ID = sitems.WORKQUEUE_ID
WHERE 1=1
AND sactivity.START_DTTM < @enddate
AND swq.QUEUE_ACTIVE_YN = 'Y' -- active wq only
--AND sactivity.LINE = 1
) items

  -- Create index for temp table #schwqhx

  CREATE UNIQUE CLUSTERED INDEX IX_schwqhx ON #schwqhx ([entry_date], [release_date], [WORKQUEUE_ID], [ITEM_ID], [LINE])

--SELECT *
--FROM #schwqhx
--ORDER BY entry_date
--       , release_date

SELECT              date_dim.day_date ,
					schwqhx.ITEM_ID ,
                    schwqhx.entry_date ,
					schwqhx.release_date ,
					schwqhx.last_action ,
					schwqhx.last_tab ,
                    schwqhx.WORKQUEUE_ID ,
                    schwqhx.WORKQUEUE_NAME ,
			        schwqhx.OWNING_AREA_C ,
					schwqhx.SUPERVISOR_ID
INTO #schcross
FROM #datetable date_dim
CROSS JOIN (SELECT ITEM_ID
                 , entry_date
			     , release_date
				 , last_action
				 , last_tab
				 , WORKQUEUE_ID
			     , WORKQUEUE_NAME
				 , OWNING_AREA_C
				 , TAB_STATUS_C
				 , SUPERVISOR_ID
            FROM #schwqhx
			WHERE LINE = 1
		   ) schwqhx
WHERE  (CAST(schwqhx.entry_date AS SMALLDATETIME) < CAST(GETDATE() AS SMALLDATETIME)
AND CAST(schwqhx.entry_date AS SMALLDATETIME) < date_dim.day_date
AND ((schwqhx.release_date IS NULL)
	 OR (CAST(schwqhx.release_date AS SMALLDATETIME) > date_dim.day_date)))

  -- Create index for temp table #schcross

  CREATE UNIQUE CLUSTERED INDEX IX_schcross ON #schcross ([last_action], [release_date], [entry_date], [day_date], [WORKQUEUE_ID], [ITEM_ID])
  
SELECT              rpt.day_date ,
					rpt.ITEM_ID ,
                    rpt.entry_date ,
					rpt.release_date ,
                    rpt.WORKQUEUE_ID ,
                    rpt.WORKQUEUE_NAME ,
			        rpt.OWNING_AREA_C ,
					rpt.SUPERVISOR_ID ,
					CASE 
					    WHEN rpt.last_tab <> 2 
						AND ((rpt.last_action <> 3)
						     AND (rpt.RELEASE_DATE IS NULL OR rpt.RELEASE_DATE > = rpt.day_date)
							 AND (rpt.ENTRY_DATE < = rpt.day_date)
							)
						OR ((rpt.last_action = 3 AND rpt.release_date >= rpt.day_date)
						    AND (rpt.ENTRY_DATE < = rpt.day_date ))
						THEN 1
						ELSE 0
					END AS ct_items
INTO #rptdate2
FROM #schcross rpt

  -- Create index for temp table #rptdate2

  CREATE UNIQUE CLUSTERED INDEX IX_rptdate2 ON #rptdate2 ([day_date], [OWNING_AREA_C], [release_date], [entry_date], [WORKQUEUE_ID], [ITEM_ID])

SELECT  sch_items.day_date ,
sch_items.OWNING_AREA_C ,
sch_items.WORKQUEUE_ID ,
sch_items.WORKQUEUE_NAME ,
ct_items = SUM(ct_items),

header = 'SCHED ORDER WQ'

INTO #schwqsum
FROM  #rptdate2 sch_items

GROUP BY sch_items.day_date ,
         sch_items.OWNING_AREA_C ,
         sch_items.WORKQUEUE_ID ,
         sch_items.WORKQUEUE_NAME

  -- Create index for temp table #schwqsum

  CREATE UNIQUE CLUSTERED INDEX IX_schwqsum ON #schwqsum ([day_date], [OWNING_AREA_C], [WORKQUEUE_ID])

--SELECT *
--FROM #schwqsum
--ORDER BY day_date
--       , OWNING_AREA_C

--SELECT  *
--INTO #patwqhx
--FROM    (

--SELECT				pitems.ITEM_ID ,
--					pitems.ENTRY_DATE AS entry_date ,
--					pitems.RELEASE_DATE AS release_date ,
--					pitems.WORKQUEUE_ID ,
--					pwq.WORKQUEUE_NAME ,
--					pwq.OWNING_AREA_C ,
--					pwq.OWNINGSUPERVISOR_ID AS SUPERVISOR_ID

--FROM    CLARITY.dbo.PAT_WQ_ITEMS pitems
--INNER JOIN CLARITY.dbo.PAT_WQ pwq ON pwq.WORKQUEUE_ID = pitems.WORKQUEUE_ID
--WHERE 1=1
--AND pitems.ENTRY_DATE < @enddate
--AND pwq.IS_QUEUE_ACTIVE_YN = 'Y' -- active wq only
--) items

--  -- Create index for temp table #patwqhx

--  CREATE UNIQUE CLUSTERED INDEX IX_patwqhx ON #patwqhx ([entry_date], [release_date], [WORKQUEUE_ID], [ITEM_ID])

--SELECT              date_dim.day_date ,
--					patwqhx.ITEM_ID ,
--                    patwqhx.entry_date ,
--					patwqhx.release_date ,
--                    patwqhx.WORKQUEUE_ID ,
--                    patwqhx.WORKQUEUE_NAME ,
--			        patwqhx.OWNING_AREA_C ,
--					patwqhx.SUPERVISOR_ID-- ,
--					--CASE 
--					--    WHEN (patwqhx.release_date IS NULL OR patwqhx.release_date > = date_dim.day_date)  AND (patwqhx.entry_date < = date_dim.day_date)
--					--		THEN 1
--					--END ct_items
--INTO #patcross
--FROM #datetable date_dim
--CROSS JOIN (SELECT ITEM_ID
--                 , entry_date
--			     , release_date
--			     , WORKQUEUE_ID
--				 , WORKQUEUE_NAME
--				 , OWNING_AREA_C
--				 , SUPERVISOR_ID
--            FROM #patwqhx) patwqhx
--WHERE  (CAST(patwqhx.entry_date AS SMALLDATETIME) < CAST(GETDATE() AS SMALLDATETIME)
--AND CAST(patwqhx.entry_date AS SMALLDATETIME) < date_dim.day_date
--AND ((patwqhx.release_date IS NULL)
--	 OR (CAST(patwqhx.release_date AS SMALLDATETIME) > date_dim.day_date)))

--  -- Create index for temp table #patcross

--  CREATE UNIQUE CLUSTERED INDEX IX_patcross ON #patcross([release_date], [entry_date], [day_date], [WORKQUEUE_ID], [ITEM_ID])

--SELECT              rpt.day_date ,
--					rpt.ITEM_ID ,
--                    rpt.entry_date ,
--					rpt.release_date ,
--                    rpt.WORKQUEUE_ID ,
--                    rpt.WORKQUEUE_NAME ,
--			        rpt.OWNING_AREA_C ,
--					rpt.SUPERVISOR_ID ,
--					CASE 
--					    WHEN (rpt.release_date IS NULL OR rpt.release_date > = rpt.day_date)  AND (rpt.entry_date < = rpt.day_date)
--							THEN 1
--							ELSE 0
--					END ct_items
--INTO #rptdate3
--FROM #patcross rpt

--  -- Create index for temp table #rptdate3

--  CREATE UNIQUE CLUSTERED INDEX IX_rptdate3 ON #rptdate3([day_date], [OWNING_AREA_C], [release_date], [entry_date], [WORKQUEUE_ID], [ITEM_ID])

--SELECT  pat_items.day_date ,
--pat_items.OWNING_AREA_C ,
--ct_items = SUM(ct_items),

--header = 'REG PAT WQ'

--INTO #patwqsum
--FROM  #rptdate3 pat_items

--GROUP BY pat_items.day_date ,
--         pat_items.OWNING_AREA_C

--  -- Create index for temp table #patwqsum

--  CREATE UNIQUE CLUSTERED INDEX IX_patwqsum ON #patwqsum ([day_date], [OWNING_AREA_C])

--SELECT *
--FROM #patwqsum
--ORDER BY day_date
--       , OWNING_AREA_C

SELECT
       wq.day_date AS [ReportDate]
      ,pod.INTERNAL_ID AS [PodId]
	  ,CASE
	     WHEN pod.NAME IS NULL THEN 'Unknown'
		 ELSE pod.NAME
	   END AS [PodName]
	  ,wq.header AS [WQType]
	  ,wq.WORKQUEUE_ID AS [WQId]
	  ,wq.WORKQUEUE_NAME AS [WQName]
	  ,wq.ct_items AS [WQVolume]
  FROM
  (SELECT refwq.day_date
         ,refwq.OWNING_AREA_C
		 ,refwq.WORKQUEUE_ID
		 ,refwq.WORKQUEUE_NAME
		 ,refwq.header
		 ,refwq.ct_items
   FROM #refwqsum refwq
   UNION ALL
   SELECT schwq.day_date
         ,schwq.OWNING_AREA_C
		 ,schwq.WORKQUEUE_ID
		 ,schwq.WORKQUEUE_NAME
		 ,schwq.header
		 ,schwq.ct_items
   FROM #schwqsum schwq
   --UNION ALL
   --SELECT patwq.day_date
   --      ,patwq.OWNING_AREA_C
		 --,patwq.header
		 --,patwq.ct_items
   --FROM #patwqsum patwq
   ) wq
LEFT OUTER JOIN clarity..ZC_OWNING_AREA_2 oa ON wq.OWNING_AREA_C = oa.OWNING_AREA_2_C
LEFT OUTER JOIN CLARITY.dbo.ZC_DEP_RPT_GRP_6 pod ON SUBSTRING(pod.NAME,1,3) = SUBSTRING(oa.NAME,1,3)
--ORDER BY PivotTable.day_date
--       , pod.INTERNAL_ID
ORDER BY wq.header
       , pod.NAME
	   , wq.WORKQUEUE_NAME
       , wq.day_date
/*
SELECT
       PivotTable.day_date
      ,pod.INTERNAL_ID AS pod_id
	  ,CASE
	     WHEN pod.NAME IS NULL THEN 'Unknown'
		 ELSE pod.NAME
	   END AS pod_name
	  ,[UNSCHED REF WQ] AS UNSCHED_REF_WQ
	  ,[SCHED ORDER WQ] AS SCHED_ORDER_WQ
	  ,[REG PAT WQ] AS REG_PAT_WQ
  INTO #allwqsum
  FROM
  (SELECT refwq.day_date
         ,refwq.OWNING_AREA_C
		 ,refwq.header
		 ,refwq.ct_items
   FROM #refwqsum refwq
   UNION ALL
   SELECT schwq.day_date
         ,schwq.OWNING_AREA_C
		 ,schwq.header
		 ,schwq.ct_items
   FROM #schwqsum schwq
   UNION ALL
   SELECT patwq.day_date
         ,patwq.OWNING_AREA_C
		 ,patwq.header
		 ,patwq.ct_items
   FROM #patwqsum patwq) wq
  PIVOT
  (
  MAX(ct_items)
  FOR header IN ([UNSCHED REF WQ],[SCHED ORDER WQ],[REG PAT WQ])
  ) AS PivotTable
LEFT OUTER JOIN clarity..ZC_OWNING_AREA_2 oa ON PivotTable.OWNING_AREA_C = oa.OWNING_AREA_2_C
LEFT OUTER JOIN CLARITY.dbo.ZC_DEP_RPT_GRP_6 pod ON SUBSTRING(pod.NAME,1,3) = SUBSTRING(oa.NAME,1,3)
ORDER BY PivotTable.day_date
       , pod.INTERNAL_ID

  -- Create index for temp table #allwqsum

  CREATE NONCLUSTERED INDEX IX_allwqsum ON #allwqsum ([day_date], [pod_name])

    SELECT	DISTINCT
            CAST('Volume' AS VARCHAR(50)) AS event_type
           ,CASE WHEN main.day_date IS NOT NULL THEN COALESCE(main.UNSCHED_REF_WQ,0) +
		                                             COALESCE(main.SCHED_ORDER_WQ,0) +
		                                             COALESCE(main.REG_PAT_WQ,0)
                 ELSE 0
            END AS event_count
           ,date_dim.day_date AS event_date
           ,CAST(NULL AS VARCHAR(150)) AS event_category
		   ,main.epic_pod AS pod_id
		   ,main.epic_pod_name AS pod_name
		   ,main.hub_id
		   ,main.hub_name
           ,main.epic_department_id
           ,main.epic_department_name
           ,main.epic_department_name_external
           ,date_dim.fmonth_num
           ,date_dim.Fyear_num
           ,date_dim.FYear_name
           ,CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date
           ,main.peds
           ,main.transplant
           ,main.sk_Dim_pt
           ,main.sk_Fact_Pt_Acct
           ,main.sk_Fact_Pt_Enc_Clrt
           ,main.person_birth_date
           ,main.person_gender
           ,main.person_id
           ,main.person_name
           ,main.practice_group_id
           ,main.practice_group_name
           ,main.provider_id
           ,main.provider_name
           ,main.service_line_id
           ,main.service_line
           ,main.sub_service_line_id
           ,main.sub_service_line
           ,main.opnl_service_id
           ,main.opnl_service_name
           ,main.corp_service_line_id
           ,main.corp_service_line
           ,main.hs_area_id
           ,main.hs_area_name
		   ,main.UNSCHED_REF_WQ
		   ,main.SCHED_ORDER_WQ
		   ,main.REG_PAT_WQ
        FROM
            Rptg.vwDim_Date AS date_dim
        LEFT OUTER JOIN (
                         --main

SELECT
        wq.pod_id AS epic_pod
	   ,wq.pod_name AS epic_pod_name
	   ,CAST(NULL AS VARCHAR(66)) AS hub_id
	   ,CAST(NULL AS VARCHAR(100)) AS hub_name
       ,CAST(NULL AS NUMERIC(18,0)) AS epic_department_id
       ,CAST(NULL AS VARCHAR(254)) AS epic_department_name
       ,CAST(NULL AS VARCHAR(254)) AS epic_department_name_external
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

--Select
       ,wq.day_date
       ,wq.UNSCHED_REF_WQ
	   ,wq.SCHED_ORDER_WQ
	   ,wq.REG_PAT_WQ
    FROM
	    #allwqsum wq
	         ) main
        ON  date_dim.day_date = main.day_date
        WHERE
            date_dim.day_date >= @startdate
            AND date_dim.day_date < @enddate
        ORDER BY
		    event_type ,
            date_dim.day_date;
*/
GO
