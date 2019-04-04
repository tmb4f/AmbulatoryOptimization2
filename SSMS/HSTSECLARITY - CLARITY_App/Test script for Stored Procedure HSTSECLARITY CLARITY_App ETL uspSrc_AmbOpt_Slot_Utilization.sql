USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate SMALLDATETIME = NULL

--SET @startdate = '7/20/2018 00:00 AM'
--SET @enddate = '7/20/2018 11:59 PM'
--SET @startdate = '6/5/2018 00:00 AM'
--SET @enddate = '6/5/2018 11:59 PM'
--SET @startdate = '6/4/2018 00:00 AM'
--SET @enddate = '6/6/2018 11:59 PM'
SET @startdate = '2/1/2019 00:00 AM'
--SET @startdate = '7/1/2018 00:00 AM'
SET @enddate = '2/28/2019 11:59 PM'

DECLARE @Department TABLE (DepartmentId NUMERIC(18,0))

INSERT INTO @Department
(
    DepartmentId
)
VALUES
-- (10210006)
--,(10210040)
--,(10210041)
--,(10211006)
--,(10214011)
--,(10214014)
--,(10217003)
--,(10239017)
--,(10239018)
--,(10239019)
--,(10239020)
--,(10241001)
--,(10242007)
--,(10242049)
--,(10243003)
--,(10244004)
--,(10348014)
--,(10354006)
--,(10354013)
--,(10354014)
--,(10354015)
--,(10354016)
--,(10354017)
--,(10354024)
--,(10354034)
--,(10354042)
--,(10354044)
--,(10354052)
--,(10354055)
 --(10214011)
 --(10210006)
 (10280004) -- AUBL PEDIATRICS
 --(10341002) -- CVPE UVA RHEU INF PNTP
 --(10228008) -- NRDG MAMMOGRAPHY
 --(10381003) -- UVEC RAD CT
 --(10354032) -- UVBB PHYSICAL THER FL4
;

DECLARE @Provider TABLE (ProviderId VARCHAR(18))

INSERT INTO @Provider
(
    ProviderId
)
VALUES
 ('28813') -- FISHER, JOSEPH D
 --('1300563') -- ARTH INF
 --('41806') -- NORTHRIDGE DEXA
 --('1301100') -- CT6
 --('82262') -- CT APPOINTMENT ERC
 --('40758') -- PAYNE, PATRICIA
;

-- =====================================================================================
-- Create procedure uspSrc_AmbOpt_Slot_Utilization
-- =====================================================================================

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Slot_Utilization]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Slot_Utilization
--WHO : Tom Burgan
--WHEN: 5/14/18
--WHY : Report slot utilization percentages by department and provider.
--		Percent Booked Overall = Openings Booked / Regular Openings Available
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:
--              CLARITY.dbo.V_AVAILABILITY
--              CLARITY.dbo.CLARITY_SER
--              Rptg.vwRef_MDM_Location_Master_EpicSvc
--              Rptg.vwPhyscn_Current_Svc_Ln
--              Rptg.Ref_Service_Line
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Slot_Utilization]
-- 
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--       05/16/2018 - TMB - create stored procedure
--       08/22/2018 - TMB - edit stored procedure to include both slot and appointment level utilization counts
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
DECLARE @slotstartdate DATETIME,
        @slotenddate DATETIME
SET @slotstartdate = CAST(@startdate AS DATETIME)
SET @slotenddate   = CAST(@enddate AS DATETIME)
-------------------------------------------------------------------------------

SELECT @slotstartdate, @slotenddate

if OBJECT_ID('tempdb..#datetable') is not NULL
DROP TABLE #datetable

if OBJECT_ID('tempdb..#utilsum') is not NULL
DROP TABLE #utilsum

if OBJECT_ID('tempdb..#util') is not NULL
DROP TABLE #util

if OBJECT_ID('tempdb..#utildatetable') is not NULL
DROP TABLE #utildatetable

if OBJECT_ID('tempdb..#RptgTable') is not NULL
DROP TABLE #RptgTable

SELECT date_dim.day_date
      ,date_dim.fmonth_num
      ,date_dim.Fyear_num
      ,date_dim.FYear_name
INTO #datetable
FROM Rptg.vwDim_Date AS date_dim
WHERE date_dim.day_date >= @slotstartdate
AND date_dim.day_date < @slotenddate

  -- Create index for temp table #datetable

  CREATE UNIQUE CLUSTERED INDEX IX_datetable ON #datetable ([day_date])

SELECT
       util.DEPARTMENT_ID
	 , util.PROV_ID
	 , CAST(util.SLOT_BEGIN_TIME AS DATE) AS SLOT_BEGIN_DATE
	 , SUM(util.[Regular Openings]) AS [Regular Openings]
	 , SUM(util.[Overbook Openings]) AS [Overbook Openings]
	 , SUM(util.[Openings Booked]) AS [Openings Booked]
	 , SUM(util.[Regular Openings Available]) AS [Regular Openings Available]
	 , SUM(util.[Regular Openings Unavailable]) AS [Regular Openings Unavailable]
	 , SUM(util.[Overbook Openings Available]) AS [Overbook Openings Available]
	 , SUM(util.[Overbook Openings Unavailable]) AS [Overbook Openings Unavailable]
	 , SUM(util.[Regular Openings Booked]) AS [Regular Openings Booked]
	 , SUM(util.[Overbook Openings Booked]) AS [Overbook Openings Booked]
	 , SUM(util.[Regular Outside Template Booked]) AS [Regular Outside Template Booked]
	 , SUM(util.[Overbook Outside Template Booked]) AS [Overbook Outside Template Booked]
	 , SUM(util.[Regular Openings Available Booked]) AS [Regular Openings Available Booked]
	 , SUM(util.[Overbook Openings Available Booked]) AS [Overbook Openings Available Booked]
	 , SUM(util.[Regular Openings Unavailable Booked]) AS [Regular Openings Unavailable Booked]
	 , SUM(util.[Overbook Openings Unavailable Booked]) AS [Overbook Openings Unavailable Booked]
     , SUM(util.[Regular Outside Template Available Booked]) AS [Regular Outside Template Available Booked]
	 , SUM(util.[Overbook Outside Template Available Booked]) AS [Overbook Outside Template Available Booked]
     , SUM(util.[Regular Outside Template Unavailable Booked]) AS [Regular Outside Template Unavailable Booked]
	 , SUM(util.[Overbook Outside Template Unavailable Booked]) AS [Overbook Outside Template Unavailable Booked]
INTO #utilsum
FROM
(
SELECT slot.DEPARTMENT_ID
	  ,slot.PROV_ID
	  ,slot.SLOT_BEGIN_TIME
	  ,slot.[Regular Openings]
	  ,slot.[Overbook Openings]
	  ,slot.[Openings Booked]
	  ,slot.[Regular Openings Available]
	  ,slot.[Regular Openings Unavailable]
	  ,slot.[Overbook Openings Available]
	  ,slot.[Overbook Openings Unavailable]
	  ,COALESCE(appt.[Regular Openings Booked],0) AS [Regular Openings Booked]
	  ,COALESCE(appt.[Overbook Openings Booked],0) AS [Overbook Openings Booked]
	  ,COALESCE(appt.[Regular Outside Template Booked],0) AS [Regular Outside Template Booked]
	  ,COALESCE(appt.[Overbook Outside Template Booked],0) AS [Overbook Outside Template Booked]
	  ,COALESCE(appt.[Regular Openings Available Booked],0) AS [Regular Openings Available Booked]
	  ,COALESCE(appt.[Overbook Openings Available Booked],0) AS [Overbook Openings Available Booked]
	  ,COALESCE(appt.[Regular Openings Unavailable Booked],0) AS [Regular Openings Unavailable Booked]
	  ,COALESCE(appt.[Overbook Openings Unavailable Booked],0) AS [Overbook Openings Unavailable Booked]
      ,COALESCE(appt.[Regular Outside Template Available Booked],0) AS [Regular Outside Template Available Booked]
	  ,COALESCE(appt.[Overbook Outside Template Available Booked],0) AS [Overbook Outside Template Available Booked]
      ,COALESCE(appt.[Regular Outside Template Unavailable Booked],0) AS [Regular Outside Template Unavailable Booked]
	  ,COALESCE(appt.[Overbook Outside Template Unavailable Booked],0) AS [Overbook Outside Template Unavailable Booked]
FROM
(
	SELECT
	       [AVAILABILITY].DEPARTMENT_ID
	      ,[AVAILABILITY].PROV_ID
	      ,[AVAILABILITY].SLOT_BEGIN_TIME
	      ,[AVAILABILITY].ORG_REG_OPENINGS AS [Regular Openings]
		  ,[AVAILABILITY].ORG_OVBK_OPENINGS AS [Overbook Openings]
	      ,[AVAILABILITY].NUM_APTS_SCHEDULED AS [Openings Booked]
	      ,CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL THEN [AVAILABILITY].ORG_REG_OPENINGS ELSE 0 END AS [Regular Openings Available]
	      ,CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL THEN [AVAILABILITY].ORG_OVBK_OPENINGS ELSE 0 END AS [Overbook Openings Available]
	      ,CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL THEN [AVAILABILITY].ORG_REG_OPENINGS ELSE 0 END AS [Regular Openings Unavailable]
	      ,CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL THEN [AVAILABILITY].ORG_OVBK_OPENINGS ELSE 0 END AS [Overbook Openings Unavailable]

    FROM  CLARITY.dbo.V_AVAILABILITY [AVAILABILITY]
		
    WHERE  [AVAILABILITY].APPT_NUMBER = 0
    AND CAST(CAST([AVAILABILITY].SLOT_BEGIN_TIME AS DATE) AS DATETIME) >= @slotstartdate
    AND CAST(CAST([AVAILABILITY].SLOT_BEGIN_TIME AS DATE) AS DATETIME) <  @slotenddate
    --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = AVAILABILITY.DEPARTMENT_ID)
    --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = AVAILABILITY.PROV_ID)

    ORDER BY [AVAILABILITY].DEPARTMENT_ID
           , [AVAILABILITY].PROV_ID
	       , [AVAILABILITY].SLOT_BEGIN_TIME
		     OFFSET 0 ROWS

) slot
LEFT OUTER JOIN
(
    SELECT
	       [AVAILABILITY].DEPARTMENT_ID
		  ,[AVAILABILITY].PROV_ID
		  ,[AVAILABILITY].SLOT_BEGIN_TIME
		  ,SUM(CASE WHEN [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Regular Openings Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Overbook Openings Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Regular Outside Template Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Overbook Outside Template Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Regular Openings Available Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Overbook Openings Available Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Regular Openings Unavailable Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Overbook Openings Unavailable Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Regular Outside Template Available Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Overbook Outside Template Available Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Regular Outside Template Unavailable Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Overbook Outside Template Unavailable Booked]

    FROM  CLARITY.dbo.V_AVAILABILITY [AVAILABILITY]
		
    WHERE  [AVAILABILITY].APPT_NUMBER > 0
    AND CAST(CAST([AVAILABILITY].SLOT_BEGIN_TIME AS DATE) AS DATETIME) >= @slotstartdate
    AND CAST(CAST([AVAILABILITY].SLOT_BEGIN_TIME AS DATE) AS DATETIME) <  @slotenddate
    --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = [AVAILABILITY].DEPARTMENT_ID)
    --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = [AVAILABILITY].PROV_ID)

    GROUP BY [AVAILABILITY].DEPARTMENT_ID
           , [AVAILABILITY].PROV_ID
	       , [AVAILABILITY].SLOT_BEGIN_TIME

    ORDER BY [AVAILABILITY].DEPARTMENT_ID
           , [AVAILABILITY].PROV_ID
	       , [AVAILABILITY].SLOT_BEGIN_TIME
		     OFFSET 0 ROWS

) appt
ON ((appt.DEPARTMENT_ID = slot.DEPARTMENT_ID)
    AND (appt.PROV_ID = slot.PROV_ID)
	AND (appt.SLOT_BEGIN_TIME = slot.SLOT_BEGIN_TIME))
) util

GROUP BY util.DEPARTMENT_ID
       , util.PROV_ID
	   , CAST(util.SLOT_BEGIN_TIME AS DATE)

ORDER BY util.DEPARTMENT_ID
       , util.PROV_ID
	   , CAST(util.SLOT_BEGIN_TIME AS DATE)

  -- Create index for temp table #utilsum

--CREATE UNIQUE CLUSTERED INDEX IX_utilsum ON #utilsum ([DEPARTMENT_ID], [PROV_ID], [SLOT_BEGIN_DATE])
CREATE UNIQUE CLUSTERED INDEX IX_utilsum ON #utilsum ([SLOT_BEGIN_DATE], [DEPARTMENT_ID], [PROV_ID])

SELECT DISTINCT
       DEPARTMENT_ID
	 , PROV_ID
INTO #util
FROM #utilsum

  -- Create index for temp table #util

  CREATE NONCLUSTERED INDEX IX_util ON #util ([DEPARTMENT_ID], [PROV_ID])

SELECT util.DEPARTMENT_ID
     , util.PROV_ID
     , dt.day_date
	 , dt.fmonth_num
	 , dt.Fyear_num
	 , dt.FYear_name
INTO #utildatetable
FROM #util util
CROSS JOIN #datetable dt

  -- Create index for temp table #utildatetable

  CREATE NONCLUSTERED INDEX IX_utildatetable ON #utildatetable ([day_date], [DEPARTMENT_ID], [PROV_ID])

---------------------------------------------------------------------------------------
--08/23/2018 BDD - insert to stage added. Assumes prior truncation handled in SSIS package
--INSERT Stage.AmbOpt_Dash_Slot_Utilization
--           (event_type
--           ,event_count
--           ,event_date
--           ,fmonth_num
--           ,Fyear_num
--           ,FYear_name
--           ,report_period
--           ,report_date
--           ,event_category
--           ,pod_id
--           ,pod_name
--           ,hub_id
--           ,hub_name
--           ,epic_department_id
--           ,epic_department_name
--           ,epic_department_name_external
--           ,peds
--           ,transplant
--           ,sk_Dim_Pt
--           ,sk_Fact_Pt_Acct
--           ,sk_Fact_Pt_Enc_Clrt
--           ,person_birth_date
--           ,person_gender
--           ,person_id
--           ,person_name
--           ,practice_group_id
--           ,practice_group_name
--           ,provider_id
--           ,provider_name
--           ,service_line_id
--           ,service_line
--           ,prov_service_line_id
--           ,prov_service_line
--           ,sub_service_line_id
--           ,sub_service_line
--           ,opnl_service_id
--           ,opnl_service_name
--           ,corp_service_line_id
--           ,corp_service_line
--           ,hs_area_id
--           ,hs_area_name
--           ,prov_hs_area_id
--           ,prov_hs_area_name
--           ,Regular_Openings
--           ,Overbook_Openings
--           ,Openings_Booked
--           ,Regular_Openings_Available
--           ,Regular_Openings_Unavailable
--           ,Overbook_Openings_Available
--           ,Overbook_Openings_Unavailable
--           ,Regular_Openings_Booked
--           ,Overbook_Openings_Booked
--           ,Regular_Outside_Template_Booked
--           ,Overbook_Outside_Template_Booked
--           ,Regular_Openings_Available_Booked
--           ,Overbook_Openings_Available_Booked
--           ,Regular_Openings_Unavailable_Booked
--           ,Overbook_Openings_Unavailable_Booked
--           ,Regular_Outside_Template_Available_Booked
--           ,Overbook_Outside_Template_Available_Booked
--           ,Regular_Outside_Template_Unavailable_Booked
--           ,Overbook_Outside_Template_Unavailable_Booked
--           ,STAFF_RESOURCE_C
--           ,STAFF_RESOURCE
--           ,PROVIDER_TYPE_C
--           ,PROV_TYPE
--		   )
SELECT 
       CAST('Slot Utilization' AS VARCHAR(50)) AS event_type
      ,CASE WHEN util.SLOT_BEGIN_DATE IS NOT NULL THEN 1
            ELSE 0
       END AS event_count
      ,date_dim.day_date AS event_date
      ,date_dim.fmonth_num
      ,date_dim.Fyear_num
      ,date_dim.FYear_name
      ,CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
      ,CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date
	  ,CAST(NULL AS VARCHAR(150)) AS event_category
	  ,mdm.POD_ID AS pod_id
      ,mdm.PFA_POD AS pod_name
	  ,mdm.HUB_ID AS hub_id
	  ,mdm.HUB AS hub_name
      ,date_dim.DEPARTMENT_ID AS epic_department_id
      ,mdm.epic_department_name
      ,mdm.epic_department_name_external
      ,util.peds
      ,util.transplant
      ,util.sk_Dim_Pt
      ,util.sk_Fact_Pt_Acct
      ,util.sk_Fact_Pt_Enc_Clrt
      ,util.person_birth_date
      ,util.person_gender
      ,util.person_id
      ,util.person_name
      ,util.practice_group_id
      ,util.practice_group_name
	  ,date_dim.PROV_ID AS provider_id
      ,ser.PROV_NAME AS provider_name
      ,mdm.service_line_id
      ,mdm.service_line
	  ,physsvc.Service_Line_ID AS prov_service_line_id
	  ,physsvc.Service_Line AS prov_service_line
      ,mdm.sub_service_line_id
      ,mdm.sub_service_line
      ,mdm.opnl_service_id
      ,mdm.opnl_service_name
      ,mdm.corp_service_line_id
      ,mdm.corp_service_line
      ,mdm.hs_area_id
      ,mdm.hs_area_name
	  ,physsvc.hs_area_id AS prov_hs_area_id
	  ,physsvc.hs_area_name AS prov_hs_area_name
	  ,util.[Regular Openings]
	  ,util.[Overbook Openings]
	  ,util.[Openings Booked]
	  ,util.[Regular Openings Available]
	  ,util.[Regular Openings Unavailable]
	  ,util.[Overbook Openings Available]
	  ,util.[Overbook Openings Unavailable]
	  ,util.[Regular Openings Booked]
	  ,util.[Overbook Openings Booked]
	  ,util.[Regular Outside Template Booked]
	  ,util.[Overbook Outside Template Booked]
	  ,util.[Regular Openings Available Booked]
	  ,util.[Overbook Openings Available Booked]
	  ,util.[Regular Openings Unavailable Booked]
	  ,util.[Overbook Openings Unavailable Booked]
      ,util.[Regular Outside Template Available Booked]
	  ,util.[Overbook Outside Template Available Booked]
      ,util.[Regular Outside Template Unavailable Booked]
	  ,util.[Overbook Outside Template Unavailable Booked]
	  ,ser.STAFF_RESOURCE_C
	  ,ser.STAFF_RESOURCE
	  ,ser.PROVIDER_TYPE_C
	  ,ser.PROV_TYPE
INTO #RptgTable
FROM
    #utildatetable AS date_dim
LEFT OUTER JOIN
(
    SELECT DISTINCT
           main.DEPARTMENT_ID AS epic_department_id
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
          ,main.PROV_ID AS provider_id

--Select
          ,main.SLOT_BEGIN_DATE
	      ,main.[Regular Openings]
	      ,main.[Overbook Openings]
	      ,main.[Openings Booked]
	      ,main.[Regular Openings Available]
	      ,main.[Regular Openings Unavailable]
	      ,main.[Overbook Openings Available]
	      ,main.[Overbook Openings Unavailable]
	      ,main.[Regular Openings Booked]
	      ,main.[Overbook Openings Booked]
	      ,main.[Regular Outside Template Booked]
	      ,main.[Overbook Outside Template Booked]
	      ,main.[Regular Openings Available Booked]
	      ,main.[Overbook Openings Available Booked]
	      ,main.[Regular Openings Unavailable Booked]
	      ,main.[Overbook Openings Unavailable Booked]
          ,main.[Regular Outside Template Available Booked]
	      ,main.[Overbook Outside Template Available Booked]
          ,main.[Regular Outside Template Unavailable Booked]
	      ,main.[Overbook Outside Template Unavailable Booked]

    FROM
        #utilsum AS main -- main

) util
ON  ((date_dim.day_date = CAST(util.SLOT_BEGIN_DATE AS SMALLDATETIME))
     AND ((date_dim.DEPARTMENT_ID = util.epic_department_id)
	      AND (date_dim.PROV_ID = util.provider_id)))

LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
ON excl.DEPARTMENT_ID = date_dim.DEPARTMENT_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS ser
ON ser.PROV_ID = date_dim.PROV_ID
LEFT OUTER JOIN Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
ON  date_dim.DEPARTMENT_ID = mdm.epic_department_id
LEFT OUTER JOIN Rptg.vwPhyscn_Current_Svc_Ln AS mdmphyscn
ON mdmphyscn.PROV_ID = date_dim.PROV_ID
LEFT OUTER JOIN Rptg.Ref_Service_Line physsvc
ON physsvc.Physician_Roster_Name = CASE
                                     WHEN mdmphyscn.Service_Line IS NOT NULL THEN mdmphyscn.Service_Line
		                             ELSE 'No Value Specified'
	                               END

WHERE
      ((date_dim.day_date >= @slotstartdate) AND (date_dim.day_date < @slotenddate))
      AND excl.DEPARTMENT_ID IS NULL
      --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = date_dim.DEPARTMENT_ID)
      --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = date_dim.PROV_ID)

SELECT *
FROM #RptgTable
--ORDER BY date_dim.DEPARTMENT_ID
--		,date_dim.PROV_ID
--		,date_dim.day_date;
ORDER BY event_date
        ,event_count DESC
		,epic_department_id
		,provider_id;

SELECT DISTINCT
    STAFF_RESOURCE
   ,PROV_TYPE
FROM #RptgTable
ORDER BY STAFF_RESOURCE
        ,PROV_TYPE

GO


