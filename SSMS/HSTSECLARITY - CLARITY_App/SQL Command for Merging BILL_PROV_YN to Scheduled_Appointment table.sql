USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--DECLARE @startdate DATETIME

SET NOCOUNT ON;
 
DECLARE @currdate DATETIME,
        --@loclastupdate DATETIME,
		@enddate DATETIME,																																	-- ******* --
		@locenddate DATETIME																																-- ******* --

SET @currdate = CAST(CAST(GETDATE() AS DATE) AS DATETIME) 
-------------------------------------------------------------------------------
 ---go back a week for safety in case of null pass
--IF @startdate IS NULL 
--   SET @startdate = DATEADD(dd,-7,@currdate)

 ---end of current fiscal year for setting default for CONTACT_TO_DATE values that are null																-- ******* --
--SET @enddate = CASE WHEN DATEPART(mm, @currdate)<7																											-- ******* --
--	                THEN CAST('07/01/'+CAST(DATEPART(yy, @currdate) AS CHAR(4)) AS DATETIME)																-- ******* --
--                    ELSE CAST('07/01/'+CAST(DATEPART(yy, @currdate)+1 AS CHAR(4)) AS DATETIME)																-- ******* --
--               END;																-- ******* --
SET @enddate = CASE WHEN DATEPART(mm, @currdate)<7																											-- ******* --
	                THEN DATEADD(DAY,45,CAST('07/01/'+CAST(DATEPART(yy, @currdate) AS CHAR(4)) AS DATETIME))															-- ******* --
                    ELSE DATEADD(DAY,45,CAST('07/01/'+CAST(DATEPART(yy, @currdate)+1 AS CHAR(4)) AS DATETIME))																-- ******* --
               END;																																			-- ******* --

 --temp set for testing
--SET @loclastupdate = @startdate
SET @locenddate = @enddate

------------------------------------------------------------------------------------------------------
SELECT @locenddate

SELECT PROV_ATTR_INFO_OT.PROV_ATTR_ID
      ,PROV_ATTR_INFO_OT.CONTACT_DATE
	  ,PROV_ATTR_INFO_OT.CONTACT_TO_DATE
      ,COALESCE(PROV_ATTR_INFO_OT.CONTACT_TO_DATE,@locenddate) AS DERIVED_CONTACT_TO_DATE
	  ,PROV_ATTR_INFO_OT.BILL_PROV_YN
FROM CLARITY.dbo.PROV_ATTR_INFO_OT AS PROV_ATTR_INFO_OT
--WHERE PROV_ATTR_INFO_OT.BILL_PROV_YN = 'Y'
--AND PROV_ATTR_INFO_OT.PROV_ATTR_ID = '11060'
--WHERE PROV_ATTR_INFO_OT.PROV_ATTR_ID = '9691'
WHERE PROV_ATTR_INFO_OT.PROV_ATTR_ID = '9648'
ORDER BY PROV_ATTR_INFO_OT.PROV_ATTR_ID
       , PROV_ATTR_INFO_OT.CONTACT_DATE

/*
SELECT PROV_ATTR_INFO_OT.PROV_ATTR_ID
      ,PROV_ATTR_INFO_OT.CONTACT_DATE
      ,COALESCE(PROV_ATTR_INFO_OT.CONTACT_TO_DATE,@locenddate) AS CONTACT_TO_DATE
FROM CLARITY.dbo.PROV_ATTR_INFO_OT AS PROV_ATTR_INFO_OT
WHERE PROV_ATTR_INFO_OT.BILL_PROV_YN = 'Y'
AND PROV_ATTR_INFO_OT.PROV_ATTR_ID = '11060'
ORDER BY PROV_ATTR_INFO_OT.PROV_ATTR_ID
       , PROV_ATTR_INFO_OT.CONTACT_DATE

DECLARE @tab_PROV_ATTR_INFO_OT TABLE
(
    PROV_ATTR_ID VARCHAR(18)
   ,CONTACT_DATE DATETIME
   --,CONTACT_TO_DATE DATETIME
   --,day_date DATETIME
);
INSERT INTO @tab_PROV_ATTR_INFO_OT
SELECT DISTINCT
       PROV_ATTR_INFO_OT.PROV_ATTR_ID
   --   ,PROV_ATTR_INFO_OT.CONTACT_DATE
   --   ,COALESCE(PROV_ATTR_INFO_OT.CONTACT_TO_DATE,@locenddate) AS CONTACT_TO_DATE
	  --,vwDim_Date.day_date
	  ,vwDim_Date.day_date AS CONTACT_DATE
FROM CLARITY.dbo.PROV_ATTR_INFO_OT AS PROV_ATTR_INFO_OT
CROSS JOIN
(
SELECT CAST(day_date AS DATETIME) AS day_date
FROM Rptg.vwDim_Date
) vwDim_Date
WHERE PROV_ATTR_INFO_OT.BILL_PROV_YN = 'Y'
--AND PROV_ATTR_INFO_OT.PROV_ATTR_ID = '1106`	0'
AND (vwDim_Date.day_date BETWEEN PROV_ATTR_INFO_OT.CONTACT_DATE AND COALESCE(PROV_ATTR_INFO_OT.CONTACT_TO_DATE,@locenddate))

SELECT *
FROM @tab_PROV_ATTR_INFO_OT
ORDER BY PROV_ATTR_ID
       , CONTACT_DATE
--ORDER BY PROV_ATTR_ID
--       , day_date
*/
/*
CROSS JOIN Rptg.vwDim_Date AS vwDim_Date
ON 
WHERE PROV_ATTR_INFO_OT.BILL_PROV_YN = 'Y'																							-- ******* --
						AND F_SCHED_APPT.CONTACT_DATE BETWEEN PROV_ATTR_INFO_OT.CONTACT_DATE AND COALESCE(PROV_ATTR_INFO_OT.CONTACT_TO_DATE,@locenddate)	-- ******* --

----filters are handled above in the join to CR_STAT_ALTER
---          WHERE     1 = 1
---                    AND F_SCHED_APPT.APPT_DTTM >= @locstartdate
---                   AND F_SCHED_APPT.APPT_DTTM <  @locenddate
*/
GO


