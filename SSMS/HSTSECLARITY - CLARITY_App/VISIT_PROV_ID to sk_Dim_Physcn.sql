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
--SET @startdate = '7/1/2018 00:00 AM'
--SET @enddate = '6/30/2019 11:59 PM'
SET @startdate = '7/1/2017 00:00 AM'
SET @enddate = '6/30/2019 11:59 PM'

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

if OBJECT_ID('tempdb..#HAR') is not NULL
DROP TABLE #HAR

if OBJECT_ID('tempdb..#HAR2') is not NULL
DROP TABLE #HAR2

if OBJECT_ID('tempdb..#HAR3') is not NULL
DROP TABLE #HAR3

SELECT DISTINCT        enc.VISIT_PROV_ID,
                       clser.PROV_NAME,
                       ser.sk_Dim_Physcn AS ser_sk_Dim_Physcn,
					   cwlk.sk_Dim_Physcn AS cwlk_sk_Dim_Physcn

                        FROM           CLARITY.dbo.PAT_ENC       AS enc					--12/17/2018 -Tom B Replaced V_SCHED_APPT with PAT_ENC
						    INNER JOIN CLARITY.dbo.CLARITY_SER clser
							ON enc.VISIT_PROV_ID = clser.PROV_ID
						    LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Clrt_SERsrc ser
							ON ser.PROV_ID = enc.VISIT_PROV_ID
							LEFT OUTER JOIN (SELECT DISTINCT PROV_ID, sk_Dim_Physcn FROM CLARITY_App.Rptg.vwRef_Crosswalk_HSEntity_Prov) cwlk
							ON cwlk.PROV_ID = enc.VISIT_PROV_ID
                        WHERE          1 = 1
                        AND            enc.APPT_TIME >= @locstartdate
                        AND            enc.APPT_TIME < @locenddate
--ORDER BY enc.VISIT_PROV_ID
ORDER BY ser_sk_Dim_Physcn DESC
/*
				LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = doc.sk_Dim_Physcn
	            LEFT OUTER JOIN
	            (
					SELECT DISTINCT
					    wd.sk_Dim_Physcn,
						wd.PROV_ID,
             			wd.Clrt_Financial_Division,
			    		wd.Clrt_Financial_Division_Name,
						wd.Clrt_Financial_SubDivision, 
					    wd.Clrt_Financial_SubDivision_Name,
					    wd.wd_Dept_Code,
					    wd.SOM_Group_ID,
					    wd.SOM_Group,
						wd.SOM_department_id,
					    wd.SOM_department,
						wd.SOM_division_id,
						wd.SOM_division_name,
						wd.SOM_division_5
					FROM
					(
					    SELECT
						    cwlk.sk_Dim_Physcn,
							cwlk.PROV_ID,
             			    cwlk.Clrt_Financial_Division,
			    		    cwlk.Clrt_Financial_Division_Name,
						    cwlk.Clrt_Financial_SubDivision, 
							cwlk.Clrt_Financial_SubDivision_Name,
							cwlk.wd_Dept_Code,
							som.SOM_Group_ID,
							som.SOM_Group,
							som.SOM_department_id,
							som.SOM_department,
							som.SOM_division_id,
							som.SOM_division_name,
							som.SOM_division_5,
							ROW_NUMBER() OVER (PARTITION BY cwlk.sk_Dim_Physcn ORDER BY som.som_group_id ASC) AS [SOMSeq]
						FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
						    LEFT OUTER JOIN (SELECT DISTINCT
							                     SOM_Group_ID,
												 SOM_Group,
												 SOM_department_id,
												 SOM_department,
												 SOM_division_id,
												 SOM_division_name,
												 SOM_division_5
						                     FROM Rptg.vwRef_SOM_Hierarchy
						                    ) AS som
						        ON cwlk.wd_Dept_Code = som.SOM_division_5
					    WHERE cwlk.wd_Is_Primary_Job = 1
                              AND cwlk.wd_Is_Position_Active = 1
					) AS wd
					WHERE wd.SOMSeq = 1
				) AS uwd
				    ON uwd.PROV_ID = appt.VISIT_PROV_ID

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

--ORDER BY            event_date;

SELECT *
--SELECT DISTINCT
--	event_type
--   ,IDENTITY_ID
--   ,sk_Dim_Physcn
--   ,dim_Physcn_PROV_ID
--   ,provider_id
--   ,provider_Name
--   ,provider_Type
--   ,CLINICIAN_TITLE
--   ,w_financial_division_id
--   ,w_financial_division_name
--   ,w_som_department_id
FROM #HAR
--WHERE provider_id IN ('61341','91274','93744')
--ORDER BY PAT_ENC_CSN_ID
--ORDER BY provider_id, PAT_ENC_CSN_ID
--ORDER BY dim_Physcn_PROV_ID
--       , IDENTITY_ID
--       , PAT_ENC_CSN_ID
--ORDER BY dim_Physcn_PROV_ID
--       , IDENTITY_ID
--ORDER BY w_financial_division_id
--       , provider_id
ORDER BY sk_Dim_Physcn, PAT_ENC_CSN_ID

SELECT har.PAT_ENC_CSN_ID
      ,har.rev_location
	  --,wd.dim_Physcn_PROV_ID
	  --,wd.Clrt_Financial_Division
	  --,wd.Clrt_Financial_Division_Name
	  --,wd.Clrt_Financial_SubDivision
	  --,wd.Clrt_Financial_SubDivision_Name
	  --,wd.SOM_DEPT_ID
	  --,wd.wd_Dept_Code
	  --,wd.wd_Department_Name
	  --,wd.wd_Is_Primary_Job
	  ,har.sk_Dim_Physcn
	  ,har.financial_division_id
	  ,har.financial_division_name
	  ,har.financial_sub_division_id
	  ,har.financial_sub_division_name
	  ,har.som_department_id
	  ,har.som_division_id
	  ,har.som_division_name
--SELECT *
     --, ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY w_som_division_id) AS [EncSeq]
     , ROW_NUMBER() OVER (PARTITION BY har.PAT_ENC_CSN_ID ORDER BY har.rev_location) AS [EncSeq]
INTO #HAR2
FROM #HAR har
	--LEFT OUTER JOIN (SELECT DISTINCT
	--                    dim_Physcn_PROV_ID
	--				   --,Clrt_Financial_Division
	--				   --,Clrt_Financial_Division_Name
	--				   --,Clrt_Financial_SubDivision
	--				   --,Clrt_Financial_SubDivision_Name
	--				   ,SOM_DEPT_ID
	--				   ,wd_Dept_Code
	--				   ,wd_Department_Name
	--				   ,wd_Is_Primary_Job
	--				 FROM
	--				   Rptg.vwRef_Crosswalk_HSEntity_Prov
	--				 WHERE
	--				   ISNULL(wd_Is_Primary_Job,1) = 1
	--				   AND Som_DEPT_ID IS NOT NULL
	--				   --Som_DEPT_ID IS NOT NULL
	--				) AS wd ON wd.dim_Physcn_PROV_ID = har.IDENTITY_ID

SELECT *
FROM #HAR2
--ORDER BY PAT_ENC_CSN_ID
--        --, w_som_division_id
--        , w_rev_name
--		, EncSeq
--ORDER BY dim_Physcn_PROV_ID
--       , PAT_ENC_CSN_ID
--       --, w_som_division_id
--       , w_rev_name
--	   , EncSeq
ORDER BY sk_Dim_Physcn
       , PAT_ENC_CSN_ID
       --, w_som_division_id
       , rev_location
	   , EncSeq

SELECT DISTINCT sk_Dim_Physcn, PAT_ENC_CSN_ID
INTO #HAR3
FROM #HAR2
WHERE [EncSeq] > 1

SELECT *
FROM #HAR3
ORDER BY sk_Dim_Physcn, PAT_ENC_CSN_ID
*/
GO


