USE DS_HSDM_App

DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate SMALLDATETIME = NULL

SET @startdate = '7/1/2018 00:00 AM'
SET @enddate = '6/30/2019 11:59 PM'

SET NOCOUNT ON
 
	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

 /* Bump Rate (All Providers)*/
  --/*
SELECT CASE WHEN appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  ,*
  FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  WHERE
  ((event_count = 1)
  AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  ORDER BY CASE WHEN appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END DESC
         , APPT_STATUS_FLAG
         , event_date
  --*/

  /* Bump Rate (Billing Providers) */
  /*
SELECT CASE WHEN (BILL_PROV_YN = 1 AND appt_event_Canceled = 0) OR (BILL_PROV_YN = 1 AND appt_event_Canceled_Late = 1) OR (BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END AS [Appointment]
	  ,CASE WHEN BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS [Bump]
	  ,*
  FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  WHERE
  ((event_count = 1)
  AND ((BILL_PROV_YN = 1 AND appt_event_Canceled = 0) OR (BILL_PROV_YN = 1 AND appt_event_Canceled_Late = 1) OR (BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  ORDER BY CASE WHEN BILL_PROV_YN = 1 AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END DESC
         , APPT_STATUS_FLAG
         , event_date
  */

GO


