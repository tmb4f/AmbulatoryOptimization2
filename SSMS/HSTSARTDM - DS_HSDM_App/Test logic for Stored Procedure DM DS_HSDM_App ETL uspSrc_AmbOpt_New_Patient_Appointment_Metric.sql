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
--SET @startdate = '2/1/2019 00:00 AM'
--SET @enddate = '3/4/2019 11:59 PM'
--SET @startdate = '7/1/2017 00:00 AM'
--SET @startdate = '7/1/2018 00:00 AM'
--SET @enddate = '6/30/2019 11:59 PM'
SET @startdate = '7/1/2019 00:00 AM'
SET @enddate = '7/31/2019 11:59 PM'

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

IF OBJECT_ID('tempdb..#metric ') IS NOT NULL
DROP TABLE #metric

--SELECT APPT_SERIAL_NUM
--     , MAX(CASE WHEN new.APPT_STATUS_C IN (1,2,6) THEN new.APPT_STATUS_C ELSE NULL END) AS APPT_STATUS_C_LAST -- Scheduled, Completed, Arrived
--     , MAX(CASE WHEN new.APPT_STATUS_C IN (2,6) THEN new.APPT_STATUS_FLAG ELSE NULL END) AS APPT_STATUS_FLAG_LAST
--     , MAX(CASE WHEN new.APPT_STATUS_C IN (2,6) THEN new.Seq ELSE NULL END) AS Seq_LAST
--     , MAX(CASE WHEN new.APPT_STATUS_C IN (2,6) THEN new.Appointment_Request_Date ELSE NULL END) AS Appointment_Request_Date_LAST
--     , MAX(CASE WHEN new.APPT_STATUS_C IN (2,6) THEN new.APPT_DT ELSE NULL END) AS APPT_DT_LAST

--INTO #metric

--FROM
--(
    SELECT  appts.PAT_ENC_CSN_ID,
	        appts.APPT_SERIAL_NUM,
	        appts.Seq,
	        appts.APPT_SERIAL_NUM_COUNT,
			appts.APPT_STATUS_FLAG,
            appts.APPT_STATUS_C,
			appts.CANCEL_INITIATOR,
            appts.APPT_DT,
			appts.APPT_MADE_DTTM,
			appts.Appointment_Request_Date,
			appts.NEW_TO_SPEC,
			CASE
				WHEN (appts.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd, appts.Appointment_Request_Date, appts.APPT_DT)
				ELSE CAST(NULL AS INT)
			END AS Appointment_Lag_Days,
			CASE
				WHEN appts.APPT_SERIAL_NUM_COUNT = 1 AND appts.Seq = 1 THEN Appointment_Request_Date
				ELSE LAG(appts.Appointment_Request_Date,appts.Seq-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
			END AS Original_Appointment_Request_Date,
			CASE
				WHEN appts.APPT_SERIAL_NUM_COUNT = 1 AND appts.Seq = 1 THEN appts.CANCEL_INITIATOR
				ELSE LAG(appts.CANCEL_INITIATOR,appts.Seq-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
			END AS Original_CANCEL_INITIATOR,
			CASE
				WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_STATUS_C,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				ELSE NULL
			END AS Last_APPT_STATUS_C,
			CASE
				WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_STATUS_FLAG,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				ELSE NULL
			END AS Last_APPT_STATUS_FLAG,
			CASE
				WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.Seq,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				ELSE NULL
			END AS Last_Seq,
			CASE
				WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.Appointment_Request_Date,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				ELSE NULL
			END AS Last_Appointment_Request_Date,
			CASE
				WHEN appts.APPT_SERIAL_NUM_COUNT > 1 AND appts.Seq = 1 THEN LEAD(appts.APPT_DT,appts.APPT_SERIAL_NUM_COUNT-1,0) OVER (PARTITION BY appts.APPT_SERIAL_NUM ORDER BY appts.Seq)
				ELSE NULL
			END AS Last_APPT_DT,
            (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE ddte.weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND ddte.day_date >= appts.Appointment_Request_Date AND ddte.day_date < appts.APPT_DT) Appointment_Lag_Business_Days

    INTO #metric

	FROM
	(
            SELECT main.PAT_ENC_CSN_ID,
			       main.APPT_SERIAL_NUM,
	               ROW_NUMBER() OVER (PARTITION BY main.APPT_SERIAL_NUM ORDER BY main.APPT_MADE_DTTM) AS Seq,
	               COUNT(*) OVER (PARTITION BY main.APPT_SERIAL_NUM) APPT_SERIAL_NUM_COUNT,
			       main.APPT_STATUS_FLAG,
                   main.APPT_STATUS_C,
				   main.CANCEL_INITIATOR,
                   main.APPT_DT,
                   --main.VIS_NEW_TO_SPEC_YN,
       --            main.APPT_MADE_DATE,
       --            main.ENTRY_DATE,
       --            main.APPT_DTTM,
				   --main.CHANGE_DATE,
				   main.APPT_MADE_DTTM,
				   CASE
					   WHEN main.ENTRY_DATE IS NULL THEN
						   main.APPT_MADE_DATE
					   WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
						   main.APPT_MADE_DATE
					   WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
						   main.ENTRY_DATE
					   ELSE
						   main.CHANGE_DATE
				   END AS Appointment_Request_Date,
				   aggr.NEW_TO_SPEC

            FROM Stage.Scheduled_Appointment AS main
			LEFT OUTER JOIN
			(
				SELECT APPT_SERIAL_NUM,
				MAX(CASE WHEN VIS_NEW_TO_SPEC_YN = 'Y' THEN 1 ELSE 0 END) AS NEW_TO_SPEC--,
				--COUNT(*) AS APPT_SERIAL_NUM_COUNT
				FROM DS_HSDM_App.Stage.Scheduled_Appointment
				GROUP BY APPT_SERIAL_NUM
			) aggr
				ON aggr.APPT_SERIAL_NUM = main.APPT_SERIAL_NUM
			--WHERE (main.APPT_MADE_DATE BETWEEN @locstartdate and @locenddate)
			--OR (main.ENTRY_DATE BETWEEN @locstartdate and @locenddate)
			--OR (main.CHANGE_DATE BETWEEN @locstartdate and @locenddate)
	) appts

	WHERE appts.NEW_TO_SPEC = 1
--) new

--SELECT *
--FROM #metric
--ORDER BY APPT_SERIAL_NUM_COUNT
--        ,APPT_SERIAL_NUM
--		,Seq
--/*
	SELECT APPT_SERIAL_NUM,
		   PAT_ENC_CSN_ID,
	       Seq,
           APPT_SERIAL_NUM_COUNT,
           APPT_STATUS_FLAG,
           APPT_STATUS_C,
           CANCEL_INITIATOR,
           APPT_MADE_DTTM,
           Appointment_Request_Date,
           APPT_DT,
           NEW_TO_SPEC,
           Appointment_Lag_Days,
           Original_Appointment_Request_Date,
           Original_CANCEL_INITIATOR,
           Last_APPT_STATUS_C,
           Last_APPT_STATUS_FLAG,
           Last_Seq,
           Last_Appointment_Request_Date,
           Last_APPT_DT,
           Appointment_Lag_Business_Days,
           (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE ddte.weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND ddte.day_date >= metric.Original_Appointment_Request_Date AND ddte.day_date < metric.Last_APPT_DT AND metric.APPT_SERIAL_NUM_COUNT > 1 AND metric.Seq = 1) Appointment_Lag_Business_Days_from_Original
	FROM #metric metric
	WHERE
		metric.Original_Appointment_Request_Date BETWEEN @locstartdate AND @locenddate
	--WHERE APPT_SERIAL_NUM_COUNT > 1
	--ORDER BY APPT_SERIAL_NUM
	--		,APPT_MADE_DTTM
	ORDER BY APPT_SERIAL_NUM_COUNT
	        ,APPT_SERIAL_NUM
			,APPT_MADE_DTTM
--*/
	--SELECT *
	--FROM #metric
	--WHERE Seq = 1
	----ORDER BY APPT_SERIAL_NUM
	----		,APPT_MADE_DTTM
	--ORDER BY APPT_SERIAL_NUM_COUNT
	--        ,APPT_SERIAL_NUM
	--		,APPT_MADE_DTTM