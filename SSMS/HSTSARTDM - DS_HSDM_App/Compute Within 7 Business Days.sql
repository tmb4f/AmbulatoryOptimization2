USE DS_HSDM_App
SELECT twodaysafter.day_date
      ,twodaysafter.day_of_week_num
	  ,CASE WHEN twodaysafter.day_of_week_num BETWEEN 2 AND 6 THEN twodaysafter.day_date_7_business_days
	        WHEN twodaysafter.day_of_week_num = 1 THEN CAST(DATEADD(DAY, -1, twodaysafter.one_day_after_day_date_7_business_days) AS SMALLDATETIME)
	        WHEN twodaysafter.day_of_week_num = 7 THEN CAST(DATEADD(DAY, -1, twodaysafter.two_days_after_day_date_7_business_days) AS SMALLDATETIME)
	   END AS day_date_7_business_days
FROM
(
SELECT onedayafter.day_date
      ,onedayafter.day_of_week_num
	  ,onedayafter.day_date_7_business_days
	  ,onedayafter.one_day_after_day_date_7_business_days
	  ,CAST(LEAD(onedayafter.two_days_after_day_date, 6, 0) OVER (PARTITION BY ddte2.weekday_ind ORDER BY onedayafter.two_days_after_day_date) AS DATETIME)  AS two_days_after_day_date_7_business_days
FROM
(
SELECT base.day_date
      ,base.day_of_week_num
	  ,base.day_date_7_business_days
	  ,CAST(LEAD(base.one_day_after_day_date, 6, 0) OVER (PARTITION BY ddte1.weekday_ind ORDER BY base.one_day_after_day_date) AS DATETIME) AS one_day_after_day_date_7_business_days
	  ,base.two_days_after_day_date
FROM
(
SELECT day_date,
       day_of_week_num,
       LEAD(day_date, 6, 0) OVER (PARTITION BY weekday_ind ORDER BY day_date) AS day_date_7_business_days,
	   DATEADD(DAY, 1, day_date) AS one_day_after_day_date,
	   DATEADD(DAY, 2, day_date) AS two_days_after_day_date
FROM DS_HSDW_Prod.Rptg.vwDim_Date
WHERE day_date BETWEEN '7/1/2017' AND '10/8/2019'
) base
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte1
ON ddte1.day_date = base.one_day_after_day_date
) onedayafter
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte2
ON ddte2.day_date = onedayafter.two_days_after_day_date
) twodaysafter
WHERE twodaysafter.day_date BETWEEN '7/1/2017' AND '9/30/2019'
ORDER BY twodaysafter.day_date
