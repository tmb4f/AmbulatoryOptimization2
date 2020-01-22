USE DS_HSDM_App

IF OBJECT_ID('tempdb..#newpt ') IS NOT NULL
DROP TABLE #newpt

--SELECT  main.PAT_ENC_CSN_ID,
--						main.APPT_SERIAL_NUM,
--						ROW_NUMBER() OVER (PARTITION BY main.APPT_SERIAL_NUM ORDER BY main.APPT_MADE_DTTM) AS Seq, -- sequence number for identifying and ordering linked appointments
--						COUNT(*) OVER (PARTITION BY main.APPT_SERIAL_NUM) APPT_SERIAL_NUM_COUNT,  -- number of linked appoointments; > 1 indicates a cancellation and a reschedule using the Cancel/Reschedule workflow
--						main.APPT_STATUS_FLAG,
--						main.APPT_STATUS_C,
--						main.CANCEL_INITIATOR,
--						main.APPT_DT,
--						--main.VIS_NEW_TO_SPEC_YN,
--			--            main.APPT_MADE_DATE,
--			--            main.ENTRY_DATE,
--			            main.APPT_DTTM,
--						main.APPT_CANC_DTTM,
--						ser.Prov_Typ,
--						--main.CHANGE_DATE,
--						main.APPT_MADE_DTTM,
--						CASE -- Appointment Request Date is the earlier of the referral entry/change dates and the creation date of the appointment
--							WHEN main.ENTRY_DATE IS NULL THEN
--								main.APPT_MADE_DATE
--							WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
--								main.APPT_MADE_DATE
--							WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
--								main.ENTRY_DATE
--							ELSE
--								main.CHANGE_DATE
--						END AS Appointment_Request_Date,
--						main.VIS_NEW_TO_SPEC_YN,
--						aggr.NEW_TO_SPEC

--				FROM Stage.Scheduled_Appointment AS main
--				LEFT OUTER JOIN -- Set flag indicating that an appointment or a set of related ("linked") appointments can be classified as a new patient encounter
--				(
--					SELECT APPT_SERIAL_NUM,
--					MAX(CASE WHEN VIS_NEW_TO_SPEC_YN = 'Y' THEN 1 ELSE 0 END) AS NEW_TO_SPEC--,
--					--COUNT(*) AS APPT_SERIAL_NUM_COUNT
--					FROM DS_HSDM_App.Stage.Scheduled_Appointment
--					GROUP BY APPT_SERIAL_NUM
--				) aggr
--					ON aggr.APPT_SERIAL_NUM = main.APPT_SERIAL_NUM
--					LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
--						ON ser.PROV_ID = main.PROV_ID
--				WHERE main.APPT_SERIAL_NUM IN (200016183260
--,200016183261)
--ORDER BY main.APPT_SERIAL_NUM
--       , Seq

                SELECT  main.PAT_ENC_CSN_ID,
						main.APPT_SERIAL_NUM,
						ROW_NUMBER() OVER (PARTITION BY main.APPT_SERIAL_NUM ORDER BY main.APPT_MADE_DTTM) AS Seq, -- sequence number for identifying and ordering linked appointments
						COUNT(*) OVER (PARTITION BY main.APPT_SERIAL_NUM) APPT_SERIAL_NUM_COUNT,  -- number of linked appoointments; > 1 indicates a cancellation and a reschedule using the Cancel/Reschedule workflow
						--COUNT(DISTINCT main.VIS_NEW_TO_SPEC_YN) OVER (PARTITION BY main.APPT_SERIAL_NUM) VIS_NEW_TO_SPEC_COUNT,
						MIN(main.APPT_MADE_DTTM) OVER (PARTITION BY main.APPT_SERIAL_NUM) AS EARLIEST_APPT_MADE_DTTM,
						aggr.VIS_NEW_TO_SPEC_COUNT,
						main.APPT_STATUS_FLAG,
						main.APPT_STATUS_C,
						main.CANCEL_INITIATOR,
						main.APPT_DT,
						--main.VIS_NEW_TO_SPEC_YN,
			--            main.APPT_MADE_DATE,
			--            main.ENTRY_DATE,
			            main.APPT_DTTM,
						main.APPT_CANC_DTTM,
						ser.Prov_Typ,
						--main.CHANGE_DATE,
						main.APPT_MADE_DTTM,
						CASE -- Appointment Request Date is the earlier of the referral entry/change dates and the creation date of the appointment
							WHEN main.ENTRY_DATE IS NULL THEN
								main.APPT_MADE_DATE
							WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
								main.APPT_MADE_DATE
							WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
								main.ENTRY_DATE
							ELSE
								main.CHANGE_DATE
						END AS Appointment_Request_Date,
						main.VIS_NEW_TO_SPEC_YN--,
						--aggr.NEW_TO_SPEC

				INTO #newpt

				FROM Stage.Scheduled_Appointment AS main
--				LEFT OUTER JOIN -- Set flag indicating that an appointment or a set of related ("linked") appointments can be classified as a new patient encounter
--				(
--					SELECT APPT_SERIAL_NUM,
--					MAX(CASE WHEN VIS_NEW_TO_SPEC_YN = 'Y' THEN 1 ELSE 0 END) AS NEW_TO_SPEC--,
--					--COUNT(*) AS APPT_SERIAL_NUM_COUNT
--					FROM DS_HSDM_App.Stage.Scheduled_Appointment
--					GROUP BY APPT_SERIAL_NUM
--				) aggr
--					ON aggr.APPT_SERIAL_NUM = main.APPT_SERIAL_NUM
				LEFT OUTER JOIN -- Count distinct VIS_NEW_TO_SPEC_YN values in a set of related ("linked") appointments
				(
					SELECT APPT_SERIAL_NUM,
					COUNT(DISTINCT VIS_NEW_TO_SPEC_YN) AS VIS_NEW_TO_SPEC_COUNT--,
					--COUNT(*) AS APPT_SERIAL_NUM_COUNT
					FROM DS_HSDM_App.Stage.Scheduled_Appointment
					GROUP BY APPT_SERIAL_NUM
				) aggr
					ON aggr.APPT_SERIAL_NUM = main.APPT_SERIAL_NUM
					LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
						ON ser.PROV_ID = main.PROV_ID
--				WHERE main.APPT_SERIAL_NUM IN (200016183260
--,200016183261)
				--WHERE main.APPT_DTTM >= '1/1/2019 00:00 AM'

--SELECT *
--FROM #newpt
--ORDER BY APPT_SERIAL_NUM
--       , Seq

SELECT newpt.PAT_ENC_CSN_ID,
       newpt.APPT_SERIAL_NUM,
       newpt.Seq,
       newpt.APPT_SERIAL_NUM_COUNT,
       newpt.VIS_NEW_TO_SPEC_COUNT,
       newpt.VIS_NEW_TO_SPEC_YN,
	   newpt.EARLIEST_APPT_MADE_DTTM,
       newpt.APPT_STATUS_FLAG,
       newpt.APPT_STATUS_C,
       newpt.CANCEL_INITIATOR,
       newpt.APPT_DT,
       newpt.APPT_DTTM,
       newpt.APPT_CANC_DTTM,
       newpt.Prov_Typ,
       newpt.APPT_MADE_DTTM,
       newpt.Appointment_Request_Date--,
       --newpt.VIS_NEW_TO_SPEC_YN
FROM #newpt newpt
INNER JOIN
(
SELECT DISTINCT APPT_SERIAL_NUM
FROM #newpt
WHERE
EARLIEST_APPT_MADE_DTTM >= '5/1/2019 00:00 AM'
AND Seq = 1
AND VIS_NEW_TO_SPEC_COUNT > 1
) aggr
ON newpt.APPT_SERIAL_NUM = aggr.APPT_SERIAL_NUM
WHERE newpt.EARLIEST_APPT_MADE_DTTM >= '5/1/2019 00:00 AM'
ORDER BY newpt.EARLIEST_APPT_MADE_DTTM
       , newpt.APPT_SERIAL_NUM
       , newpt.Seq