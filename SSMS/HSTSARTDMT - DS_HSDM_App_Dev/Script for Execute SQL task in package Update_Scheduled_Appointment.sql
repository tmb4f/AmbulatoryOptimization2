USE DS_HSDM_App_Dev

MERGE Stage.Scheduled_Appointment AS Target
USING Stage.Scheduled_Appointment_Daily_Update AS Source
ON Target.PAT_ENC_CSN_ID = Source.PAT_ENC_CSN_ID
WHEN MATCHED
THEN UPDATE SET Target.PAT_ID = Source.PAT_ID
               ,Target.PAT_NAME = Source.PAT_NAME
			   ,Target.IDENTITY_ID = Source.IDENTITY_ID
			   ,Target.MYCHART_STATUS_C = Source.MYCHART_STATUS_C
			   ,Target.MYCHART_STATUS_NAME = Source.MYCHART_STATUS_NAME
			   ,Target.VIS_NEW_TO_SYS_YN = Source.VIS_NEW_TO_SYS_YN
			   ,Target.VIS_NEW_TO_DEP_YN = Source.VIS_NEW_TO_DEP_YN
			   ,Target.VIS_NEW_TO_PROV_YN = Source.VIS_NEW_TO_PROV_YN
			   ,Target.APPT_SERIAL_NUM = Source.APPT_SERIAL_NUM
			   ,Target.RESCHED_APPT_CSN_ID = Source.RESCHED_APPT_CSN_ID
			   ,Target.PRC_ID = Source.PRC_ID
			   ,Target.PRC_NAME = Source.PRC_NAME
			   ,Target.APPT_BLOCK_C = Source.APPT_BLOCK_C
			   ,Target.APPT_BLOCK_NAME = Source.APPT_BLOCK_NAME
			   ,Target.APPT_LENGTH = Source.APPT_LENGTH
			   ,Target.APPT_STATUS_C = Source.APPT_STATUS_C
			   ,Target.APPT_STATUS_NAME = Source.APPT_STATUS_NAME
			   ,Target.APPT_CONF_STAT_C = Source.APPT_CONF_STAT_C
			   ,Target.CANCEL_LEAD_HOURS = Source.CANCEL_LEAD_HOURS
			   ,Target.CANCEL_INITIATOR = Source.CANCEL_INITIATOR
			   ,Target.COMPLETED_STATUS_YN = Source.COMPLETED_STATUS_YN
			   ,Target.SAME_DAY_YN = Source.SAME_DAY_YN
			   ,Target.SAME_DAY_CANC_YN = Source.SAME_DAY_CANC_YN
			   ,Target.CANCEL_REASON_C = Source.CANCEL_REASON_C
			   ,Target.CANCEL_REASON_NAME = Source.CANCEL_REASON_NAME
			   ,Target.WALK_IN_YN = Source.WALK_IN_YN
			   ,Target.OVERBOOKED_YN = Source.OVERBOOKED_YN
			   ,Target.OVERRIDE_YN = Source.OVERRIDE_YN
			   ,Target.UNAVAILABLE_TIME_YN = Source.UNAVAILABLE_TIME_YN
			   ,Target.CHANGE_CNT = Source.CHANGE_CNT
			   ,Target.JOINT_APPT_YN = Source.JOINT_APPT_YN
			   ,Target.PHONE_REM_STAT_C = Source.PHONE_REM_STAT_C
			   ,Target.PHONE_REM_STAT_NAME = Source.PHONE_REM_STAT_NAME
			   ,Target.CONTACT_DATE = Source.CONTACT_DATE
			   ,Target.APPT_MADE_DATE = Source.APPT_MADE_DATE
			   ,Target.APPT_CANC_DATE = Source.APPT_CANC_DATE
			   ,Target.APPT_CONF_DTTM = Source.APPT_CONF_DTTM
			   ,Target.APPT_DTTM = Source.APPT_DTTM
			   ,Target.SIGNIN_DTTM = Source.SIGNIN_DTTM
			   ,Target.PAGED_DTTM = Source.PAGED_DTTM
			   ,Target.BEGIN_CHECKIN_DTTM = Source.BEGIN_CHECKIN_DTTM
			   ,Target.CHECKIN_DTTM = Source.CHECKIN_DTTM
			   ,Target.ARVL_LIST_REMOVE_DTTM = Source.ARVL_LIST_REMOVE_DTTM
			   ,Target.ROOMED_DTTM = Source.ROOMED_DTTM
			   ,Target.FIRST_ROOM_ASSIGN_DTTM = Source.FIRST_ROOM_ASSIGN_DTTM
			   ,Target.NURSE_LEAVE_DTTM = Source.NURSE_LEAVE_DTTM
			   ,Target.PHYS_ENTER_DTTM = Source.PHYS_ENTER_DTTM
			   ,Target.VISIT_END_DTTM = Source.VISIT_END_DTTM
			   ,Target.CHECKOUT_DTTM = Source.CHECKOUT_DTTM
			   ,Target.TIME_TO_ROOM_MINUTES = Source.TIME_TO_ROOM_MINUTES
			   ,Target.TIME_IN_ROOM_MINUTES = Source.TIME_IN_ROOM_MINUTES
			   ,Target.CYCLE_TIME_MINUTES = Source.CYCLE_TIME_MINUTES
			   ,Target.REFERRING_PROV_ID = Source.REFERRING_PROV_ID
			   ,Target.REFERRING_PROV_NAME_WID = Source.REFERRING_PROV_NAME_WID
			   ,Target.PROV_ID = Source.PROV_ID
			   ,Target.PROV_NAME_WID = Source.PROV_NAME_WID
			   ,Target.RPT_GRP_FIVE = Source.RPT_GRP_FIVE
			   ,Target.APPT_ENTRY_USER_ID = Source.APPT_ENTRY_USER_ID
			   ,Target.APPT_ENTRY_USER_NAME_WID = Source.APPT_ENTRY_USER_NAME_WID
			   ,Target.PROV_NAME = Source.PROV_NAME
			   ,Target.PAYOR_ID = Source.PAYOR_ID
			   ,Target.PAYOR_NAME = Source.PAYOR_NAME
			   ,Target.BENEFIT_PLAN_ID = Source.BENEFIT_PLAN_ID
			   ,Target.BENEFIT_PLAN_NAME = Source.BENEFIT_PLAN_NAME
			   ,Target.FIN_CLASS_NAME = Source.FIN_CLASS_NAME
			   ,Target.DO_NOT_BILL_INS_YN = Source.DO_NOT_BILL_INS_YN
			   ,Target.SELF_PAY_VISIT_YN = Source.SELF_PAY_VISIT_YN
			   ,Target.HSP_ACCOUNT_ID = Source.HSP_ACCOUNT_ID
			   ,Target.ACCOUNT_ID = Source.ACCOUNT_ID
			   ,Target.COPAY_DUE = Source.COPAY_DUE
			   ,Target.COPAY_COLLECTED = Source.COPAY_COLLECTED
			   ,Target.COPAY_USER_ID = Source.COPAY_USER_ID
			   ,Target.COPAY_USER_NAME_WID = Source.COPAY_USER_NAME_WID
			   ,Target.DEPARTMENT_ID = Source.DEPARTMENT_ID
			   ,Target.DEPARTMENT_NAME = Source.DEPARTMENT_NAME
			   ,Target.DEPT_ABBREVIATION = Source.DEPT_ABBREVIATION
			   ,Target.RPT_GRP_THIRTY = Source.RPT_GRP_THIRTY
			   ,Target.RPT_GRP_SIX = Source.RPT_GRP_SIX
			   ,Target.RPT_GRP_SEVEN = Source.RPT_GRP_SEVEN
			   ,Target.DEPT_SPECIALTY_C = Source.DEPT_SPECIALTY_C
			   ,Target.DEPT_SPECIALTY_NAME = Source.DEPT_SPECIALTY_NAME
			   ,Target.CENTER_C = Source.CENTER_C
			   ,Target.CENTER_NAME = Source.CENTER_NAME
			   ,Target.LOC_ID = Source.LOC_ID
			   ,Target.LOC_NAME = Source.LOC_NAME
			   ,Target.SERV_AREA_ID = Source.SERV_AREA_ID
			   ,Target.APPT_STATUS_FLAG = Source.APPT_STATUS_FLAG
			   ,Target.Load_Dtm = Source.Load_Dtm
			   ,Target.sk_Dim_Pt = Source.sk_Dim_Pt
			   ,Target.sk_Fact_Pt_Enc_Clrt = Source.sk_Fact_Pt_Enc_Clrt
			   ,Target.sk_Fact_Pt_Acct = Source.sk_Fact_Pt_Acct
			   ,Target.VIS_NEW_TO_SPEC_YN = Source.VIS_NEW_TO_SPEC_YN
			   ,Target.VIS_NEW_TO_SERV_AREA_YN = Source.VIS_NEW_TO_SERV_AREA_YN
			   ,Target.VIS_NEW_TO_LOC_YN = Source.VIS_NEW_TO_LOC_YN
			   ,Target.REFERRAL_ID = Source.REFERRAL_ID
			   ,Target.ENTRY_DATE = Source.ENTRY_DATE
			   ,Target.RFL_STATUS_NAME = Source.RFL_STATUS_NAME
			   ,Target.RFL_TYPE_NAME = Source.RFL_TYPE_NAME
			   ,Target.PROV_SPECIALTY_C = Source.PROV_SPECIALTY_C
			   ,Target.PROV_SPECIALTY_NAME = Source.PROV_SPECIALTY_NAME
			   ,Target.APPT_DT = Source.APPT_DT
			   ,Target.ENC_TYPE_C = Source.ENC_TYPE_C
			   ,Target.ENC_TYPE_TITLE = Source.ENC_TYPE_TITLE
			   ,Target.APPT_CONF_STAT_NAME = Source.APPT_CONF_STAT_NAME
			   ,Target.ZIP = Source.ZIP
			   ,Target.SER_RPT_GRP_SIX = Source.SER_RPT_GRP_SIX
			   ,Target.SER_RPT_GRP_EIGHT = Source.SER_RPT_GRP_EIGHT
			   ,Target.F2F_Flag = Source.F2F_Flag
			   ,Target.APPT_CANC_DTTM = Source.APPT_CANC_DTTM
			   ,Target.APPT_CANC_USER_ID = Source.APPT_CANC_USER_ID
			   ,Target.APPT_CANC_USER_NAME_WID = Source.APPT_CANC_USER_NAME_WID
			   ,Target.APPT_CONF_USER_ID = Source.APPT_CONF_USER_ID
			   ,Target.APPT_CONF_USER_NAME = Source.APPT_CONF_USER_NAME
			   ,Target.CHANGE_DATE = Source.CHANGE_DATE
			   ,Target.APPT_MADE_DTTM = Source.APPT_MADE_DTTM
			   ,Target.UPDATE_DATE = Source.UPDATE_DATE
WHEN NOT MATCHED BY Target
THEN INSERT ([PAT_ENC_CSN_ID]
            ,[PAT_ID]
            ,[PAT_NAME]
            ,[IDENTITY_ID]
            ,[MYCHART_STATUS_C]
            ,[MYCHART_STATUS_NAME]
            ,[VIS_NEW_TO_SYS_YN]
            ,[VIS_NEW_TO_DEP_YN]
            ,[VIS_NEW_TO_PROV_YN]
            ,[APPT_SERIAL_NUM]
            ,[RESCHED_APPT_CSN_ID]
            ,[PRC_ID]
            ,[PRC_NAME]
            ,[APPT_BLOCK_C]
            ,[APPT_BLOCK_NAME]
            ,[APPT_LENGTH]
            ,[APPT_STATUS_C]
            ,[APPT_STATUS_NAME]
            ,[APPT_CONF_STAT_C]
            ,[CANCEL_LEAD_HOURS]
            ,[CANCEL_INITIATOR]
            ,[COMPLETED_STATUS_YN]
            ,[SAME_DAY_YN]
            ,[SAME_DAY_CANC_YN]
            ,[CANCEL_REASON_C]
            ,[CANCEL_REASON_NAME]
            ,[WALK_IN_YN]
            ,[OVERBOOKED_YN]
            ,[OVERRIDE_YN]
            ,[UNAVAILABLE_TIME_YN]
            ,[CHANGE_CNT]
            ,[JOINT_APPT_YN]
            ,[PHONE_REM_STAT_C]
            ,[PHONE_REM_STAT_NAME]
            ,[CONTACT_DATE]
            ,[APPT_MADE_DATE]
            ,[APPT_CANC_DATE]
            ,[APPT_CONF_DTTM]
            ,[APPT_DTTM]
            ,[SIGNIN_DTTM]
            ,[PAGED_DTTM]
            ,[BEGIN_CHECKIN_DTTM]
            ,[CHECKIN_DTTM]
            ,[ARVL_LIST_REMOVE_DTTM]
            ,[ROOMED_DTTM]
            ,[FIRST_ROOM_ASSIGN_DTTM]
            ,[NURSE_LEAVE_DTTM]
            ,[PHYS_ENTER_DTTM]
            ,[VISIT_END_DTTM]
            ,[CHECKOUT_DTTM]
            ,[TIME_TO_ROOM_MINUTES]
            ,[TIME_IN_ROOM_MINUTES]
            ,[CYCLE_TIME_MINUTES]
            ,[REFERRING_PROV_ID]
            ,[REFERRING_PROV_NAME_WID]
            ,[PROV_ID]
            ,[PROV_NAME_WID]
            ,[RPT_GRP_FIVE]
            ,[APPT_ENTRY_USER_ID]
            ,[APPT_ENTRY_USER_NAME_WID]
            ,[PROV_NAME]
            ,[PAYOR_ID]
            ,[PAYOR_NAME]
            ,[BENEFIT_PLAN_ID]
            ,[BENEFIT_PLAN_NAME]
            ,[FIN_CLASS_NAME]
            ,[DO_NOT_BILL_INS_YN]
            ,[SELF_PAY_VISIT_YN]
            ,[HSP_ACCOUNT_ID]
            ,[ACCOUNT_ID]
            ,[COPAY_DUE]
            ,[COPAY_COLLECTED]
            ,[COPAY_USER_ID]
            ,[COPAY_USER_NAME_WID]
            ,[DEPARTMENT_ID]
            ,[DEPARTMENT_NAME]
            ,[DEPT_ABBREVIATION]
            ,[RPT_GRP_THIRTY]
            ,[RPT_GRP_SIX]
            ,[RPT_GRP_SEVEN]
            ,[DEPT_SPECIALTY_C]
            ,[DEPT_SPECIALTY_NAME]
            ,[CENTER_C]
            ,[CENTER_NAME]
            ,[LOC_ID]
            ,[LOC_NAME]
            ,[SERV_AREA_ID]
            ,[APPT_STATUS_FLAG]
            ,[Load_Dtm]
            ,[sk_Dim_Pt]
            ,[sk_Fact_Pt_Enc_Clrt]
            ,[sk_Fact_Pt_Acct]
            ,[VIS_NEW_TO_SPEC_YN]
            ,[VIS_NEW_TO_SERV_AREA_YN]
            ,[VIS_NEW_TO_LOC_YN]
            ,[REFERRAL_ID]
            ,[ENTRY_DATE]
            ,[RFL_STATUS_NAME]
            ,[RFL_TYPE_NAME]
            ,[PROV_SPECIALTY_C]
            ,[PROV_SPECIALTY_NAME]
            ,[APPT_DT]
            ,[ENC_TYPE_C]
            ,[ENC_TYPE_TITLE]
            ,[APPT_CONF_STAT_NAME]
            ,[ZIP]
            ,[SER_RPT_GRP_SIX]
            ,[SER_RPT_GRP_EIGHT]
            ,[F2F_Flag]
            ,[APPT_CANC_DTTM]
            ,[APPT_CANC_USER_ID]
            ,[APPT_CANC_USER_NAME_WID]
            ,[APPT_CONF_USER_ID]
            ,[APPT_CONF_USER_NAME]
            ,[CHANGE_DATE]
            ,[APPT_MADE_DTTM]
            ,[UPDATE_DATE])
     VALUES
	        (Source.[PAT_ENC_CSN_ID]
            ,Source.[PAT_ID]
            ,Source.[PAT_NAME]
            ,Source.[IDENTITY_ID]
            ,Source.[MYCHART_STATUS_C]
            ,Source.[MYCHART_STATUS_NAME]
            ,Source.[VIS_NEW_TO_SYS_YN]
            ,Source.[VIS_NEW_TO_DEP_YN]
            ,Source.[VIS_NEW_TO_PROV_YN]
            ,Source.[APPT_SERIAL_NUM]
            ,Source.[RESCHED_APPT_CSN_ID]
            ,Source.[PRC_ID]
            ,Source.[PRC_NAME]
            ,Source.[APPT_BLOCK_C]
            ,Source.[APPT_BLOCK_NAME]
            ,Source.[APPT_LENGTH]
            ,Source.[APPT_STATUS_C]
            ,Source.[APPT_STATUS_NAME]
            ,Source.[APPT_CONF_STAT_C]
            ,Source.[CANCEL_LEAD_HOURS]
            ,Source.[CANCEL_INITIATOR]
            ,Source.[COMPLETED_STATUS_YN]
            ,Source.[SAME_DAY_YN]
            ,Source.[SAME_DAY_CANC_YN]
            ,Source.[CANCEL_REASON_C]
            ,Source.[CANCEL_REASON_NAME]
            ,Source.[WALK_IN_YN]
            ,Source.[OVERBOOKED_YN]
            ,Source.[OVERRIDE_YN]
            ,Source.[UNAVAILABLE_TIME_YN]
            ,Source.[CHANGE_CNT]
            ,Source.[JOINT_APPT_YN]
            ,Source.[PHONE_REM_STAT_C]
            ,Source.[PHONE_REM_STAT_NAME]
            ,Source.[CONTACT_DATE]
            ,Source.[APPT_MADE_DATE]
            ,Source.[APPT_CANC_DATE]
            ,Source.[APPT_CONF_DTTM]
            ,Source.[APPT_DTTM]
            ,Source.[SIGNIN_DTTM]
            ,Source.[PAGED_DTTM]
            ,Source.[BEGIN_CHECKIN_DTTM]
            ,Source.[CHECKIN_DTTM]
            ,Source.[ARVL_LIST_REMOVE_DTTM]
            ,Source.[ROOMED_DTTM]
            ,Source.[FIRST_ROOM_ASSIGN_DTTM]
            ,Source.[NURSE_LEAVE_DTTM]
            ,Source.[PHYS_ENTER_DTTM]
            ,Source.[VISIT_END_DTTM]
            ,Source.[CHECKOUT_DTTM]
            ,Source.[TIME_TO_ROOM_MINUTES]
            ,Source.[TIME_IN_ROOM_MINUTES]
            ,Source.[CYCLE_TIME_MINUTES]
            ,Source.[REFERRING_PROV_ID]
            ,Source.[REFERRING_PROV_NAME_WID]
            ,Source.[PROV_ID]
            ,Source.[PROV_NAME_WID]
            ,Source.[RPT_GRP_FIVE]
            ,Source.[APPT_ENTRY_USER_ID]
            ,Source.[APPT_ENTRY_USER_NAME_WID]
            ,Source.[PROV_NAME]
            ,Source.[PAYOR_ID]
            ,Source.[PAYOR_NAME]
            ,Source.[BENEFIT_PLAN_ID]
            ,Source.[BENEFIT_PLAN_NAME]
            ,Source.[FIN_CLASS_NAME]
            ,Source.[DO_NOT_BILL_INS_YN]
            ,Source.[SELF_PAY_VISIT_YN]
            ,Source.[HSP_ACCOUNT_ID]
            ,Source.[ACCOUNT_ID]
            ,Source.[COPAY_DUE]
            ,Source.[COPAY_COLLECTED]
            ,Source.[COPAY_USER_ID]
            ,Source.[COPAY_USER_NAME_WID]
            ,Source.[DEPARTMENT_ID]
            ,Source.[DEPARTMENT_NAME]
            ,Source.[DEPT_ABBREVIATION]
            ,Source.[RPT_GRP_THIRTY]
            ,Source.[RPT_GRP_SIX]
            ,Source.[RPT_GRP_SEVEN]
            ,Source.[DEPT_SPECIALTY_C]
            ,Source.[DEPT_SPECIALTY_NAME]
            ,Source.[CENTER_C]
            ,Source.[CENTER_NAME]
            ,Source.[LOC_ID]
            ,Source.[LOC_NAME]
            ,Source.[SERV_AREA_ID]
            ,Source.[APPT_STATUS_FLAG]
			,Source.[Load_Dtm]
			,Source.[sk_Dim_Pt]
			,Source.[sk_Fact_Pt_Enc_Clrt]
			,Source.[sk_Fact_Pt_Acct]
            ,Source.[VIS_NEW_TO_SPEC_YN]
            ,Source.[VIS_NEW_TO_SERV_AREA_YN]
            ,Source.[VIS_NEW_TO_LOC_YN]
            ,Source.[REFERRAL_ID]
            ,Source.[ENTRY_DATE]
            ,Source.[RFL_STATUS_NAME]
            ,Source.[RFL_TYPE_NAME]
            ,Source.[PROV_SPECIALTY_C]
            ,Source.[PROV_SPECIALTY_NAME]
            ,Source.[APPT_DT]
            ,Source.[ENC_TYPE_C]
            ,Source.[ENC_TYPE_TITLE]
            ,Source.[APPT_CONF_STAT_NAME]
            ,Source.[ZIP]
            ,Source.[SER_RPT_GRP_SIX]
            ,Source.[SER_RPT_GRP_EIGHT]
            ,Source.[F2F_Flag]
            ,Source.[APPT_CANC_DTTM]
            ,Source.[APPT_CANC_USER_ID]
            ,Source.[APPT_CANC_USER_NAME_WID]
            ,Source.[APPT_CONF_USER_ID]
            ,Source.[APPT_CONF_USER_NAME]
            ,Source.[CHANGE_DATE]
            ,Source.[APPT_MADE_DTTM]
            ,Source.[UPDATE_DATE])
;