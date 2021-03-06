tmb4USE CLARITY

/*PROV_ATTR_1_C 2	Non - Billable Provider */
/*
SELECT ser.PROV_ID
      ,attr.*
	  --,zcpa1.NAME
FROM CLARITY.dbo.CLARITY_SER ser
LEFT OUTER JOIN CLARITY.dbo.PROV_ATTR_OT attr
ON attr.PROV_ATTR_ID = ser.PROV_ATTR_ID
--LEFT OUTER JOIN CLARITY.dbo.ZC_PROV_ATTR_1 zcpa1
--ON zcpa1.PROV_ATTR_1_C = attr.PROV_ATTR_1_C
--WHERE ser.PROV_ID = '58340'
--WHERE attr.PROV_ATTR_ID = '10000'
WHERE attr.PROV_ATTR_1_C = 2 OR attr.PROV_ATTR_2_C = 2 OR attr.PROV_ATTR_3_C = 2
--ORDER BY attr.CONTACT_DATE_REAL
ORDER BY attr.PROV_ATTR_ID
        ,attr.CONTACT_DATE_REAL
*/
SELECT [PROV_ATTR_ID]
      ,[CONTACT_DATE_REAL]
      ,[CONTACT_DATE]
      ,[CONTACT_TYPE]
      ,[RECORD_STATE] -- Indicates whether the record is active, inactive, or deleted.
      ,[COMMENTS]
      ,[CONTACT_TO_DATE]
      ,[SERV_PROV_YN]
      ,[BILL_PROV_YN] -- Indicates whether the provider is a valid billing provider. Y indicates they are a valid billing provider, N indicates they are not.
      ,[BILL_PROV_ID]
      ,[CM_PHY_OWNER_ID]
      ,[CM_LOG_OWNER_ID]
  FROM [CLARITY].[dbo].[PROV_ATTR_INFO_OT]
  --WHERE CONTACT_TYPE = 'Billing Provider Effective Dates'
  --AND RECORD_STATE = 'ACTIVE'
  --WHERE PROV_ATTR_ID = '10675'
  WHERE (BILL_PROV_YN = 'Y' AND RECORD_STATE = 'ACTIVE')
  ORDER BY PROV_ATTR_ID
         , CONTACT_DATE
		 , CONTACT_TO_DATE
