/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [PROV_ID]
      ,[CONTACT_DATE_REAL]
      ,[CONTACT_DATE]
	  ,ddte.day_date
      ,[EFF_TO_DATE]
      ,[PROV_TYPE_OT_C]
      ,[PROV_TYPE_OT_NAME]
  FROM [CLARITY_App].[Rptg].[vwCLARITY_SER_OT_PROV_TYPE] ot
  CROSS JOIN CLARITY_App.Rptg.vwDim_Date ddte
  WHERE (ddte.day_date BETWEEN ot.CONTACT_DATE AND ot.EFF_TO_DATE)
  AND ot.PROV_ID = '10161'
  AND ot.PROV_TYPE_OT_C IS NOT NULL
  ORDER BY PROV_ID
         , ddte.day_date
         --, EFF_TO_DATE