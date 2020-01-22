USE DS_HSDM_App

IF OBJECT_ID('tempdb..#main ') IS NOT NULL
DROP TABLE #main

--
-- 12/5/2019: 3847
--
--		1887 with a physcn.Clrt_Financial_Division_Name value
--			1404 Prov_Typ = 'Physician'
--		1960 without a physcn.Clrt_Financial_Division_Name value
--
--		Same whether you join to DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined using sk_Dim_Physcn or PROV_ID
--

			--SELECT DISTINCT
			--       ser.Staff_Resource,
			--	   ser.Prov_Typ,
   --                appts.PROV_ID AS provider_id,
   --                appts.PROV_NAME AS provider_name,
   --                appts.RPT_GRP_SIX AS pod_id,
   --                mdmloc.PFA_POD AS pod_name,
   --                appts.RPT_GRP_SEVEN AS hub_id,
   --                mdmloc.HUB AS hub_name,
   --                appts.DEPARTMENT_ID AS epic_department_id,
   --                mdm.epic_department_name AS epic_department_name,
   --                mdm.epic_department_name_external AS epic_department_name_external,
			--	   --CASE WHEN ISNULL(COALESCE(physcn.Clrt_Financial_Division_Name, appts.SER_RPT_GRP_SIX, NULL),'No') <> 'No' THEN 'Yes' ELSE 'No' END AS est_financial_division_name,
			--	   CASE WHEN ISNULL(COALESCE(physcn.Clrt_Financial_Division_Name, NULL),'No') <> 'No' THEN 'Yes' ELSE 'No' END AS est_financial_division_name,
   --                -- MDM
   --                mdm.service_line_id,
   --                mdm.service_line,
   --                physsvc.Service_Line_ID AS prov_service_line_id,
   --                physsvc.Service_Line AS prov_service_line,
   --                mdm.sub_service_line_id,
   --                mdm.sub_service_line,
   --                mdm.opnl_service_id,
   --                mdm.opnl_service_name,
   --                mdm.corp_service_line_id,
   --                mdm.corp_service_line,
   --                mdm.hs_area_id,
   --                mdm.hs_area_name,
   --                physsvc.hs_area_id AS prov_hs_area_id,
   --                physsvc.hs_area_name AS prov_hs_area_name,
   --                --Select
   --                ser.sk_Dim_Physcn,
   --                doc.UVaID,
	  --             appts.SER_RPT_GRP_SIX AS financial_division,
	  --             appts.SER_RPT_GRP_EIGHT AS financial_subdivision,
			--	   mdmloc.LOC_ID AS rev_location_id,
			--	   mdmloc.REV_LOC_NAME AS rev_location,				   
   --                -- SOM
			--	   physcn.Clrt_Financial_Division AS financial_division_id,
			--	   physcn.Clrt_Financial_Division_Name AS financial_division_name,
			--	   physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id,
			--	   physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name,
			--	   physcn.SOM_Group_ID AS som_group_id,
			--	   physcn.SOM_group AS som_group_name,
			--	   physcn.SOM_department_id AS som_department_id,
			--	   physcn.SOM_department AS	som_department_name,
			--	   physcn.SOM_division_5 AS	som_division_id,
			--	   physcn.SOM_division_name AS som_division_name,
			--	   physcn.som_hs_area_id AS	som_hs_area_id,
			--	   physcn.som_hs_area_name AS som_hs_area_name,
			--	   appts.BILL_PROV_YN

			SELECT DISTINCT
			       ser.Staff_Resource,
				   ser.Prov_Typ,
                   --appts.PROV_ID AS provider_id,
                   appts.PROV_NAME AS provider_name,
                   --appts.RPT_GRP_SIX AS pod_id,
                   --mdmloc.PFA_POD AS pod_name,
                   --appts.RPT_GRP_SEVEN AS hub_id,
                   ----mdmloc.HUB AS hub_name,
                   --appts.DEPARTMENT_ID AS epic_department_id,
                   mdm.epic_department_name AS epic_department_name,
                   --mdm.epic_department_name_external AS epic_department_name_external,
				   CASE WHEN ISNULL(COALESCE(physcn.Clrt_Financial_Division_Name, appts.SER_RPT_GRP_SIX, NULL),'No') <> 'No' THEN 'Yes' ELSE 'No' END AS est_financial_division_name--,
				   --CASE WHEN ISNULL(COALESCE(physcn.Clrt_Financial_Division_Name, NULL),'No') <> 'No' THEN 'Yes' ELSE 'No' END AS est_financial_division_name--,
                   -- MDM
       --            mdm.service_line_id,
       --            mdm.service_line,
       --            physsvc.Service_Line_ID AS prov_service_line_id,
       --            physsvc.Service_Line AS prov_service_line,
       --            mdm.sub_service_line_id,
       --            mdm.sub_service_line,
       --            mdm.opnl_service_id,
       --            mdm.opnl_service_name,
       --            mdm.corp_service_line_id,
       --            mdm.corp_service_line,
       --            mdm.hs_area_id,
       --            mdm.hs_area_name,
       --            physsvc.hs_area_id AS prov_hs_area_id,
       --            physsvc.hs_area_name AS prov_hs_area_name,
       --            --Select
       --            ser.sk_Dim_Physcn,
       --            doc.UVaID,
	      --         appts.SER_RPT_GRP_SIX AS financial_division,
	      --         appts.SER_RPT_GRP_EIGHT AS financial_subdivision,
				   --mdmloc.LOC_ID AS rev_location_id,
				   --mdmloc.REV_LOC_NAME AS rev_location,				   
                   -- SOM
				   --physcn.Clrt_Financial_Division AS financial_division_id,
				   --physcn.Clrt_Financial_Division_Name AS financial_division_name,
				   --physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id,
				   --physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name,
				   --physcn.SOM_Group_ID AS som_group_id,
				   --physcn.SOM_group AS som_group_name,
				   --physcn.SOM_department_id AS som_department_id,
				   --physcn.SOM_department AS	som_department_name,
				   --physcn.SOM_division_5 AS	som_division_id,
				   --physcn.SOM_division_name AS som_division_name,
				   --physcn.som_hs_area_id AS	som_hs_area_id,
				   --physcn.som_hs_area_name AS som_hs_area_name,
				   --appts.BILL_PROV_YN

			INTO #main

            FROM Stage.Scheduled_Appointment AS appts
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
                    ON ser.PROV_ID = appts.PROV_ID
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                    ON appts.DEPARTMENT_ID = mdm.epic_department_id
                LEFT OUTER JOIN
                (
                    SELECT DISTINCT
                        EPIC_DEPARTMENT_ID,
                        SERVICE_LINE,
                        PFA_POD,
                        HUB,
						BUSINESS_UNIT,
						LOC_ID,
						REV_LOC_NAME
                    FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
                ) AS mdmloc
                    ON appts.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID
                LEFT JOIN
                (
                    SELECT sk_Dim_Physcn,
                           UVaID,
                           Service_Line
                    FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
                    WHERE current_flag = 1
                ) AS doc
                    ON ser.sk_Dim_Physcn = doc.sk_Dim_Physcn
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc
                    ON physsvc.Physician_Roster_Name = CASE
                                                           WHEN (ser.sk_Dim_Physcn > 0) THEN
                                                               doc.Service_Line
                                                           ELSE
                                                               'No Value Specified'
                                                       END

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    --ON physcn.sk_Dim_Physcn = doc.sk_Dim_Physcn
				    ON physcn.PROV_ID = appts.PROV_ID
				WHERE appts.APPT_DTTM >= '7/1/2019 00:00 AM' AND appts.APPT_DTTM <= '12/9/2019 11:59 PM'

			
			--ORDER BY
			--	   ser.Staff_Resource,
			--	   ser.Prov_Typ,
   --                provider_id,
   --                pod_id,
   --                hub_id,
   --                epic_department_id		
			--ORDER BY
			--	   CASE WHEN ISNULL(COALESCE(physcn.Clrt_Financial_Division_Name, appts.SER_RPT_GRP_SIX, NULL),'No') <> 'No' THEN 'Yes' ELSE 'No' END,
			--	   ser.Staff_Resource,
			--	   ser.Prov_Typ,
   --                epic_department_id,
   --                provider_id,
   --                pod_id,
   --                hub_id		
			--ORDER BY
			--	   CASE WHEN ISNULL(COALESCE(physcn.Clrt_Financial_Division_Name, NULL),'No') <> 'No' THEN 'Yes' ELSE 'No' END,
			--	   ser.Staff_Resource,
			--	   ser.Prov_Typ,
   --                epic_department_id,
   --                provider_id,
   --                pod_id,
   --                hub_id
			--ORDER BY
			--	   CASE WHEN ISNULL(COALESCE(physcn.Clrt_Financial_Division_Name, NULL),'No') <> 'No' THEN 'Yes' ELSE 'No' END,
			--	   ser.Staff_Resource,
			--	   ser.Prov_Typ,
   --                mdm.epic_department_name,
   --                provider_name

   --SELECT Staff_Resource
   --       , Prov_Typ
		 -- , epic_department_name
		 -- , SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) AS No_Clrt_Financial_Division_Name
		 -- , SUM(CASE WHEN est_financial_division_name = 'Yes' THEN 1 ELSE 0 END) AS Clrt_Financial_Division_Name
   --FROM #main
   --GROUP BY Staff_Resource
   --       , Prov_Typ
		 -- , epic_department_name

   --SELECT Staff_Resource
   --       , Prov_Typ
		 -- --, SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) AS No_Clrt_Financial_Division_Name
		 -- --, SUM(CASE WHEN est_financial_division_name = 'Yes' THEN 1 ELSE 0 END) AS Clrt_Financial_Division_Name
		 -- , SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) AS No_Financial_Division_Name
		 -- , SUM(CASE WHEN est_financial_division_name = 'Yes' THEN 1 ELSE 0 END) AS Financial_Division_Name
   --FROM #main
   --GROUP BY Staff_Resource
   --       , Prov_Typ
   --ORDER BY SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) DESC
   --       , Staff_Resource
   --       , Prov_Typ

   --SELECT Staff_Resource
   --       , Prov_Typ
		 -- , provider_name
		 -- , epic_department_name
		 -- --, SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) AS No_Clrt_Financial_Division_Name
		 -- --, SUM(CASE WHEN est_financial_division_name = 'Yes' THEN 1 ELSE 0 END) AS Clrt_Financial_Division_Name
		 -- , SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) AS No_Financial_Division_Name
		 -- , SUM(CASE WHEN est_financial_division_name = 'Yes' THEN 1 ELSE 0 END) AS Financial_Division_Name
   --FROM #main
   --GROUP BY Staff_Resource
   --       , Prov_Typ
		 -- , provider_name
		 -- , epic_department_name
   ----ORDER BY SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) DESC
   ----       , Staff_Resource
   ----       , Prov_Typ
   --ORDER BY Staff_Resource
   --       , Prov_Typ
		 -- , provider_name
		 -- , epic_department_name

   SELECT   epic_department_name
          , Staff_Resource
          , Prov_Typ
		  , provider_name
		  --, SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) AS No_Clrt_Financial_Division_Name
		  --, SUM(CASE WHEN est_financial_division_name = 'Yes' THEN 1 ELSE 0 END) AS Clrt_Financial_Division_Name
		  , SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) AS No_Financial_Division_Name
		  , SUM(CASE WHEN est_financial_division_name = 'Yes' THEN 1 ELSE 0 END) AS Financial_Division_Name
   FROM #main
   WHERE epic_department_name IS NOT NULL
   GROUP BY epic_department_name
          , Staff_Resource
          , Prov_Typ
		  , provider_name
   --ORDER BY SUM(CASE WHEN est_financial_division_name = 'No' THEN 1 ELSE 0 END) DESC
   --       , Staff_Resource
   --       , Prov_Typ
   ORDER BY epic_department_name
          , Staff_Resource
          , Prov_Typ
		  , provider_name
