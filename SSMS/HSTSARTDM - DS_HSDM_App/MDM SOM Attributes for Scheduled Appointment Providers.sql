USE [DS_HSDM_App]
GO

DECLARE @startdate SMALLDATETIME
DECLARE @enddate SMALLDATETIME
 
	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

            SELECT DISTINCT
                   appts.DEPARTMENT_ID AS epic_department_id,
                   mdm.epic_department_name AS epic_department_name,
                   mdm.epic_department_name_external AS epic_department_name_external,
                   appts.PROV_ID AS provider_id,
                   appts.PROV_NAME AS provider_name,
                   ser.sk_Dim_Physcn,
				   appts.RPT_GRP_THIRTY AS epic_service_line,
                   mdmloc.SERVICE_LINE AS mdmloc_service_line,
                   appts.RPT_GRP_SIX AS epic_pod,
                   mdmloc.PFA_POD AS mdmloc_pod,
                   appts.RPT_GRP_SEVEN AS epic_hub,
                   mdmloc.HUB AS mdmloc_hub,
                   -- MDM
                   mdm.service_line_id,
                   mdm.service_line,
                   mdm.sub_service_line_id,
                   mdm.sub_service_line,
                   mdm.opnl_service_id,
                   mdm.opnl_service_name,
                   mdm.corp_service_line_id,
                   mdm.corp_service_line,
                   mdm.hs_area_id,
                   mdm.hs_area_name,
				   mdmloc.LOC_ID AS rev_location_id,
				   mdmloc.REV_LOC_NAME AS rev_location,				   
                   -- SOM
				   physcn.Clrt_Financial_Division AS financial_division_id,
				   physcn.Clrt_Financial_Division_Name AS financial_division_name,
				   physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id,
				   physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name,
				   physcn.SOM_Group_ID AS som_group_id,
				   physcn.SOM_group AS som_group_name,
				   physcn.SOM_department_id AS som_department_id,
				   physcn.SOM_department AS	som_department_name,
				   physcn.SOM_division_5 AS	som_division_id,
				   physcn.SOM_division_name AS som_division_name,
				   physcn.som_hs_area_id AS	som_hs_area_id,
				   physcn.som_hs_area_name AS som_hs_area_name

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

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = doc.sk_Dim_Physcn

            WHERE (appts.APPT_DT >= @locstartdate
              AND appts.APPT_DT < @locenddate)

ORDER BY appts.DEPARTMENT_ID
       , appts.PROV_ID;

GO


