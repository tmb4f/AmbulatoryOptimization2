USE CLARITY_App

DECLARE @Provider TABLE (ProviderId VARCHAR(18))

INSERT INTO @Provider
(
    ProviderId
)
VALUES
 --('28813') -- FISHER, JOSEPH D
 --('1300563') -- ARTH INF
 --('41806') -- NORTHRIDGE DEXA
 --('1301100') -- CT6
 --('82262') -- CT APPOINTMENT ERC
 --('40758') -- PAYNE, PATRICIA
 --('73571') -- LEEDS, JOSEPH THOMAS
 --,('29303') -- KALANTARI, KAMBIZ
 --('73725') -- ROSS, BUERLEIN
 --('41013') -- MANN, JAMES A
 --('85744') -- CORBETT, SUSAN
 ('581926') -- CHARLTON, NATHAN P
;
					SELECT DISTINCT
					    wd.sk_Dim_Physcn,
						wd.PROV_ID,
						ser.PROV_NAME,
						ser.PROV_TYPE,
						ser.PROV_ABBR,
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
					LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser
					ON ser.PROV_ID = wd.PROV_ID
					WHERE wd.SOMSeq = 1
                          AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = wd.PROV_ID)

					SELECT *
					FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
					WHERE EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = cwlk.PROV_ID)