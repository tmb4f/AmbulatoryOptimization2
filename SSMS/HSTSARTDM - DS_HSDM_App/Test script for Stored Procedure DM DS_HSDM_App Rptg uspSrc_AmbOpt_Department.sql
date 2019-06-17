USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME,
        @EndDate SMALLDATETIME,
        @in_servLine VARCHAR(MAX),
	    @in_pods VARCHAR(MAX),
	    @in_hubs VARCHAR(MAX)

--SET @StartDate = NULL
--SET @EndDate = NULL
--SET @StartDate = '2/3/2019 00:00 AM'
--SET @EndDate = '2/9/2019 11:59 PM'
--SET @StartDate = '5/27/2019 00:00 AM'
--SET @EndDate = '6/3/2019 11:59 PM'
SET @StartDate = '5/1/2019 00:00 AM'
SET @EndDate = '5/31/2019 00:00 AM'

DECLARE @ServiceLine TABLE (ServiceLineId SMALLINT)

INSERT INTO @ServiceLine
(
    ServiceLineId
)
VALUES
--(1),--Digestive Health
--(2),--Heart and Vascular
--(3),--Medical Subspecialties
--(4),--Musculoskeletal
--(5),--Neurosciences and Behavioral Health
--(6),--Oncology
--(7),--Ophthalmology
--(8),--Primary Care
--(9),--Surgical Subspecialties
--(10),--Transplant
--(11) --Womens and Childrens
(0)  --(All)
--(1) --Digestive Health
--(1),--Digestive Health
--(2) --Heart and Vascular

SELECT @in_servLine = COALESCE(@in_servLine+',' ,'') + CAST(ServiceLineId AS VARCHAR(MAX))
FROM @ServiceLine

--SELECT @in_servLine

DECLARE @Pod TABLE (PodId VARCHAR(100))

INSERT INTO @Pod
(
    PodId
)
VALUES
--('1'),--Cancer
--('2'),--Musculoskeletal
--('3'),--Primary Care
--('4'),--Surgical Procedural Specialties
--('5'),--Transplant
--('6'),--Medical Specialties
--('7'),--Radiology
--('8'),--Heart and Vascular Center
--('9'),--Neurosciences and Psychiatry
--('10'),--Women's and Children's
--('12'),--CPG
--('13'),--UVA Community Cancer POD
--('14'),--Digestive Health
--('15'),--Ophthalmology
--('16') --Community Medicine
--('0') --(All)
--('1') --Cancer
--('1'), --Cancer
--('10') --Women's and Children's
('14')--,--Digestive Health
;

SELECT @in_pods = COALESCE(@in_pods+',' ,'') + CAST(PodId AS VARCHAR(MAX))
FROM @Pod

--SELECT @in_pods

DECLARE @Hub TABLE (HubId VARCHAR(66))

INSERT INTO @Hub
(
    HubId
)
VALUES
--('2'),--Non-Hub registration
--('3'),--ECCC
--('4'),--415 Fontaine
--('5'),--Battle
--('6'),--JPA
--('7'),--Northridge
--('8'),--PCC
--('9'),--West Complex
--('10') --500 Fontaine
('0') --(All)
--('3'),--ECCC
--('10') --500 Fontaine
;

SELECT @in_hubs = COALESCE(@in_hubs+',' ,'') + CAST(HubId AS VARCHAR(MAX))
FROM @Hub

--SELECT @in_hubs

--ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_Department]
--    (
--     @StartDate SMALLDATETIME = NULL,
--     @EndDate SMALLDATETIME = NULL,
--     @in_servLine VARCHAR(MAX),
--	 @in_pods VARCHAR(MAX),
--	 @in_hubs VARCHAR(MAX)
--    )
--AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_Department
--WHO : Tom Burgan
--WHEN: 6/13/19
--WHY : Report dataset for Epic department parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_Department]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/13/2019 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------
DECLARE @locStartDate SMALLDATETIME,
        @locEndDate SMALLDATETIME

SET @locStartDate = CAST(CAST(@StartDate AS DATE) AS SMALLDATETIME) + CAST(CAST('00:00:00' AS TIME) AS SMALLDATETIME)
SET @locEndDate   = CAST(DATEADD(MINUTE,-1,CAST((DATEADD(DAY,1,CAST(@EndDate AS DATE))) AS SMALLDATETIME)) AS SMALLDATETIME)

DECLARE @tab_servLine TABLE
(
    Service_Line_Id int
);
INSERT INTO @tab_servLine
SELECT Param
FROM ETL.fn_ParmParse(@in_servLine, ',');
DECLARE @tab_pods TABLE
(
    pod_id VARCHAR(66)
);
INSERT INTO @tab_pods
SELECT Param
FROM ETL.fn_ParmParse(@in_pods, ',');
DECLARE @tab_hubs TABLE
(
    hub_id VARCHAR(66)
);
INSERT INTO @tab_hubs
SELECT Param
FROM ETL.fn_ParmParse(@in_hubs, ',');
SELECT     0 AS DEPARTMENT_ID, '(All)' AS DEPARTMENT_NAME 
UNION	  
SELECT DISTINCT
	appts.DEPARTMENT_ID DEPARTMENT_ID,
	mdm.epic_department_name_external DEPARTMENT_NAME
FROM Stage.Scheduled_Appointment AS appts
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
ON appts.DEPARTMENT_ID = mdm.epic_department_id
LEFT OUTER JOIN
(
    SELECT DISTINCT
		EPIC_DEPARTMENT_ID,
	    HUB
	FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
) AS mdmloc
ON appts.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID
WHERE (appts.APPT_DTTM BETWEEN @locStartDate AND @locEndDate)
      AND
      (
          @in_servLine = 0
          OR (COALESCE(mdm.service_line_id, mdm.opnl_service_id) IN (SELECT Service_Line_Id FROM @tab_servLine))
			 
      )
      AND
      (
          @in_pods = 0
          OR (appts.RPT_GRP_SIX IN (SELECT pod_id FROM @tab_pods))
      )
      AND
      (
          @in_hubs = 0
          OR (appts.RPT_GRP_SEVEN IN (SELECT hub_id FROM @tab_hubs))
      )
      AND appts.DEPARTMENT_ID IS NOT NULL AND mdm.epic_department_name_external IS NOT NULL
ORDER BY DEPARTMENT_NAME

GO


