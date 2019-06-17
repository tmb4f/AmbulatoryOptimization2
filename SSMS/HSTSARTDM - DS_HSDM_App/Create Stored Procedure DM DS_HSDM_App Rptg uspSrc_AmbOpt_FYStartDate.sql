USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_FYStartDate]
AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_FYStartDate
--WHO : Tom Burgan
--WHEN: 6/14/19
--WHY : Report dataset for Start Date parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:		DS_HSDW_Prod.Rptg.vwDim_Date
--                
--      OUTPUTS:	[Rptg].[uspSrc_AmbOpt_FYStartDate]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/14/2019 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------

SELECT CAST(CASE WHEN MONTH(DATEADD(DAY, - 1, CAST(month_begin_date AS DATE))) = 6 THEN CONVERT(DATE, '7/1/' + CONVERT(VARCHAR(4),Fyear_num-2),101)
                 ELSE CONVERT(DATE, '7/1/' + CONVERT(VARCHAR(4),Fyear_num-1),101)
		    END AS SMALLDATETIME) AS FYStartDate
	FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte
	WHERE (CONVERT(VARCHAR(10), GETDATE(), 101) = CONVERT(VARCHAR(10), ddte.day_date, 101))

GO


