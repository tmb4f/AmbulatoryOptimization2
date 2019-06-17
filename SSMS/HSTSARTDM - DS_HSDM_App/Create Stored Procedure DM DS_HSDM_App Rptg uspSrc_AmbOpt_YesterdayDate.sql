USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Rptg].[uspSrc_AmbOpt_YesterdayDate]
AS  
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_YesterdayDate
--WHO : Tom Burgan
--WHEN: 6/14/19
--WHY : Report dataset for Start Date parameter.
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:		DS_HSDW_Prod.Rptg.vwDim_Date
--                
--      OUTPUTS:	[Rptg].[uspSrc_AmbOpt_YesterdayDate]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/14/2019 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------

SELECT CAST(CAST(DATEADD(MINUTE,-1,CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME)) AS DATE) AS SMALLDATETIME) AS YesterdayDate

GO


