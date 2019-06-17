SELECT DISTINCT
       [w_som_department_id]
      ,[w_som_department_name]
      ,[w_som_division_id]
      ,[w_som_division_name]
  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  WHERE w_som_department_id IS NOT NULL
  ORDER BY w_som_department_name
         , w_som_division_name