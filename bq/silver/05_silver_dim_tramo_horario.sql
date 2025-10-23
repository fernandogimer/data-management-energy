-- Consulta: 05_silver_dim_tramo_horario
-- Crea la tabla: silver_data.dim_tramo_horario
CREATE OR REPLACE TABLE `datamanagementbi.silver_data.dim_tramo_horario` AS
SELECT
  ROW_NUMBER() OVER(ORDER BY Tram_Horari) AS id_tramo_horario,
  Tram_Horari AS descripcion_tramo,
  PARSE_TIME('%H:%M:%S', SUBSTR(Tram_Horari, 4, 8)) as hora_inicio,
  PARSE_TIME('%H:%M:%S', SUBSTR(Tram_Horari, 15, 8)) as hora_fin
FROM (
  SELECT DISTINCT Tram_Horari
  FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2024`
  WHERE Tram_Horari != 'No consta'
);