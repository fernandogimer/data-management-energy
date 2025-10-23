-- Consulta: 06_silver_fact_consumo_horario
-- Crea la tabla: silver_data.fact_consumo_horario
CREATE OR REPLACE TABLE `datamanagementbi.silver_data.fact_consumo_horario` AS
WITH
consumo_unificado AS (
  SELECT Data, Codi_Postal, Tram_Horari, Sector_Economic, Valor FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2021` UNION ALL
  SELECT Data, Codi_Postal, Tram_Horari, Sector_Economic, Valor FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2022` UNION ALL
  SELECT Data, Codi_Postal, Tram_Horari, Sector_Economic, Valor FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2023` UNION ALL
  SELECT Data, Codi_Postal, Tram_Horari, Sector_Economic, Valor FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2024` UNION ALL
  SELECT Data, Codi_Postal, Tram_Horari, Sector_Economic, Valor FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2025`
)
SELECT
  c.Data AS fecha,
  g.id_geografia,
  s.id_sector_economico,
  t.id_tramo_horario,
  CAST(c.Valor * 1000 AS FLOAT64) AS consumo_kwh
FROM
  consumo_unificado c
JOIN `datamanagementbi.silver_data.dim_geografia` g ON LPAD(CAST(c.Codi_Postal AS STRING), 5, '0') = g.id_geografia
JOIN `datamanagementbi.silver_data.dim_sector_economico` s ON c.Sector_Economic = s.nombre_sector
JOIN `datamanagementbi.silver_data.dim_tramo_horario` t ON c.Tram_Horari = t.descripcion_tramo
WHERE c.Valor > 0;