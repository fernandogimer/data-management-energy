-- Consulta: 03_silver_fact_poblacion_anual
-- Crea la tabla: silver_data.fact_poblacion_anual
CREATE OR REPLACE TABLE `datamanagementbi.silver_data.fact_poblacion_anual` AS
SELECT
  p.anio,
  g.id_geografia,
  p.poblacion
FROM
  `datamanagementbi.bronze_data.raw_poblacion_anual_barrio` AS p
INNER JOIN
  `datamanagementbi.silver_data.dim_geografia` AS g ON p.nombre_barrio = g.nombre_barrio
WHERE
  p.anio IS NOT NULL AND p.poblacion IS NOT NULL
ORDER BY
  g.id_geografia, p.anio;