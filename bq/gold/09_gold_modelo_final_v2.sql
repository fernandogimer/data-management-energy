-- ===================================================================================== --
-- Script: 09_gold_modelo_final_v2 (Enriquecido con Fiestas de Barrio)
-- ===================================================================================== --
CREATE OR REPLACE TABLE `datamanagementbi.gold_data.modelo_final_v2` AS
SELECT
  -- Seleccionamos todas las columnas del modelo original
  m.*,
  -- Añadimos la nueva característica: una bandera que indica si hay fiesta de barrio
  CASE WHEN fb.id_fiesta_dia IS NOT NULL THEN TRUE ELSE FALSE END AS es_fiesta_barrio,
  fb.nombre_fiesta
FROM
  `datamanagementbi.gold_data.modelo_final` AS m
LEFT JOIN
  `datamanagementbi.silver_data.dim_fiestas_barrio` AS fb
  ON m.fecha = fb.fecha AND m.id_geografia = fb.id_geografia;