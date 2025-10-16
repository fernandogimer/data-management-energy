-- ================================================================================= --
-- Script: (GOLD) - Agregación Diaria y Feature Engineering
-- Descripción: Agrega los datos climáticos a nivel diario y crea métricas
--              agregadas para toda la ciudad, listas para el análisis.
-- Capa de Origen: silver_data
-- Capa de Destino: gold_data
-- Tabla de Destino: clima_diario_barcelona
-- ================================================================================= --

CREATE OR REPLACE TABLE `datamanagementbi.gold_data.clima_diario_barcelona` AS (

  -- Paso 3.1: CTE para agregar los datos de 30 min a nivel diario POR ESTACIÓN.
  WITH agregacion_diaria_por_estacion_cte AS (
    SELECT
      CAST(timestamp_lectura AS DATE) AS fecha_dia,
      codi_estacio,
      AVG(temperatura) AS temp_media,
      MAX(temperatura) AS temp_max,
      MIN(temperatura) AS temp_min,
      SUM(precipitacion) AS precipitacion_total,
      AVG(humedad_relativa) AS humedad_media,
      AVG(irradiancia_solar) AS irradiancia_media
    FROM
      `datamanagementbi.silver_data.clima_pivotado_30min`
    GROUP BY
      fecha_dia,
      codi_estacio
  )

  -- Paso 3.2: Agregación final a nivel CIUDAD y creación de Features.
  SELECT
    fecha_dia,
    -- Agregamos las métricas de las estaciones para obtener un valor para toda la ciudad
    AVG(temp_media) AS temp_media_bcn,
    MAX(temp_max) AS temp_max_bcn,
    MIN(temp_min) AS temp_min_bcn,
    SUM(precipitacion_total) AS precipitacion_total_bcn,
    AVG(humedad_media) AS humedad_media_bcn,
    AVG(irradiancia_media) AS irradiancia_media_bcn,

    -- Feature Engineering: Intensidad de la Isla de Calor Urbana (UHI)
    -- Compara la temperatura media del centro (Raval, X4) vs. la periferia (Zona Universitària, D5)
    MAX(CASE WHEN codi_estacio = 'X4' THEN temp_media ELSE NULL END) -
    MAX(CASE WHEN codi_estacio = 'D5' THEN temp_media ELSE NULL END) AS uhi_intensidad_c

  FROM
    agregacion_diaria_por_estacion_cte
  GROUP BY
    fecha_dia
  ORDER BY
    fecha_dia
);