-- ===================================================================================== --
-- Script: 08_silver_dim_fiestas_barrio (Versión Unificada)
-- Descripción: Unifica y limpia los eventos de FiestasBarrio y del histórico de eventos
--              para crear una única dimensión de eventos.
-- ===================================================================================== --

CREATE OR REPLACE TABLE `datamanagementbi.silver_data.dim_fiestas_barrio` AS

WITH
-- CTE 1: Extraer y limpiar eventos de la tabla FiestasBarrio
eventos_fiestas_barrio AS (
  SELECT
    Fiesta AS nombre_evento,
    Fecha_inicio AS fecha_inicio,
    Fecha_fin AS fecha_fin,
    LPAD(CAST(Codigo_Postal AS STRING), 5, '0') AS codigo_postal
  FROM `datamanagementbi.bronze_data.FiestasBarrio`
  WHERE Fecha_inicio IS NOT NULL AND Fecha_fin IS NOT NULL
),

-- CTE 2: Extraer y limpiar eventos de la tabla raw_historico_eventos
eventos_historicos AS (
  SELECT
    title AS nombre_evento,
    CAST(firstdate_begin AS DATE) AS fecha_inicio,
    CAST(firstdate_end AS DATE) AS fecha_fin,
    location_postalcode AS codigo_postal
  FROM `datamanagementbi.bronze_data.raw_historico_eventos`
  WHERE location_city = 'Barcelona'
    AND firstdate_begin IS NOT NULL
    AND firstdate_end IS NOT NULL
    AND location_postalcode IS NOT NULL
),

-- CTE 3: Unificar ambas fuentes de eventos
eventos_unificados AS (
  SELECT * FROM eventos_fiestas_barrio
  UNION ALL
  SELECT * FROM eventos_historicos
),

-- CTE 4: Unir con la dimensión geográfica para obtener la clave foránea
eventos_con_id_geo AS (
  SELECT
    e.nombre_evento,
    e.fecha_inicio,
    e.fecha_fin,
    g.id_geografia
  FROM eventos_unificados AS e
  JOIN `datamanagementbi.silver_data.dim_geografia` AS g
    ON e.codigo_postal = g.codigo_postal -- El CP ya está estandarizado en ambas CTEs
),

-- CTE 5: Expandir los rangos de fechas para tener una fila por cada día de evento
dias_de_evento AS (
  SELECT
    nombre_evento,
    id_geografia,
    dia_evento
  FROM eventos_con_id_geo,
       UNNEST(GENERATE_DATE_ARRAY(fecha_inicio, fecha_fin)) AS dia_evento
)

-- CONSULTA FINAL: Crear la tabla de dimensión final
SELECT
  FARM_FINGERPRINT(CONCAT(CAST(dia_evento AS STRING), id_geografia, nombre_evento)) AS id_fiesta_dia,
  dia_evento AS fecha,
  id_geografia,
  nombre_evento
FROM dias_de_evento;