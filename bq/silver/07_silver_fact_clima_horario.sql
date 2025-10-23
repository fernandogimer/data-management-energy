-- Consulta: 07_silver_fact_clima_horario
-- Crea la tabla: silver_data.fact_clima_horario
CREATE OR REPLACE TABLE `datamanagementbi.silver_data.fact_clima_horario` AS
WITH
clima_formateado AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP(data_lectura), HOUR) AS fecha_hora,
    codi_estacio,
    codi_variable,
    SAFE_CAST(valor_lectura AS FLOAT64) AS valor
  FROM `datamanagementbi.bronze_data.raw_clima_historico`
  WHERE SAFE_CAST(valor_lectura AS FLOAT64) IS NOT NULL
),
clima_pivotado_por_estacion AS (
  SELECT
    fecha_hora,
    codi_estacio,
    AVG(CASE WHEN codi_variable = '32' THEN valor END) AS temperatura,
    AVG(CASE WHEN codi_variable = '33' THEN valor END) AS humedad_relativa,
    SUM(CASE WHEN codi_variable = '35' THEN valor END) AS precipitacion,
    AVG(CASE WHEN codi_variable = '30' THEN valor END) AS velocidad_viento,
    AVG(CASE WHEN codi_variable = '36' THEN valor END) AS irradiancia_solar
  FROM clima_formateado
  GROUP BY fecha_hora, codi_estacio
)
SELECT
  DATE(fecha_hora) AS fecha,
  t.id_tramo_horario,
  AVG(temperatura) AS temperatura_media_ciudad,
  AVG(humedad_relativa) AS humedad_media_ciudad,
  SUM(precipitacion) AS precipitacion_total_ciudad,
  AVG(CASE WHEN codi_estacio = 'X4' THEN temperatura END) AS temp_raval,
  AVG(CASE WHEN codi_estacio = 'X8' THEN temperatura END) AS temp_zuniversitaria,
  AVG(CASE WHEN codi_estacio = 'D5' THEN temperatura END) AS temp_fabra,
  AVG(CASE WHEN codi_estacio = 'D5' THEN temperatura END) - AVG(CASE WHEN codi_estacio = 'X4' THEN temperatura END) AS temp_spread_montana_centro
FROM clima_pivotado_por_estacion
-- Unimos con la dim_tramo_horario para obtener el ID correcto
JOIN `datamanagementbi.silver_data.dim_tramo_horario` t
  ON PARSE_TIME('%H:%M:%S', FORMAT_TIME('%T', TIME(fecha_hora))) BETWEEN t.hora_inicio AND t.hora_fin
GROUP BY fecha, id_tramo_horario
HAVING id_tramo_horario IS NOT NULL
ORDER BY fecha, id_tramo_horario;