-- Consulta: 01_silver_dim_calendario
-- Crea la tabla: silver_data.dim_calendario
CREATE OR REPLACE TABLE `datamanagementbi.silver_data.dim_calendario` AS
WITH
dates_from_consumo AS (
  SELECT DISTINCT Data AS fecha FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2021` UNION ALL
  SELECT DISTINCT Data AS fecha FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2022` UNION ALL
  SELECT DISTINCT Data AS fecha FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2023` UNION ALL
  SELECT DISTINCT Data AS fecha FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2024` UNION ALL
  SELECT DISTINCT Data AS fecha FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2025`
),
dates_from_clima AS (
  SELECT DISTINCT DATE(TIMESTAMP(data_lectura)) AS fecha FROM `datamanagementbi.bronze_data.raw_clima_historico`
),
dates_from_festivos AS (
  SELECT DISTINCT Fecha AS fecha FROM `datamanagementbi.bronze_data.raw_festivos_barcelona`
),
all_dates AS (
  SELECT fecha FROM dates_from_consumo WHERE fecha IS NOT NULL UNION ALL
  SELECT fecha FROM dates_from_clima WHERE fecha IS NOT NULL UNION ALL
  SELECT fecha FROM dates_from_festivos WHERE fecha IS NOT NULL
),
date_range AS (
  SELECT MIN(fecha) AS min_date, MAX(fecha) AS max_date FROM all_dates
),
generated_dates AS (
  SELECT calendar_date
  FROM date_range,
       UNNEST(GENERATE_DATE_ARRAY(date_range.min_date, date_range.max_date, INTERVAL 1 DAY)) AS calendar_date
)
SELECT
  g.calendar_date AS fecha,
  EXTRACT(YEAR FROM g.calendar_date) AS anio,
  EXTRACT(MONTH FROM g.calendar_date) AS mes,
  EXTRACT(DAY FROM g.calendar_date) AS dia_del_mes,
  CASE EXTRACT(DAYOFWEEK FROM g.calendar_date)
    WHEN 1 THEN 'domingo' WHEN 2 THEN 'lunes' WHEN 3 THEN 'martes'
    WHEN 4 THEN 'miércoles' WHEN 5 THEN 'jueves' WHEN 6 THEN 'viernes'
    WHEN 7 THEN 'sábado'
  END AS dia_de_la_semana_nombre,
  EXTRACT(DAYOFWEEK FROM g.calendar_date) IN (1, 7) AS es_fin_de_semana,
  f.Fecha IS NOT NULL AS es_festivo,
  f.`Descripción` AS festivo_descripcion,
  f.`Ámbito` AS festivo_ambito
FROM
  generated_dates g
LEFT JOIN
  `datamanagementbi.bronze_data.raw_festivos_barcelona` f
  ON g.calendar_date = f.Fecha
ORDER BY
  fecha;