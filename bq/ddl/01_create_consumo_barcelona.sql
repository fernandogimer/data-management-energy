-- Unifica 2022–2025 en una sola tabla particionada por fecha (BigQuery)
-- Dataset destino: neat-tangent-472516-v8.Data_Management
CREATE OR REPLACE TABLE `neat-tangent-472516-v8.Data_Management.consumo_barcelona`
PARTITION BY fecha
CLUSTER BY codigo_postal, sector_economico
AS
WITH base AS (
  SELECT
    DATE(`Data`)                                         AS fecha,
    LPAD(CAST(`Codi_Postal` AS STRING), 5, '0')          AS codigo_postal,
    TRIM(`Sector_Economic`)                              AS sector_economico,
    TRIM(`Tram_Horari`)                                  AS tramo_horario,
    SAFE_CAST(`Valor` AS FLOAT64)                        AS kwh,
    CAST(`Any` AS STRING)                                AS anio_origen,
    CURRENT_TIMESTAMP()                                  AS load_ts
  FROM `neat-tangent-472516-v8.Data_Management.2022_consumo_electrico_BCN`

  UNION ALL
  SELECT
    DATE(`Data`),
    LPAD(CAST(`Codi_Postal` AS STRING), 5, '0'),
    TRIM(`Sector_Economic`),
    TRIM(`Tram_Horari`),
    SAFE_CAST(`Valor` AS FLOAT64),
    CAST(`Any` AS STRING),
    CURRENT_TIMESTAMP()
  FROM `neat-tangent-472516-v8.Data_Management.2023_consumo_electrico_BCN`

  UNION ALL
  SELECT
    DATE(`Data`),
    LPAD(CAST(`Codi_Postal` AS STRING), 5, '0'),
    TRIM(`Sector_Economic`),
    TRIM(`Tram_Horari`),
    SAFE_CAST(`Valor` AS FLOAT64),
    CAST(`Any` AS STRING),
    CURRENT_TIMESTAMP()
  FROM `neat-tangent-472516-v8.Data_Management.2024_consumo_electrico_BCN`

  UNION ALL
  SELECT
    DATE(`Data`),
    LPAD(CAST(`Codi_Postal` AS STRING), 5, '0'),
    TRIM(`Sector_Economic`),
    TRIM(`Tram_Horari`),
    SAFE_CAST(`Valor` AS FLOAT64),
    CAST(`Any` AS STRING),
    CURRENT_TIMESTAMP()
  FROM `neat-tangent-472516-v8.Data_Management.2025_consumo_electrico_BCN`
)
SELECT * FROM base;
