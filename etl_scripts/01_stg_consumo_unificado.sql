-- Script 1: Crea la tabla de staging para el consumo unificado y limpio.
CREATE OR REPLACE TABLE `datamanagementbi.silver_data.stg_consumo_unificado` AS (
  SELECT
    CAST(Data AS DATE) AS fecha,
    CAST(Codi_Postal AS INT64) AS codigo_postal,
    TRIM(Sector_Economic) AS sector_economico,
    TRIM(Tram_Horari) AS tramo_horario,
    CAST(Valor AS INT64) AS consumo_kwh
  FROM
    `datamanagementbi.bronze_data.raw_consumo_electrico_*`
);