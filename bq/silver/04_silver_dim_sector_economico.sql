-- Consulta: 04_silver_dim_sector_economico
-- Crea la tabla: silver_data.dim_sector_economico
CREATE OR REPLACE TABLE `datamanagementbi.silver_data.dim_sector_economico` AS
SELECT
  ROW_NUMBER() OVER() AS id_sector_economico,
  Sector_Economic AS nombre_sector
FROM (
  SELECT DISTINCT Sector_Economic
  FROM `datamanagementbi.bronze_data.raw_consumo_electrico_2024`
  WHERE Sector_Economic IS NOT NULL
);