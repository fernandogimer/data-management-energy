-- Consulta: 02_silver_dim_geografia
-- Crea la tabla: silver_data.dim_geografia
CREATE OR REPLACE TABLE `datamanagementbi.silver_data.dim_geografia` AS
WITH
municipio_mapping AS (
  SELECT DISTINCT
    Codigo_Postal,
    Municipio AS nombre_municipio
  FROM `datamanagementbi.bronze_data.raw_densidad_poblacion`
)
SELECT
  LPAD(CAST(g.codigo_postal AS STRING), 5, '0') AS id_geografia,
  LPAD(CAST(g.codigo_postal AS STRING), 5, '0') AS codigo_postal,
  g.nombre_barrio,
  g.nombre_distrito,
  m.nombre_municipio
FROM
  `datamanagementbi.bronze_data.raw_cod_postal_barrio_distrito` AS g
LEFT JOIN
  municipio_mapping AS m ON g.codigo_postal = m.Codigo_Postal
WHERE
  g.codigo_postal IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
ORDER BY id_geografia;