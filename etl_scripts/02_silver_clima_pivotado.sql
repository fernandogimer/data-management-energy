-- ================================================================================= --
-- Script: (SILVER) - Limpieza y Pivotado de Datos Climáticos
-- Descripción: Transforma los datos brutos de clima de formato largo a ancho,
--              limpiando tipos de datos y manteniendo la granularidad de 30 min.
-- Capa de Origen: bronze_data
-- Capa de Destino: silver_data
-- Tabla de Destino: clima_pivotado_30min
-- ================================================================================= --

CREATE OR REPLACE TABLE `datamanagementbi.silver_data.clima_pivotado_30min` AS (

  -- CTE para limpiar los tipos de datos de la fuente Bronze.
  WITH clima_limpio_cte AS (
    SELECT
      CAST(data_lectura AS TIMESTAMP) AS timestamp_lectura,
      codi_estacio,
      nom_estacio,
      nom_variable,
      -- Usamos SAFE_CAST para convertir a número de forma segura,
      -- devolviendo NULL si encuentra un valor no numérico (ej. 'V')
      SAFE_CAST(valor_lectura AS FLOAT64) AS valor_numerico
    FROM
      `datamanagementbi.bronze_data.raw_clima_historico`
  )

  -- Pivotar los datos limpios de la CTE.
  SELECT
    timestamp_lectura,
    codi_estacio,
    -- Como nom_estacio es el mismo para cada codi_estacio, usamos MAX para seleccionarlo.
    MAX(nom_estacio) AS nom_estacio,
    MAX(CASE WHEN nom_variable = 'Temperatura' THEN valor_numerico ELSE NULL END) AS temperatura,
    MAX(CASE WHEN nom_variable = 'Humitat relativa' THEN valor_numerico ELSE NULL END) AS humedad_relativa,
    MAX(CASE WHEN nom_variable = 'Precipitació' THEN valor_numerico ELSE NULL END) AS precipitacion,
    MAX(CASE WHEN nom_variable = 'Irradiància solar global' THEN valor_numerico ELSE NULL END) AS irradiancia_solar
    -- Añade aquí más variables si las necesitas, siguiendo el mismo patrón.
  FROM
    clima_limpio_cte
  GROUP BY
    timestamp_lectura,
    codi_estacio
);