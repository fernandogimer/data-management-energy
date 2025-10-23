-- Consulta: 08_gold_modelo_final
-- Crea la tabla: gold_data.modelo_final
CREATE OR REPLACE TABLE `datamanagementbi.gold_data.modelo_final` AS
SELECT
  c.fecha,
  c.id_geografia,
  c.id_tramo_horario,
  c.id_sector_economico,
  c.consumo_kwh,
  cal.anio, cal.mes, cal.dia_del_mes, cal.dia_de_la_semana_nombre, cal.es_fin_de_semana, cal.es_festivo,
  geo.nombre_barrio, geo.nombre_distrito, geo.nombre_municipio,
  pob.poblacion,
  clima.temperatura_media_ciudad, clima.humedad_media_ciudad, clima.precipitacion_total_ciudad,
  clima.temp_raval, clima.temp_zuniversitaria, clima.temp_fabra, clima.temp_spread_montana_centro
FROM
  `datamanagementbi.silver_data.fact_consumo_horario` AS c
LEFT JOIN `datamanagementbi.silver_data.dim_calendario` AS cal ON c.fecha = cal.fecha
LEFT JOIN `datamanagementbi.silver_data.dim_geografia` AS geo ON c.id_geografia = geo.id_geografia
LEFT JOIN `datamanagementbi.silver_data.fact_poblacion_anual` AS pob ON c.id_geografia = pob.id_geografia AND cal.anio = pob.anio
LEFT JOIN `datamanagementbi.silver_data.fact_clima_horario` AS clima ON c.fecha = clima.fecha AND c.id_tramo_horario = clima.id_tramo_horario
ORDER BY
  c.fecha, c.id_geografia, c.id_tramo_horario, c.id_sector_economico;