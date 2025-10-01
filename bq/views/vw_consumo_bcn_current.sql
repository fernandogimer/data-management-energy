-- Vista consumible por BI / modelo (filtra casos no válidos)
CREATE OR REPLACE VIEW `neat-tangent-472516-v8.Data_Management.vw_consumo_bcn_current` AS
SELECT *
FROM `neat-tangent-472516-v8.Data_Management.consumo_barcelona`
WHERE tramo_horario <> 'No consta'
  AND kwh IS NOT NULL
  AND kwh >= 0;
