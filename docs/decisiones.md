# Decisiones de diseño (MVP)
- 2025-10-01: Tabla única `consumo_barcelona` (partition by `fecha`, cluster `codigo_postal, sector_economico`).
  Origen: 2022 a 2025 (`2022_consumo_electrico_BCN` ... `2025_consumo_electrico_BCN`) en `Data_Management`.

Próximas decisiones:
- Reglas de calidad (nulos/duplicados)
- Derivación de horas desde `tramo_horario`
- Joins con meteo/festivos/eventos
