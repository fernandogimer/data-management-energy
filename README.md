Data Management – Predicción de Demanda Energética (Barcelona)
Repositorio para versionar SQL, vistas, checks y documentación del proyecto de la asignatura Data Management.
El objetivo es predecir y explicar la demanda energética combinando datos estructurados (consumo) y no estructurados (eventos/alertas), con una arquitectura Google BigQuery + Azure y visualización en Power BI / Looker Studio.
🎯 Objetivo del proyecto
Integrar datos públicos de consumo eléctrico por CP/sector/tramo horario (Barcelona) en BigQuery.
Enriquecer con contexto (meteo, festivos, eventos/alertas) para explicar picos.
Modelar (Azure/AutoML o similar) para mejorar la predicción respecto a un baseline histórico.
Medir con KPIs claros (p. ej., MAPE ≤ 25% en test) y visualizar con un dashboard.
🧩 Alcance (MVP)
Datos estructurados (BigQuery): carga de CSV (2022–2025) → tabla única particionada → vistas para consumo.
BI: dashboard básico (real vs. pred, desgloses) conectado a BigQuery.
Gobierno y calidad: reglas mínimas (nulos, duplicados, trazabilidad).
Modelo: baseline (rolling) y/o AutoML con features de calendario y clima (fase siguiente).


