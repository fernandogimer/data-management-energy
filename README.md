# Data Management — Predicción de Demanda Energética (Barcelona)

Repositorio del proyecto de la asignatura Data Management.

**Objetivo:** Predecir y explicar la demanda energética en Barcelona integrando datos estructurados (BigQuery) y no estructurados / ML (Azure). Visualización en Power BI/Looker Studio.

---

## 🎯 Objetivos

- Integrar CSV públicos de consumo (CP / sector / tramo horario, 2022–2025) en una tabla única particionada en BigQuery
- Enriquecer con contexto (meteo, festivos y eventos/alertas) para explicar picos
- Modelar (baseline + AutoML/ML en Azure) para mejorar la precisión frente a un baseline histórico
- Medir y comunicar mediante KPIs (ej.: XX) y dashboard

---

## 🧩 Alcance (MVP)

**Estructurados (BigQuery):** Unificar 2022–2025 en `Data_Management.consumo_barcelona` 
- **Partition by:** `fecha`
- **Cluster by:** `codigo_postal`, `sector_economico`

**Vistas BI/ML:**
- Generar vista

**Checks:** Nulos, negativos, duplicados y rangos

**Dashboard mínimo:** Serie real vs. (luego) pred, KPIs básicos

**Iteración siguiente:** Ingestión meteo/festivos/eventos + entrenamiento/AutoML en Azure

---

## 🏗️ Arquitectura (alto nivel)

### Flujo por capas

```
[CSV consumo BCN 2022–2025] 
    ↓
Google BigQuery (dataset: Data_Management)
    ↓
(1) Tabla única: consumo_barcelona
    • Partition By: fecha (DATE)
    • Cluster By: codigo_postal, sector_economico
    ↓
(2) Vistas:
    • vw_consumo_bcn_current (filtro calidad BI/ML)
    ↓
(3) BI: Power BI / Looker Studio
    ↓
(4) Enriquecimiento (próxima fase):
    • Meteo (AEMET/Meteocat)
    • Festivos (ICS)
    • Eventos/alertas (Azure Logic Apps + Azure AI Text Analytics)
    • Joins en BigQuery
    • Entrenamiento/AutoML en Azure
    • Persistencia de predicciones
```

### Principios clave

- **Particionado por fecha** para coste/velocidad
- **Vistas estables** para no romper BI/ML al cambiar tablas base
- **Trazabilidad:** `anio_origen`, `load_ts`

---

## 🗂️ Estructura del repositorio

```
bq/
├── ddl/
│   └── 01_create_consumo_barcelona.sql   # CTAS que unifica 2022–2025 en tabla única
├── views/
│   ├── vw_consumo_bcn_current.sql        # Vista con filtro básico de calidad
│   └── vw_consumo_bcn_con_horas.sql      # (Opcional) Parseo de horas desde tramo_horario
└── checks/
    └── checks_basicos.sql                # Nulos/negativos/duplicados + rangos

docs/
└── decisiones.md                         # Registro de cambios/decisiones (qué/por qué/cuándo/quién)

README.md
.gitignore
```


---

## 🧱 Esquema (MVP)

### Tabla: `Data_Management.consumo_barcelona`

**Partition By:** `fecha` (DATE) · **Cluster By:** `cp5`, `sector_economico`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `fecha` | DATE | Fecha del registro |
| `codigo_postal` | STRING | Código postal (5 dígitos) |
| `sector_economico` | STRING | Sector económico |
| `tramo_horario` | STRING | "De 00:00:00 a 06:00:00" / "No consta" |
| `kwh` | FLOAT64 | Consumo del tramo en kWh |
| `anio_origen` | STRING | Año/origen del registro (p. ej., "2023") |
| `load_ts` | TIMESTAMP | Timestamp de carga/transformación |

### Vistas sugeridas

- **`vw_consumo_bcn_current`:** Excluye "No consta" y kWh nulos/negativos
- **`vw_consumo_bcn_con_horas`:** Extrae `hora_ini`/`hora_fin`

---

## 📥 Cómo reproducir (paso a paso)

### 1. Crear/actualizar la tabla única (CTAS)

Ejecuta en BigQuery: 

```bash
bq/ddl/01_create_consumo_barcelona.sql
```

### 2. Conectar BI

Conecta Power BI / Looker Studio a `vw_consumo_bcn_current`

---

## ✅ Calidad de datos (mínimo viable)

- **Completitud:** `kwh` no nulo ≥ 99%
- **Validez:** `kwh` ≥ 0; `fecha` en rango dataset
- **Unicidad lógica:** (`fecha`, `codigoo_postal`, `tramo_horario`, `sector_economico`) sin duplicados
- **Trazabilidad:** `anio_origen`, `load_ts`

---

## 📊 KPIs

aplicar kpis que seleccionamos


---

## 📝 Licencia

_[Especifica tu licencia aquí]_

## 👤 Autores

_[Añade los nombres del equipo aquí]_
---

## Subproyecto: Pipeline de Ingesta y Enriquecimiento NLP con Databricks

Como parte del proceso de ingesta de datos, se desarrolló un pipeline de datos independiente en **Azure Databricks** para generar y enriquecer una de las fuentes de datos.

- **Objetivo:** Demostrar la orquestación de un pipeline end-to-end en Databricks, incluyendo la simulación de datos, el enriquecimiento con una API de IA Generativa (Google Gemini) y la exportación de resultados a BigQuery.
- **Tecnologías:** Azure Databricks, Python (PySpark, Pandas), Google Gemini API.
- **Resultado:** La tabla `bronze_data.precios_actividad_nlp_manual` en BigQuery.
- **Detalles:** Ver el [informe técnico completo](docs/informe_pipeline_nlp.md) y el [código del notebook](notebooks/databricks/pipeline_databricks_nlp.py).







