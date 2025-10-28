# ⚡ Data Management - Predicción de Demanda Energética en Barcelona

![Python](https://img.shields.io/badge/Python-3.9+-blue)
![BigQuery](https://img.shields.io/badge/BigQuery-Enabled-orange)
![Azure](https://img.shields.io/badge/Azure-Databricks-blue)
![Looker Studio](https://img.shields.io/badge/Looker_Studio-Visualization-yellow)

## 📋 Descripción del Proyecto

Repositorio del proyecto de la asignatura Data Management.

**Objetivo:** Predecir y explicar la demanda energética en Barcelona integrando datos estructurados (BigQuery) y no estructurados / ML (Azure). Visualización en Looker Studio.

### Alcance del Proyecto

- Integrar CSV públicos de consumo (CP / sector / tramo horario, 2022–2025) en una tabla única particionada en BigQuery
- Enriquecer con contexto (meteo, festivos y eventos/alertas) para explicar picos
- Modelar (baseline + AutoML/ML en Azure) para mejorar la precisión frente a un baseline histórico
- Medir y comunicar mediante KPIs y dashboard

---

## 🏗️ Arquitectura de Datos

### Capa de Almacenamiento (BigQuery)

**Estructurados (BigQuery):** Unificar 2022–2025 en `Data_Management.consumo_barcelona`

- **Partition by:** `fecha`
- **Cluster by:** `codigo_postal`, `sector_economico`

**Vistas BI/ML:**
- Generar vista para análisis y ML

**Checks:** Nulos, negativos, duplicados y rangos

**Dashboard mínimo:** Serie real vs. predicción, KPIs básicos

**Iteración siguiente:** Ingestión meteo/festivos/eventos + entrenamiento/AutoML en Azure

### Flujo de Datos

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
(3) BI: Looker Studio
         ↓
(4) Enriquecimiento (próxima fase):
    • Meteo (AEMET/Meteocat)
    • Festivos (ICS)
    • Eventos/alertas (Azure Logic Apps + Azure AI Text Analytics)
    • Joins en BigQuery
    • Entrenamiento/AutoML en Azure
    • Persistencia de predicciones
```

### Decisiones Técnicas Clave

- **Particionado por fecha** para optimizar coste y velocidad
- **Vistas estables** para no romper BI/ML al cambiar tablas base
- **Trazabilidad:** `anio_origen`, `load_ts`

---

## 📁 Estructura del Repositorio

```
bq/
├── ddl/
│   └── 01_create_consumo_barcelona.sql  # CTAS que unifica 2022–2025 en tabla única
├── views/
│   ├── vw_consumo_bcn_current.sql       # Vista con filtro básico de calidad
│   └── vw_consumo_bcn_con_horas.sql     # (Opcional) Parseo de horas desde tramo_horario
└── checks/
    └── checks_basicos.sql               # Nulos/negativos/duplicados + rangos

docs/
└── decisiones.md                        # Registro de cambios/decisiones (qué/por qué/cuándo/quién)
└── informe_pipeline_nlp.md             # Informe técnico del pipeline NLP

notebooks/
└── databricks/
    └── pipeline_databricks_nlp.py      # Pipeline de datos en Databricks

README.md
.gitignore
LICENSE
```

---

## 📊 Esquema de Datos

### Tabla Principal: `consumo_barcelona`

**Partition By:** `fecha` (DATE) · **Cluster By:** `codigo_postal`, `sector_economico`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `fecha` | DATE | Fecha del registro |
| `codigo_postal` | STRING | Código postal (5 dígitos) |
| `sector_economico` | STRING | Sector económico |
| `tramo_horario` | STRING | "De 00:00:00 a 06:00:00" / "No consta" |
| `kwh` | FLOAT64 | Consumo del tramo en kWh |
| `anio_origen` | STRING | Año/origen del registro (p. ej., "2023") |
| `load_ts` | TIMESTAMP | Timestamp de carga/transformación |

### Vistas Disponibles

- **`vw_consumo_bcn_current`**: Excluye "No consta" y kWh nulos/negativos
- **`vw_consumo_bcn_con_horas`**: Extrae `hora_ini` / `hora_fin`

---

## 🚀 Cómo Usar Este Proyecto

### Requisitos Previos

- Acceso a Google BigQuery
- Looker Studio
- Azure Databricks (para el pipeline NLP)
- Python 3.9+

### Pasos de Implementación

1. **Ejecuta en BigQuery:**
   ```sql
   -- Ejecutar los scripts en orden:
   bq/ddl/01_create_consumo_barcelona.sql
   ```

2. **Conecta Looker Studio:**
   - Conectar a la vista `vw_consumo_bcn_current`

3. **Verifica la calidad de datos:**
   ```sql
   -- Ejecutar checks
   bq/checks/checks_basicos.sql
   ```

---

## 📈 KPIs y Calidad de Datos

### Métricas de Calidad

- **Completitud:** `kwh` no nulo ≥ 99%
- **Validez:** `kwh` ≥ 0; `fecha` en rango dataset
- **Unicidad lógica:** (`fecha`, `codigo_postal`, `tramo_horario`, `sector_economico`) sin duplicados
- **Trazabilidad:** `anio_origen`, `load_ts`

### KPIs del Proyecto

- Error medio absoluto (MAE) de predicción
- Cobertura de datos por código postal
- Porcentaje de datos validados
- Tiempo de actualización del pipeline

---

## 🤖 Pipeline NLP en Azure Databricks

Como parte del proceso de ingesta de datos, se desarrolló un pipeline de datos independiente en Azure Databricks para generar y enriquecer una de las fuentes de datos.

- **Objetivo:** Demostrar la orquestación de un pipeline end-to-end en Databricks, incluyendo la simulación de datos, el enriquecimiento con una API de IA Generativa (Google Gemini) y la exportación de resultados a BigQuery.
- **Tecnologías:** Azure Databricks, Python (PySpark, Pandas), Google Gemini API.
- **Resultado:** La tabla `bronze_data.precios_actividad_nlp_manual` en BigQuery.
- **Detalles:** Ver el [informe técnico completo](docs/informe_pipeline_nlp.md) y el [código del notebook](notebooks/databricks/pipeline_databricks_nlp.py).

---

## 🛠️ Tecnologías Utilizadas

- **Cloud & Data Warehouse:** Google BigQuery
- **Procesamiento:** Azure Databricks, PySpark
- **Visualización:** Looker Studio
- **IA Generativa:** Google Gemini API
- **Lenguajes:** Python, SQL
- **Control de versiones:** Git, GitHub

---

## 👥 Autores

**Fernando Gimenez** - *Desarrollo y documentación*  
**Julio Clavijo** - julio.clavijo88@gmail.com  
**Miguel Roces** - miguelrocesdiaz@gmail.com

**Asignatura:** Data Management  
**Fecha:** Octubre 2025

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

---

## 📞 Contacto

Si tienes preguntas sobre este proyecto, puedes contactarnos a través de:
- GitHub: [@fernandogimer](https://github.com/fernandogimer)
- Julio Clavijo: julio.clavijo88@gmail.com
- Miguel Roces: miguelrocesdiaz@gmail.com

---

## 🙏 Agradecimientos

- Datos de consumo energético proporcionados por fuentes públicas de Barcelona
- Asignatura Data Management por la guía del proyecto
