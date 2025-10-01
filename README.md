# Data Management — Predicción de Demanda Energética (Barcelona)

Repositorio del proyecto de la asignatura Data Management.

**Objetivo:** Predecir y explicar la demanda energética en Barcelona integrando datos estructurados (BigQuery) y no estructurados / ML (Azure). Visualización en Power BI o Looker Studio.

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
- `vw_consumo_bcn_current` (filtro de calidad)

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

### Objetos BigQuery

> **Nota:** Ajusta el `projectId` si cambia

- **Tabla:** `neat-tangent-472516-v8.Data_Management.consumo_barcelona`
- **Vista (BI/ML):** `neat-tangent-472516-v8.Data_Management.vw_consumo_bcn_current`

---

## 🧱 Esquema (MVP)

### Tabla: `Data_Management.consumo_barcelona`

**Partition By:** `fecha` (DATE) · **Cluster By:** `cp5`, `sector_economico`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `fecha` | DATE | Fecha del registro |
| `cp5` | STRING | Código postal (5 dígitos) |
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

### 2. Crear vistas

Ejecuta:

```bash
bq/views/vw_consumo_bcn_current.sql
```

Opcional:

```bash
bq/views/vw_consumo_bcn_con_horas.sql
```

### 3. Validar (checks)

Ejecuta: 

```bash
bq/checks/checks_basicos.sql
```

Valida:
- Rango por `anio_origen`
- Nulos/negativos en `kwh`
- Duplicados por clave lógica (`fecha`+`cp5`+`tramo_horario`+`sector_economico`)

### 4. Conectar BI

Conecta Power BI / Looker Studio a `vw_consumo_bcn_current`

---

## ✅ Calidad de datos (mínimo viable)

- **Completitud:** `kwh` no nulo ≥ 99%
- **Validez:** `kwh` ≥ 0; `fecha` en rango dataset
- **Unicidad lógica:** (`fecha`, `cp5`, `tramo_horario`, `sector_economico`) sin duplicados
- **Trazabilidad:** `anio_origen`, `load_ts`

### Ejemplos de checks

Inclúyelos en tu archivo `checks_basicos.sql`:

```sql
-- Nulos/negativos
SELECT 
  SUM(kwh IS NULL) AS nulos, 
  SUM(kwh < 0) AS negativos
FROM `neat-tangent-472516-v8.Data_Management.consumo_barcelona`;

-- Duplicados por clave lógica
SELECT 
  fecha, 
  cp5, 
  tramo_horario, 
  sector_economico, 
  COUNT(*) AS cnt
FROM `neat-tangent-472516-v8.Data_Management.consumo_barcelona`
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50;
```

---

## 📊 KPIs

- **Principal (ejemplo):** MAPE test ≤ 25%
- **Secundarios:** RMSE/MAE por CP/sector/mes; % de picos explicados por eventos
- **Operativos:** Tiempo de refresh; bytes escaneados por consulta

---

## 🔐 Buenas prácticas

### Particionado

Filtra siempre por `fecha` para reducir costes (partition pruning)

### Descripciones en BQ

```sql
ALTER TABLE `neat-tangent-472516-v8.Data_Management.consumo_barcelona`
SET OPTIONS (
  description = 'Histórico BCN 2022–2025 unificado, particionado por fecha'
);

ALTER TABLE `neat-tangent-472516-v8.Data_Management.consumo_barcelona`
ALTER COLUMN kwh SET OPTIONS (
  description = 'kWh por tramo/CP/sector'
);
```

### Evolución sin romper BI

Usa vistas como capa estable; para cambios mayores, crea `*_v2` y re-apunta la vista

---

## 👥 Colaboración (equipo)

### Ramas

- **`main`** (protegida)
- **`feat/<cambio>`** (una por tarea)

### Flujo

1. Crear rama
2. Editar SQL/docs
3. Commit
4. (Opcional) PR/revisión
5. Ejecutar SQL
6. Actualizar `docs/decisiones.md`

### RACI sugerido

- **Data Eng.:** Ingestión / curated / calidad
- **ML/Analytics:** Features / modelo / validación
- **BI/PM:** Vistas para BI, dashboard, storytelling

---

## 🛣️ Roadmap (próxima iteración)

- [ ] `vw_consumo_bcn_con_horas` (parseo horario)
- [ ] Ingesta meteo (AEMET/Meteocat) y festivos (ICS)
- [ ] Vista `vw_consumo_eventos` (join consumo + contexto)
- [ ] Baseline ML + AutoML en Azure
- [ ] Dashboard final (KPI, real vs. pred, explicaciones por evento)

---

## 📚 Referencias

- [Open Data BCN](https://opendata-ajuntament.barcelona.cat/) (consumo por CP/sector/tramo)
- [REE/ESIOS](https://www.esios.ree.es/) (demanda/mercado)
- [AEMET](https://www.aemet.es/) / [Meteocat](https://www.meteo.cat/) (clima)
- [PROCICAT](https://interior.gencat.cat/ca/arees_dactuacio/proteccio_civil/) / [Turisme BCN](https://www.barcelonaturisme.com/) / [Fira de Barcelona](https://www.firabarcelona.com/) (eventos/alertas)

---

## 📝 Licencia

_[Especifica tu licencia aquí]_

## 👤 Autores

_[Añade los nombres del equipo aquí]_
---







