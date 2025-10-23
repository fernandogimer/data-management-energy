# Decisiones de diseño (MVP)
- 2025-10-01: Tabla única `consumo_barcelona` (partition by `fecha`, cluster `codigo_postal, sector_economico`).
  Origen: 2022 a 2025 (`2022_consumo_electrico_BCN` ... `2025_consumo_electrico_BCN`) en `Data_Management`.

Próximas decisiones:
- Reglas de calidad (nulos/duplicados)
- Derivación de horas desde `tramo_horario`
- Joins con meteo/festivos/eventos

# Registro de Decisiones de Arquitectura (ADR)

## 2025-10-23: Refactorización a Modelo en Estrella para las Capas Silver y Gold

**Contexto:**
El diseño inicial del pipeline de datos (MVP) consistía en una transformación directa de la capa Bronze a una tabla Silver ancha y denormalizada. Durante la fase de desarrollo del modelo de ML, se identificó que esta estructura presentaba limitaciones de rendimiento, mantenibilidad y escalabilidad. Generaba redundancia de datos y dificultaba el análisis de negocio (BI) independiente.

**Decisión:**
Se ha decidido refactorizar completamente el pipeline de datos para implementar una arquitectura de **Data Warehouse basada en un Modelo en Estrella**, siguiendo las mejores prácticas de la industria y la metodología Medallion de forma estricta.

**Estado:** **Implementado.**

---

### Estructura de la Nueva Arquitectura (Capa Silver)

La capa Silver ahora consiste en un modelo relacional normalizado (3NF) compuesto por tablas de **Dimensiones** (contexto) y **Hechos** (métricas).

#### **Tablas de Dimensiones (dims):**

1.  **`dim_calendario`**:
    *   **Propósito:** Fuente única de verdad para la dimensión temporal.
    *   **Características:** Contiene una fila por cada día. Se enriquece con atributos como `dia_de_la_semana`, `es_fin_de_semana` y `es_festivo`.
    *   **Clave Primaria:** `fecha` (DATE).

2.  **`dim_geografia`**:
    *   **Propósito:** Diccionario centralizado de ubicaciones.
    *   **Características:** Normaliza la relación entre `codigo_postal`, `nombre_barrio` y `nombre_distrito`.
    *   **Clave Primaria:** `id_geografia` (STRING, ej: "08001").

3.  **`dim_sector_economico`**:
    *   **Propósito:** Catálogo de los diferentes sectores económicos del consumo.
    *   **Características:** Reemplaza el texto "Residencial", "Industrial", etc., por una clave numérica eficiente.
    *   **Clave Primaria:** `id_sector_economico` (INTEGER).

4.  **`dim_tramo_horario`**:
    *   **Propósito:** Catálogo de los tramos horarios del consumo.
    *   **Características:** Normaliza los tramos de 6 horas y los enriquece con `hora_inicio` y `hora_fin`.
    *   **Clave Primaria:** `id_tramo_horario` (INTEGER).

#### **Tablas de Hechos (facts):**

1.  **`fact_poblacion_anual`**:
    *   **Propósito:** Almacena la métrica de población, que varía anualmente (Dimensión de Cambio Lento).
    *   **Granularidad:** `anio` + `id_geografia`.

2.  **`fact_clima_horario`**:
    *   **Propósito:** Almacena las métricas climáticas agregadas para toda la ciudad.
    *   **Características:** Contiene *features* de ingeniería como `temperatura_media_ciudad` y `temp_spread_montana_centro`.
    *   **Granularidad:** `fecha` + `id_tramo_horario`.

3.  **`fact_consumo_horario`**:
    *   **Propósito:** La tabla de hechos principal. Almacena la métrica de consumo.
    *   **Características:** Tabla "delgada" que contiene solo la métrica (`consumo_kwh`) y las claves foráneas a todas las dimensiones.
    *   **Granularidad:** `fecha` + `id_geografia` + `id_sector_economico` + `id_tramo_horario`.

---

### Estructura de la Capa Gold

*   **`gold_data.modelo_final`**:
    *   **Propósito:** Servir como la tabla de entrada final para el entrenamiento de modelos de Machine Learning.
    *   **Características:** Es una **denormalización controlada** de la capa Silver. Se construye uniendo todas las tablas de hechos y dimensiones a través de sus claves (`JOIN`), creando una única tabla ancha y plana, optimizada para el consumo de algoritmos de ML.