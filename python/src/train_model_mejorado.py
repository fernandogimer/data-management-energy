# ============================================================================
# SCRIPT DE ENTRENAMIENTO DEL MODELO DE PREDICCIÓN DE DEMANDA ENERGÉTICA
# ============================================================================
# Autor: Julio Clavijo
# Versión: 3.0 (con Lags Horarios Mejorados)
#
# Descripción:
# Esta versión introduce lags horarios para capturar la dependencia
# del consumo con las horas inmediatamente anteriores, buscando reducir
# aún más el MAPE.
# ============================================================================

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_percentage_error
import shap
import matplotlib.pyplot as plt
import os
from google.oauth2 import service_account
from dotenv import load_dotenv

print("--- Iniciando el pipeline de entrenamiento de modelo v3.0 ---")

# --- 1. CARGA DE DATOS DESDE BIGQUERY ---
print("\nPaso 1: Cargando datos desde la tabla Gold 'modelo_final_v2'...")
load_dotenv()
GCP_KEY_PATH = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
PROJECT_ID = "datamanagementbi"
TABLE_ID = "gold_data.modelo_final_v2" 
try:
    credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
    # ORDENAMOS CRONOLÓGICAMENTE para los lags
    query = f"SELECT * FROM `{PROJECT_ID}.{TABLE_ID}` ORDER BY fecha, id_tramo_horario, id_geografia"
    df = pd.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, progress_bar_type='console')
    print(f"✅ Carga de datos completada. Se han cargado {len(df)} registros.")
except Exception as e:
    print(f"❌ Error al cargar datos desde BigQuery: {e}")
    exit()

# --- 2. PREPARACIÓN DE DATOS Y FEATURE ENGINEERING MEJORADO ---
print("\nPaso 2: Preparando datos y aplicando Feature Engineering Avanzado...")
df['fecha'] = pd.to_datetime(df['fecha'])

# 2.1. Ingeniería de Características Temporales (CON LAGS HORARIOS)
print("   Paso 2.1: Creando lags y rolling features...")

# --- Lags Horarios (¡LA MEJORA CLAVE!) ---
# Para cada ubicación geográfica, tomamos el valor del registro anterior (la hora anterior).
df['consumo_lag_1_hora'] = df.groupby('id_geografia')['consumo_kwh'].shift(1)
# También puede ser útil el lag de 2 y 3 horas antes.
df['consumo_lag_2_horas'] = df.groupby('id_geografia')['consumo_kwh'].shift(2)
df['consumo_lag_3_horas'] = df.groupby('id_geografia')['consumo_kwh'].shift(3)

# --- Lags Diarios y Semanales (conservamos los que teníamos) ---
# El consumo del mismo tramo horario el día anterior
df['consumo_lag_1_dia'] = df.groupby('id_geografia')['consumo_kwh'].shift(4) # 4 tramos por día
# El consumo del mismo tramo horario la semana anterior
df['consumo_lag_7_dias'] = df.groupby('id_geografia')['consumo_kwh'].shift(28) # 4 tramos * 7 días

# --- Rolling Features (conservamos las que teníamos) ---
df['consumo_media_movil_7d'] = df.groupby('id_geografia')['consumo_kwh'].rolling(window=28, min_periods=1).mean().reset_index(level=0, drop=True)
df['temp_media_movil_3d'] = df.groupby('id_geografia')['temperatura_media_ciudad'].rolling(window=12, min_periods=1).mean().reset_index(level=0, drop=True)

print(f"   Filas antes de eliminar NaNs: {len(df)}")
df.dropna(inplace=True)
print(f"   Filas después de eliminar NaNs: {len(df)}")

# 2.2. Preparación de Tipos de Datos
print("\n   Paso 2.2: Ajustando tipos de datos...")
df.set_index('fecha', inplace=True)
categorical_cols = [
    'id_geografia', 'id_tramo_horario', 'id_sector_economico', 
    'dia_de_la_semana_nombre', 'nombre_barrio', 'nombre_distrito', 'nombre_fiesta'
]
for col in categorical_cols:
    if col in df.columns:
        df[col] = df[col].astype('category')
print("✅ Preparación de datos completada.")


# --- 3. SELECCIÓN DE FEATURES Y TARGET ---
# (Esta sección no cambia)
print("\nPaso 3: Definiendo features y target...")
target = 'consumo_kwh'
features = [col for col in df.columns if col not in [target, 'nombre_municipio', 'festivo_descripcion']]
X = df[features]
y = df[target]
print("✅ Features y target definidos.")

# --- 4. DIVISIÓN DE DATOS ---
# (Esta sección no cambia)
print("\nPaso 4: Dividiendo datos en entrenamiento y prueba...")
test_size_ratio = 0.2
test_size_index = int(len(df) * (1 - test_size_ratio))
X_train, X_test = X[:test_size_index], X[test_size_index:]
y_train, y_test = y[:test_size_index], y[test_size_index:]
print(f"✅ Datos divididos: {len(X_train)} para entrenamiento, {len(X_test)} para prueba.")


# --- 5. ENTRENAMIENTO DEL MODELO XGBOOST ---
# (Esta sección no cambia)
print("\nPaso 5: Entrenando el modelo XGBoost...")
model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=1000, learning_rate=0.05, max_depth=8, early_stopping_rounds=10, eval_metric='mape', enable_categorical=True)
model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
print("✅ Modelo entrenado con éxito.")


# --- 6. EVALUACIÓN DEL MODELO ---
# (Esta sección no cambia)
print("\nPaso 6: Evaluando el rendimiento...")
y_pred = model.predict(X_test)
mape = mean_absolute_percentage_error(y_test, y_pred) * 100
print("\n" + "="*50)
print(f"  RESULTADO FINAL -> MAPE: {mape:.2f}%")
print("="*50 + "\n")


# --- 7. INTERPRETACIÓN DEL MODELO (SHAP) ---
# (Esta sección no cambia, usamos la versión compatible)
print("Paso 7: Generando análisis de interpretabilidad (SHAP)...")
explainer = shap.Explainer(model)
shap_values = explainer(X_test, check_additivity=False)

print("   Generando gráfico de barras de importancia...")
shap.summary_plot(shap_values, X_test, plot_type="bar")

fig = plt.gcf()
fig.set_size_inches(10, 8)
plt.tight_layout()

output_path = os.path.join(os.getcwd(), 'shap_summary_final.png')
try:
    fig.savefig(output_path, bbox_inches='tight')
    print(f"✅ Gráfico SHAP guardado en: {output_path}")
except Exception as e:
    print(f"❌ Error al guardar el gráfico SHAP: {e}")
plt.close(fig)

print("\n--- Pipeline de entrenamiento finalizado ---")