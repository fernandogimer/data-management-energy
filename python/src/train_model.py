# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_percentage_error
import shap
import matplotlib.pyplot as plt
import os
from google.oauth2 import service_account
from dotenv import load_dotenv

# --- 1. CARGA DE DATOS DESDE BIGQUERY ---
print("--- Iniciando el pipeline de entrenamiento de modelo ---")
print("Paso 1: Cargando datos desde la tabla Gold 'modelo_final'...")

# Carga las variables de entorno para la autenticación
load_dotenv()
GCP_KEY_PATH = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
PROJECT_ID = "datamanagementbi"  # Tu ID de proyecto de GCP
TABLE_ID = "gold_data.modelo_final"

# Autenticación y ejecución de la consulta
try:
    credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
    query = f"SELECT * FROM `{PROJECT_ID}.{TABLE_ID}` ORDER BY fecha, id_tramo_horario"
    df = pd.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, progress_bar_type=None)
    print(f"Carga de datos completada. Se han cargado {len(df)} registros.")
except Exception as e:
    print(f"Error al cargar datos desde BigQuery: {e}")
    exit() # Detiene el script si no se pueden cargar los datos

# --- 2. PREPARACIÓN DE DATOS Y FEATURE ENGINEERING ---
print("\nPaso 2: Preparando datos para el entrenamiento...")
# Asegurar que 'fecha' es de tipo datetime y establecerla como índice
df['fecha'] = pd.to_datetime(df['fecha'])
df.set_index('fecha', inplace=True)

# Convertir columnas categóricas a tipo 'category' para que XGBoost las maneje eficientemente
# Estas son las claves foráneas de nuestro modelo en estrella
categorical_cols = [
    'id_geografia', 'id_tramo_horario', 'id_sector_economico', 
    'dia_de_la_semana_nombre', 'nombre_barrio', 'nombre_distrito'
]
for col in categorical_cols:
    if col in df.columns:
        df[col] = df[col].astype('category')
print("Tipos de datos preparados y columnas categóricas configuradas.")

# --- 3. SELECCIÓN DE CARACTERÍSTICAS Y OBJETIVO ---
print("\nPaso 3: Seleccionando características (features) y objetivo (target)...")
# El objetivo es la columna que queremos predecir
target = 'consumo_kwh'

# Las características son todas las columnas que usaremos como "pistas"
# Excluimos el objetivo y columnas de texto que no aportan información predictiva directa
features = [col for col in df.columns if col not in [
    target, 'nombre_municipio', 'festivo_descripcion'
]]

X = df[features]
y = df[target]

# Aseguramos que todas las features sean numéricas antes de entrenar
X = X.apply(pd.to_numeric, errors='coerce')

print(f"Se usarán {len(features)} características para predecir '{target}'.")

# --- 4. DIVISIÓN DE DATOS (TRAIN/TEST SPLIT PARA SERIES TEMPORALES) ---
# Para series temporales, NO debemos mezclar los datos. El conjunto de prueba
# debe ser el período más reciente para simular una predicción real.
test_size = int(len(df) * 0.2) # Usamos el 20% más reciente para la prueba
X_train = X[:-test_size]
X_test = X[-test_size:]
y_train = y[:-test_size]
y_test = y[-test_size:]
print(f"Datos divididos cronológicamente: {len(X_train)} registros para entrenamiento, {len(X_test)} para prueba.")

# --- 5. ENTRENAMIENTO DEL MODELO XGBOOST ---
print("\nPaso 5: Entrenando el modelo XGBoost...")
model = xgb.XGBRegressor(
    objective='reg:squarederror',
    n_estimators=1000,
    learning_rate=0.05,
    max_depth=8,
    early_stopping_rounds=10,
    eval_metric='mape',
    enable_categorical=True # ¡Clave para que XGBoost entienda las categorías!
)
model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
print("Modelo entrenado con éxito.")

# --- 6. EVALUACIÓN DEL MODELO ---
print("\nPaso 6: Evaluando el rendimiento del modelo...")
y_pred = model.predict(X_test)
mape = mean_absolute_percentage_error(y_test, y_pred)
print("="*50)
print(f"  RESULTADO FINAL -> MAPE: {mape:.4f}")
print("="*50)

# --- 7. INTERPRETACIÓN DEL MODELO (SHAP) ---
print("\nPaso 7: Generando gráfico de importancia de características (SHAP)...")

explainer = shap.Explainer(model)
shap_values = explainer(X_test, check_additivity=False)

# ¡CORRECCIÓN! Volvemos al método simple de ploteo
shap.summary_plot(shap_values, X_test, plot_type="bar")

# Guardamos la figura que SHAP ha creado automáticamente
plt.savefig('shap_summary_final.png', bbox_inches='tight')
plt.close()

print("Gráfico 'shap_summary_final.png' guardado en el directorio del proyecto.")
print("\n--- Pipeline de entrenamiento finalizado ---")