# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_percentage_error
import shap
import matplotlib.pyplot as plt
import os

from google.oauth2 import service_account
import pandas_gbq

# --- 1. CARGA DE DATOS DESDE BIGQUERY ---
print("Iniciando la carga de datos desde BigQuery...")

# Carga las variables de entorno para encontrar la clave de GCP
load_dotenv()
GCP_KEY_PATH = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")

# --- CONFIGURACIÓN DE BIGQUERY ---

PROJECT_ID = "datamanagementbi" 
TABLE_ID = "gold_data.consumo_barcelona_gold"

# Valida que la ruta al archivo de credenciales existe
if not GCP_KEY_PATH or not os.path.exists(GCP_KEY_PATH):
    raise FileNotFoundError(f"No se pudo encontrar el archivo de credenciales de GCP en la ruta: {GCP_KEY_PATH}")

# Autenticación con la cuenta de servicio
credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)

# Construcción de la consulta SQL
query = f"SELECT * FROM `{PROJECT_ID}.{TABLE_ID}` ORDER BY fecha"

print(f"Ejecutando consulta en la tabla: {TABLE_ID}")
# Lectura de datos desde BigQuery a un DataFrame
df = pd.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)

# Es fundamental asegurar que la columna 'fecha' sea el índice y de tipo Datetime
df['fecha'] = pd.to_datetime(df['fecha'])
df.set_index('fecha', inplace=True)

print(f"Carga de datos completada. Se han cargado {len(df)} registros.")

# --- 2. INGENIERÍA DE CARACTERÍSTICAS (FEATURE ENGINEERING) ---
print("\nIniciando ingeniería de características...")
# Creación de características basadas en la fecha
df['dia_semana'] = df.index.dayofweek
df['dia_mes'] = df.index.day
df['mes'] = df.index.month
df['año'] = df.index.year
df['trimestre'] = df.index.quarter

# Creación de características de retraso (lag features)
df['consumo_lag_1'] = df['consumo_energia'].shift(1)

# Eliminar filas con valores NaN generados por el lag
df.dropna(inplace=True)
print("Ingeniería de características completada.")

# --- 3. PREPARACIÓN DE DATOS PARA EL MODELO ---
print("\nPreparando datos para el entrenamiento...")
# Selección de características (features) y objetivo (target)
features = ['temperatura_media', 'dia_semana', 'dia_mes', 'mes', 'año', 'trimestre', 'consumo_lag_1']
target = 'consumo_energia'

X = df[features]
y = df[target]

# División de datos en conjuntos de entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False, random_state=42)
print(f"Datos divididos: {len(X_train)} para entrenamiento, {len(X_test)} para prueba.")

# --- 4. ENTRENAMIENTO DEL MODELO ---
print("\nEntrenando el modelo XGBoost...")
# Inicialización y entrenamiento del modelo XGBoost
model = xgb.XGBRegressor(
    objective='reg:squarederror',
    n_estimators=1000,
    learning_rate=0.05,
    max_depth=5,
    early_stopping_rounds=10,
    eval_metric='mape'
)

model.fit(
    X_train, y_train,
    eval_set=[(X_test, y_test)],
    verbose=False
)
print("Modelo entrenado con éxito.")

# --- 5. EVALUACIÓN DEL MODELO ---
print("\nEvaluando el rendimiento del modelo...")
# Realización de predicciones en el conjunto de prueba
y_pred = model.predict(X_test)

# Cálculo del error (MAPE)
mape = mean_absolute_percentage_error(y_test, y_pred)
print(f"Resultado de la Evaluación:")
print(f"  -> Error Porcentual Absoluto Medio (MAPE): {mape:.4f}")

# --- 6. INTERPRETACIÓN DEL MODELO (SHAP) ---
print("\nGenerando gráfico de importancia de características (SHAP)...")
# Creación del explicador SHAP y cálculo de los valores
explainer = shap.Explainer(model)
shap_values = explainer(X)

# Generación y guardado del gráfico de resumen
plt.figure()
shap.summary_plot(shap_values, X, show=False)
plt.tight_layout()
plt.savefig('shap_summary.png')
print("Gráfico 'shap_summary.png' guardado en el directorio del proyecto.")

print("\n--- Pipeline de entrenamiento finalizado ---")