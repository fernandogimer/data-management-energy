# ============================================================================
# SCRIPT DE ENTRENAMIENTO DEL MODELO DE PREDICCIÓN DE DEMANDA ENERGÉTICA
# ============================================================================
# Autor: Julio Clavijo
# Versión: 4.0 (Modelo Segmentado por Sector)
#
# Descripción:
# Esta es la versión final y más avanzada. Combina tres estrategias clave:
# 1. Segmentación: Entrena un modelo especializado para cada sector económico.
# 2. Feature Engineering Avanzado: Incluye lags horarios, diarios y features no lineales.
# 3. Evaluación Robusta: Utiliza una división cronológica para evitar data leakage.
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
import warnings
import pickle

warnings.filterwarnings('ignore', category=FutureWarning)
print("--- Iniciando el pipeline de entrenamiento de modelo v4.0 (Segmentado) ---")

# --- 1. CARGA DE DATOS DESDE BIGQUERY (Se hace una sola vez) ---
print("\nPaso 1: Cargando y preparando el dataset completo...")
load_dotenv()
GCP_KEY_PATH = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
PROJECT_ID = "datamanagementbi"
TABLE_ID = "gold_data.modelo_final_v2" 
try:
    credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
    query = f"SELECT * FROM `{PROJECT_ID}.{TABLE_ID}` ORDER BY fecha, id_tramo_horario, id_geografia"
    df_full = pd.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, progress_bar_type='console')
    
    # Mapeo de IDs a nombres para facilitar el bucle
    sector_map = {1: 'Industrial', 2: 'Residencial', 3: 'Servicios'}
    df_full['sector_nombre'] = df_full['id_sector_economico'].map(sector_map)
    
    print(f"✅ Carga de datos completada. Se han cargado {len(df_full)} registros.")
except Exception as e:
    print(f"❌ Error al cargar datos desde BigQuery: {e}")
    exit()

# Diccionario para guardar los resultados de cada modelo
resultados_finales = {}

# --- 2. BUCLE DE ENTRENAMIENTO POR SECTOR ---
for sector_nombre in ['Industrial', 'Residencial', 'Servicios']:
    print("\n" + "="*80)
    print(f"🤖 ENTRENANDO MODELO PARA EL SECTOR: {sector_nombre.upper()}")
    print("="*80)

    # 2.1. Filtrar datos para el sector actual
    df_sector = df_full[df_full['sector_nombre'] == sector_nombre].copy()
    print(f"   - Registros para este sector: {len(df_sector)}")

    # 2.2. Feature Engineering (Temporal y No Lineal)
    print("   - Aplicando Feature Engineering...")
    df_sector['fecha'] = pd.to_datetime(df_sector['fecha'])

    # Lags Horarios y Diarios
    df_sector['consumo_lag_1_hora'] = df_sector.groupby('id_geografia')['consumo_kwh'].shift(1)
    df_sector['consumo_lag_2_horas'] = df_sector.groupby('id_geografia')['consumo_kwh'].shift(2)
    df_sector['consumo_lag_1_dia'] = df_sector.groupby('id_geografia')['consumo_kwh'].shift(4)
    df_sector['consumo_media_movil_7d'] = df_sector.groupby('id_geografia')['consumo_kwh'].rolling(window=28, min_periods=1).mean().reset_index(level=0, drop=True)
    
    # Features No Lineales (inspirado en el código de Fernando)
    df_sector['temp_cuadrado'] = df_sector['temperatura_media_ciudad'] ** 2
    df_sector['dist_confort'] = abs(df_sector['temperatura_media_ciudad'] - 20)
    
    df_sector.dropna(inplace=True)

    # 2.3. Preparación de Tipos de Datos
    df_sector.set_index('fecha', inplace=True)
    categorical_cols = [col for col in df_sector.columns if df_sector[col].dtype.name in ['object', 'category'] and col != 'sector_nombre']
    for col in categorical_cols:
        df_sector[col] = df_sector[col].astype('category')
    
    # 2.4. Selección de Features y División de Datos (CRONOLÓGICA)
    target = 'consumo_kwh'
    # Excluimos 'id_sector_economico' y 'sector_nombre' porque ya son constantes en este subset
    features = [col for col in df_sector.columns if col not in [target, 'id_sector_economico', 'sector_nombre', 'nombre_municipio', 'festivo_descripcion']]
    X = df_sector[features]
    y = df_sector[target]

    test_size_ratio = 0.2
    test_size_index = int(len(X) * (1 - test_size_ratio))
    X_train, X_test = X[:test_size_index], X[test_size_index:]
    y_train, y_test = y[:test_size_index], y[test_size_index:]
    print(f"   - Datos divididos: {len(X_train)} para entrenamiento, {len(X_test)} para prueba.")

    # 2.5. Entrenamiento del Modelo
    print("   - Entrenando modelo XGBoost...")
    model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=1000, learning_rate=0.05, max_depth=8, early_stopping_rounds=10, eval_metric='mape', enable_categorical=True)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    # 2.6. Evaluación
    y_pred = model.predict(X_test)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100
    print("\n" + "-"*40)
    print(f"  📊 RESULTADO PARA {sector_nombre.upper()} -> MAPE: {mape:.2f}%")
    print("-"*40 + "\n")

    # 2.7. Interpretabilidad (SHAP)
    print("   - Generando gráfico SHAP...")
    explainer = shap.Explainer(model)
    shap_values = explainer(X_test)
    
    shap.summary_plot(shap_values, X_test, plot_type="bar")
    fig = plt.gcf()
    fig.set_size_inches(10, 8)
    plt.title(f'Importancia de Features - Sector {sector_nombre}')
    plt.tight_layout()
    
    output_path = os.path.join(os.getcwd(), f'shap_summary_{sector_nombre}.png')
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f"   - Gráfico SHAP guardado en: {output_path}")

    # Guardar resultados
    resultados_finales[sector_nombre] = {'mape': mape, 'modelo': model}

# --- 3. RESUMEN FINAL ---
print("\n" + "="*80)
print("🏆 RESUMEN FINAL DE RENDIMIENTO POR SECTOR")
print("="*80)
for sector, resultado in resultados_finales.items():
    print(f"  - {sector}: {resultado['mape']:.2f}% MAPE")

# --- 4. GUARDAR MODELOS ENTRENADOS ---
print("\n" + "="*80)
print("💾 GUARDANDO MODELOS ENTRENADOS PARA PRODUCCIÓN")
print("="*80)

# El diccionario 'resultados_finales' ya contiene los modelos entrenados.
# Lo guardaremos usando pickle.
output_model_path = 'modelos_entrenados_por_sector.pkl'

try:
    with open(output_model_path, 'wb') as file:
        pickle.dump(resultados_finales, file)
    print(f"✅ Modelos guardados correctamente en: {output_model_path}")
except Exception as e:
    print(f"❌ Error al guardar los modelos: {e}")

print("\n--- Proceso completo finalizado ---")