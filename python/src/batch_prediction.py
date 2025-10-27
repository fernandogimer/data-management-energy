# ============================================================================
# SCRIPT DE PREDICCIÓN POR LOTES (BATCH PREDICTION)
# ============================================================================
# Autor: Julio Clavijo
# Versión: 1.0
#
# Descripción:
# Este script carga el dataset completo de la tabla gold_data.modelo_final_v2,
# utiliza los modelos segmentados entrenados para generar predicciones para cada
# registro, y sube el resultado (datos originales + predicciones) a una nueva
# tabla en BigQuery: gold_data.predicciones_modelo_final.
# ============================================================================

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import pandas as pd
from google.oauth2 import service_account
from dotenv import load_dotenv
import pickle
import warnings
import os

warnings.filterwarnings('ignore', category=FutureWarning)
print("--- Iniciando el pipeline de Predicción por Lotes ---")

# --- 1. CONFIGURACIÓN Y CARGA DE MODELOS ---
print("\nPaso 1: Cargando configuración y modelos entrenados...")
load_dotenv()
GCP_KEY_PATH = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
PROJECT_ID = "datamanagementbi"

# Tabla de origen (de donde leeremos los datos a predecir)
SOURCE_TABLE_ID = "gold_data.modelo_final_v2"
# Tabla de destino (donde guardaremos los resultados)
DESTINATION_TABLE_ID = "gold_data.predicciones_modelo_final"

# Carga de los modelos entrenados
MODEL_FILE = 'modelos_entrenados_por_sector.pkl'
try:
    with open(MODEL_FILE, 'rb') as file:
        resultados_cargados = pickle.load(file)
    modelos_entrenados = {sector: res['modelo'] for sector, res in resultados_cargados.items()}
    print(f"✅ Modelos para los sectores {list(modelos_entrenados.keys())} cargados correctamente.")
except FileNotFoundError:
    print(f"❌ ERROR: No se encontró el archivo de modelos '{MODEL_FILE}'.")
    print("   Asegúrate de ejecutar 'train_model_final.py' para generarlo.")
    exit()

# --- 2. CARGA DEL DATASET COMPLETO A PREDECIR ---
print(f"\nPaso 2: Cargando dataset completo desde '{SOURCE_TABLE_ID}'...")
try:
    credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
    query = f"SELECT * FROM `{PROJECT_ID}.{SOURCE_TABLE_ID}`"
    df_full = pd.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, progress_bar_type='console')

    # Mapeo de IDs a nombres para facilitar el bucle
    sector_map = {1: 'Industrial', 2: 'Residencial', 3: 'Servicios'}
    df_full['sector_nombre'] = df_full['id_sector_economico'].map(sector_map)
    
    print(f"✅ Carga de datos completada. Se han cargado {len(df_full)} registros.")
except Exception as e:
    print(f"❌ Error al cargar datos desde BigQuery: {e}")
    exit()

# --- 3. GENERACIÓN DE PREDICCIONES POR LOTES ---
print("\nPaso 3: Generando predicciones para cada sector...")

# Lista para almacenar los DataFrames con predicciones de cada sector
lista_predicciones = []

for sector_nombre in ['Industrial', 'Residencial', 'Servicios']:
    print(f"   - Prediciendo para el sector: {sector_nombre.upper()}")

    # Filtrar el DataFrame para el sector actual
    df_sector = df_full[df_full['sector_nombre'] == sector_nombre].copy()
    
    if df_sector.empty:
        print(f"   - No hay datos para el sector {sector_nombre}. Saltando.")
        continue

     # --- INICIO DEL BLOQUE A AÑADIR ---

    # Aplicar EXACTAMENTE la misma ingeniería de características que en el entrenamiento
    print("      - Aplicando Feature Engineering...")
    df_sector['fecha'] = pd.to_datetime(df_sector['fecha'])

    # Lags Horarios y Diarios
    df_sector['consumo_lag_1_hora'] = df_sector.groupby('id_geografia')['consumo_kwh'].shift(1)
    df_sector['consumo_lag_2_horas'] = df_sector.groupby('id_geografia')['consumo_kwh'].shift(2)
    df_sector['consumo_lag_1_dia'] = df_sector.groupby('id_geografia')['consumo_kwh'].shift(4)
    df_sector['consumo_media_movil_7d'] = df_sector.groupby('id_geografia')['consumo_kwh'].rolling(window=28, min_periods=1).mean().reset_index(level=0, drop=True)
    
    # Features No Lineales
    df_sector['temp_cuadrado'] = df_sector['temperatura_media_ciudad'] ** 2
    df_sector['dist_confort'] = abs(df_sector['temperatura_media_ciudad'] - 20)
    
    # ¡Importante! La creación de lags genera NaNs al principio de la serie.
    # Tenemos que manejarlos. Para la predicción, es crucial no perder filas.
    # En lugar de dropear, rellenaremos con un valor neutral (como la media o 0).
    # O, para este caso, simplemente predeciremos sobre los datos válidos.
    df_sector.dropna(inplace=True)

    if df_sector.empty:
        print(f"      - No hay datos válidos para el sector {sector_nombre} tras el feature engineering. Saltando.")
        continue
    
    # --- FIN DEL BLOQUE A AÑADIR ---

    # Seleccionar el modelo correcto
    modelo = modelos_entrenados[sector_nombre]
    
    # Obtener la lista de features que el modelo espera, en el orden correcto
    features_modelo = modelo.get_booster().feature_names
    
    # Asegurarse de que el DataFrame tiene todas las features necesarias
    # (esto previene errores si alguna columna faltara)
    X_pred = df_sector[features_modelo].copy()

    # Convertir columnas a tipo 'category' para que coincida con el entrenamiento
    for col in X_pred.columns:
        if X_pred[col].dtype.name in ['object', 'category']:
            X_pred[col] = X_pred[col].astype('category')

    # Generar predicciones
    predicciones = modelo.predict(X_pred)
    
    # Añadir la columna de predicciones al DataFrame del sector
    df_sector['consumo_kwh_predicho'] = predicciones
    
    # Añadir el DataFrame resultante a nuestra lista
    lista_predicciones.append(df_sector)

# Concatenar los resultados de todos los sectores en un único DataFrame
df_resultado_final = pd.concat(lista_predicciones)

print("✅ Predicciones generadas para todos los registros.")

# --- 4. SUBIDA DE RESULTADOS A BIGQUERY ---
print(f"\nPaso 4: Subiendo {len(df_resultado_final)} registros a la tabla '{DESTINATION_TABLE_ID}'...")

# Seleccionamos y ordenamos las columnas para la tabla final
columnas_finales = [
    'fecha', 'id_geografia', 'id_sector_economico', 'sector_nombre', 
    'id_tramo_horario', 'consumo_kwh', 'consumo_kwh_predicho'
]
df_para_subir = df_resultado_final[columnas_finales].copy()

# Renombramos 'consumo_kwh' para mayor claridad en la tabla final
df_para_subir.rename(columns={'consumo_kwh': 'consumo_kwh_real'}, inplace=True)

try:
    df_para_subir.to_gbq(
        destination_table=DESTINATION_TABLE_ID,
        project_id=PROJECT_ID,
        credentials=credentials,
        if_exists='replace', # 'replace' borra la tabla y la crea de nuevo. Usa 'append' si quieres añadir.
        progress_bar=True
    )
    print("✅ ¡Éxito! La tabla de predicciones ha sido creada/actualizada en BigQuery.")
except Exception as e:
    print(f"❌ Error al subir los datos a BigQuery: {e}")

print("\n--- Pipeline de Predicción por Lotes finalizado ---")