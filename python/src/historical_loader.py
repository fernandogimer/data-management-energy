import requests
import pandas as pd
from datetime import datetime
import time
import os
from google.cloud import storage
from dotenv import load_dotenv

# --- 1. CONFIGURACIÓN ---
# Carga las variables de tu archivo .env (tu "caja fuerte") para que el script pueda usarlas.
load_dotenv()
API_KEY = os.getenv("AEMET_API_KEY")
GCP_KEY_PATH = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")

# Variables específicas de esta tarea.
IDEMA_BARCELONA = "0200E" # ID de la estación meteorológica de Barcelona
START_DATE = datetime(2021, 1, 1)
END_DATE = datetime.now()
GCS_BUCKET_NAME = "dm-bi-project-raw-data" # El nombre de tu bucket en GCS
HEADERS = {'accept': 'application/json', 'api_key': API_KEY}

# --- 2. FUNCIONES (Tus "Herramientas" reutilizables) ---

# Herramienta 1: Descarga los datos del clima de AEMET año por año.
def fetch_historical_weather(start_date, end_date, idema):
    all_data = []
    # Itera desde el año de inicio hasta el año actual.
    for year in range(start_date.year, end_date.year + 1):
        print(f"Procesando año: {year}...")
        
        # Formatea las fechas como la API de AEMET las necesita.
        start_str = f"{year}-01-01T00:00:00UTC"
        end_str = f"{year}-12-31T23:59:59UTC"

        # Construye y realiza la llamada a la API.
        url_solicitud = (f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/"
                       f"fechaini/{start_str}/fechafin/{end_str}/estacion/{idema}")
        try:
            # El primer 'get' obtiene la URL donde están los datos reales.
            response_solicitud = requests.get(url_solicitud, headers=HEADERS, verify=True)
            response_solicitud.raise_for_status() # Lanza un error si la llamada falla.
            respuesta_url = response_solicitud.json()

            if respuesta_url.get('estado') == 200:
                url_datos = respuesta_url.get('datos')
                
                # El segundo 'get' descarga los datos de esa URL.
                response_datos = requests.get(url_datos, headers=HEADERS, verify=True)
                response_datos.raise_for_status()
                datos_anuales = response_datos.json()
                all_data.extend(datos_anuales)
                print(f"Año {year} descargado con éxito.")
            else:
                print(f"Error en la solicitud para el año {year}: {respuesta_url.get('descripcion')}")
            
            # Pausa para no sobrecargar la API de AEMET.
            time.sleep(6)
        except requests.exceptions.RequestException as e:
            print(f"Error de red procesando el año {year}: {e}")
            continue
    # Convierte la lista de datos en una tabla de Pandas.
    return pd.DataFrame(all_data)

# Herramienta 2: Sube la tabla de Pandas a Google Cloud Storage como un archivo Parquet.
def upload_df_to_gcs(df, bucket_name, destination_blob_name):
    # Guarda temporalmente el DataFrame como un archivo .parquet en tu disco.
    temp_file_path = "temp_historical_data.parquet"
    df.to_parquet(temp_file_path, index=False)
    
    # Se conecta a Google Cloud usando tus credenciales.
    storage_client = storage.Client.from_service_account_json(GCP_KEY_PATH)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Sube el archivo temporal a la nube.
    blob.upload_from_filename(temp_file_path)
    print(f"Archivo {destination_blob_name} subido con éxito a GCS.")
    
    # Borra el archivo temporal de tu disco.
    os.remove(temp_file_path)

# --- 3. EJECUCIÓN PRINCIPAL ---
# Esta es la sección que se ejecuta cuando corres 'python historical_loader.py'.
if __name__ == "__main__":
    # 1. Llama a la herramienta para descargar los datos.
    historical_df = fetch_historical_weather(START_DATE, END_DATE, IDEMA_BARCELONA)
    
    if not historical_df.empty:
        # 2. Limpia y transforma los datos.
        historical_df['fecha'] = pd.to_datetime(historical_df['fecha'])
        numeric_cols = ['tmed', 'tmin', 'tmax', 'prec']
        for col in numeric_cols:
            if col in historical_df.columns:
                # Convierte texto como '0,5' a números como 0.5.
                historical_df[col] = pd.to_numeric(historical_df[col].str.replace(',', '.'), errors='coerce')
        
        # 3. Define un nombre para el archivo en la nube.
        blob_name = f"api_raw_data/clima_historico_{START_DATE.year}-{END_DATE.year}.parquet"
        
        # 4. Llama a la herramienta para subir los datos.
        upload_df_to_gcs(historical_df, GCS_BUCKET_NAME, blob_name)
    else:
        print("No se pudieron obtener datos históricos.")