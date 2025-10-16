# ==============================================================================
# MÓDULO DE EXTRACCIÓN DE DATOS METEOROLÓGICOS DE CATALUNYA
# ==============================================================================
#
# Descripción:
#   Este script se encarga de extraer datos meteorológicos históricos de la API
#   de Dades Obertes de la Generalitat de Catalunya. Los datos se filtran para
#   estaciones específicas de Barcelona y para un conjunto de variables de interés.
#   Finalmente, los datos son enriquecidos y cargados a un bucket de
#   Google Cloud Storage (GCS) en formato Parquet para su posterior análisis.
#
# Autor: Julio Clavijo
# Fecha: 15/10/2025
#
# ==============================================================================

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
# Se importan todas las librerías necesarias al principio del archivo.
# Esta es una buena práctica para tener una visión clara de las dependencias del script.

import requests  # Librería estándar para realizar peticiones HTTP a las APIs.
import pandas as pd  # La librería fundamental para la manipulación de datos en formato tabular (DataFrames).
from datetime import datetime  # Para manejar fechas y horas de forma sencilla.
import time  # Utilizada para añadir pausas entre llamadas a la API.
import os  # Permite interactuar con el sistema operativo, como leer variables de entorno.
from google.cloud import storage  # La librería oficial de Google para interactuar con Cloud Storage.
from dotenv import load_dotenv  # Herramienta para cargar secretos desde un archivo .env.

# --- 1. CONFIGURACIÓN GLOBAL Y PARÁMETROS ---
# En esta sección se definen todas las variables que controlan el comportamiento del script.
# Mantener la configuración centralizada aquí facilita futuras modificaciones.

# Ejecuta la función que busca un archivo .env en la raíz del proyecto y carga
# sus variables para que 'os.getenv' pueda acceder a ellas.
load_dotenv()

# --- Carga de Secretos ---
# Se leen las claves secretas desde las variables de entorno.
# Esto evita tener que escribir información sensible directamente en el código.
CATALUNYA_APP_TOKEN = os.getenv("CATALUNYA_APP_TOKEN")
GCP_KEY_PATH = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
GCS_BUCKET_NAME = "dm-bi-project-raw-data" # Nombre del bucket en GCS donde se guardarán los datos.

# --- Parámetros de la Extracción ---
# Se define una lista con los identificadores de las estaciones de Barcelona
# para obtener una cobertura geográfica representativa de la ciudad.
ESTACIONES_BARCELONA = ['X4', 'X8', 'D5']

# Se define una lista con los códigos de las variables meteorológicas de interés.
# Esto reduce el volumen de datos descargados a solo lo estrictamente necesario.
VARIABLES_DE_INTERES = ['32', '33', '35', '30', '36'] # Temp, Humedad, Precip, Viento, Rad. Solar

# Se establece la fecha de inicio a partir de la cual se descargarán los datos.
START_DATE = datetime(2022, 1, 1)

# Se define la URL base del "recurso" de la API. Sobre esta URL se construirán las consultas.
RESOURCE_URL = "https://analisi.transparenciacatalunya.cat/resource/nzvn-apee.json"

# --- Diccionarios de Metadatos para Enriquecimiento ---
# Estos diccionarios son una herramienta de transformación. Permiten convertir
# los códigos crípticos de la API en valores legibles para humanos.
DICCIONARIO_ESTACIONES = {
    "X4": "Barcelona - el Raval",
    "X8": "Barcelona - Zona Universitària",
    "D5": "Barcelona - Observatori Fabra"
}

DICCIONARIO_VARIABLES = {
    "32": "Temperatura",
    "33": "Humedad relativa",
    "35": "Precipitación",
    "30": "Velocidad del viento",
    "36": "Irradiancia solar global"
}

# --- 2. DEFINICIÓN DE FUNCIONES ---
# Se modulariza el código en funciones reutilizables. Cada función tiene una
# única responsabilidad, lo que hace el código más limpio, fácil de leer y de depurar.

def fetch_catalunya_weather(start_date, station_codes, variables, app_token):
    """
    Función principal de extracción.
    Descarga datos meteorológicos para una lista de estaciones y variables,
    implementando un bucle de paginación para asegurar la obtención de todos los registros.

    Args:
        start_date (datetime): La fecha de inicio de la extracción.
        station_codes (list): Lista de códigos de las estaciones a consultar.
        variables (list): Lista de códigos de las variables a consultar.
        app_token (str): El token de aplicación para autenticarse en la API.

    Returns:
        pd.DataFrame: Un DataFrame de Pandas con todos los datos combinados, o un DataFrame vacío si falla.
    """
    print("--- INICIANDO EXTRACCIÓN COMPLETA (CON PAGINACIÓN) ---")
    
    # Se crea una lista vacía que acumulará los datos de todas las estaciones y todas las páginas.
    all_stations_data = []
    
    # Se itera sobre cada estación para procesarlas individualmente.
    for station_code in station_codes:
        print(f"\nProcesando estación: {station_code} ({DICCIONARIO_ESTACIONES.get(station_code, 'Desconocida')})...")
        
        # --- Implementación del Bucle de Paginación ---
        # Este bucle asegura que se descarguen todos los datos, incluso si superan el límite por petición.
        PAGE_SIZE = 50000  # Tamaño de cada "página" de datos. 50,000 es un límite estándar y seguro para APIs SODA.
        offset = 0         # Contador que indica desde qué registro empezar en cada nueva petición.
        
        while True:
            print(f"  -> Obteniendo página... (Registros desde el {offset})")
            
            # Prepara las partes de la cláusula de filtrado (cláusula WHERE en lenguaje SoQL).
            variables_str = ', '.join([f"'{v}'" for v in variables])
            start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')

            # Se construye la cláusula WHERE. Se formatea en varias líneas para cumplir con PEP 8 y mejorar la legibilidad.
            where_clause = (
                f"codi_estacio = '{station_code}' "
                f"AND data_lectura >= '{start_date_str}' "
                f"AND codi_variable IN ({variables_str})"
            )
            
            # Se define el diccionario de parámetros para la petición GET.
            # La librería 'requests' se encargará de codificar esto en la URL final.
            params = {
                "$where": where_clause,
                "$limit": PAGE_SIZE,    # Cuántos registros pedir como máximo.
                "$offset": offset,      # Desde qué registro empezar.
                "$$app_token": app_token # El token de autenticación.
            }
            
            try:
                # Se realiza la petición a la API.
                response = requests.get(RESOURCE_URL, params=params, timeout=900)
                response.raise_for_status() # Lanza un error si el código de estado no es 2xx.
                data_page = response.json() # Convierte la respuesta en una lista de diccionarios Python.
                
                # --- Lógica para detener el bucle ---
                if not data_page:
                    # Condición de salida 1: La API devuelve una página vacía, lo que significa que ya no hay más datos.
                    print("  -> No se encontraron más registros. Fin de la paginación para esta estación.")
                    break # Rompe el bucle 'while' y pasa a la siguiente estación.

                # Se añaden los datos de esta página a la lista general.
                all_stations_data.extend(data_page)
                
                if len(data_page) < PAGE_SIZE:
                    # Condición de salida 2: La página devuelta tiene menos registros que el límite solicitado.
                    # Esto indica que es la última página de resultados.
                    print(f"  -> Descargada la última página con {len(data_page)} registros.")
                    break
                
                # Si llegamos aquí, significa que la página estaba llena y debemos pedir la siguiente.
                # Se incrementa el offset para la próxima iteración.
                offset += PAGE_SIZE
                
            except requests.exceptions.RequestException as e:
                # Si ocurre un error de red (ej. sin conexión a internet), se registra y se aborta
                # la paginación para esta estación.
                print(f"ERROR de red al procesar la estación {station_code}: {e}")
                break # Sale del bucle 'while'.

    if not all_stations_data:
        # Si después de procesar todas las estaciones la lista de datos sigue vacía, se notifica y se devuelve un DataFrame vacío.
        print("\nNo se pudo obtener ningún dato de ninguna estación. Proceso abortado.")
        return pd.DataFrame()
        
    print(f"\n--- EXTRACCIÓN COMPLETADA --- Total de registros obtenidos: {len(all_stations_data)}")
    # Se convierte la lista final de diccionarios en un DataFrame de Pandas para su manipulación.
    return pd.DataFrame(all_stations_data)


def upload_df_to_gcs(df, bucket_name, destination_blob_name):
    """
    Función genérica para subir un DataFrame de Pandas a Google Cloud Storage en formato Parquet.

    Args:
        df (pd.DataFrame): El DataFrame a subir.
        bucket_name (str): El nombre del bucket de destino en GCS.
        destination_blob_name (str): La ruta y nombre del archivo a crear en el bucket (ej. 'carpeta/archivo.parquet').
    """
    # El formato Parquet es columnar y comprimido, ideal para analítica y mucho más eficiente que CSV.
    temp_file_path = "temp_catalunya_data.parquet"
    df.to_parquet(temp_file_path, index=False)
    
    # El cliente de storage se autentica usando el archivo JSON de credenciales.
    storage_client = storage.Client.from_service_account_json(GCP_KEY_PATH)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Se realiza la subida del archivo desde el disco local a la nube.
    blob.upload_from_filename(temp_file_path)
    print(f"Archivo '{destination_blob_name}' subido con éxito al bucket '{bucket_name}'.")
    
    # Se elimina el archivo temporal para mantener limpio el entorno de ejecución.
    os.remove(temp_file_path)

# --- 3. EJECUCIÓN PRINCIPAL DEL SCRIPT ---
# El bloque `if __name__ == "__main__":` es una convención en Python.
# Asegura que el código dentro de este bloque solo se ejecute cuando el archivo
# es llamado directamente desde la terminal, y no cuando es importado por otro script.
if __name__ == "__main__":
    
    # Paso 1: Orquestar la extracción de datos llamando a la función principal.
    weather_df = fetch_catalunya_weather(START_DATE, ESTACIONES_BARCELONA, VARIABLES_DE_INTERES, CATALUNYA_APP_TOKEN)
    
    # Paso 2: Ejecutar la transformación y carga solo si la extracción fue exitosa.
    if not weather_df.empty:
        
        print("\n--- INICIANDO TRANSFORMACIÓN Y ENRIQUECIMIENTO DE DATOS ---")
        
        # Paso 2.1: Enriquecer los datos. Se crean nuevas columnas con los nombres legibles
        # a partir de los códigos, usando los diccionarios de metadatos.
        weather_df['nom_estacio'] = weather_df['codi_estacio'].map(DICCIONARIO_ESTACIONES)
        weather_df['nom_variable'] = weather_df['codi_variable'].map(DICCIONARIO_VARIABLES)
        
        print("Datos enriquecidos. Columnas añadidas: 'nom_estacio', 'nom_variable'.")
        
        # Paso 3: Definir un nombre único y descriptivo para el archivo en GCS.
        # Incluir fechas en el nombre es una buena práctica para el versionado.
        blob_name = f"api_raw_data/catalunya_clima_barcelona_{START_DATE.year}-presente_COMPLETO.parquet"
        
        # Paso 4: Orquestar la carga llamando a la función de subida.
        upload_df_to_gcs(weather_df, GCS_BUCKET_NAME, blob_name)
        
        print("\n--- PROCESO FINALIZADO CON ÉXITO ---")
        
    else:
        # Este bloque se ejecuta si la función de extracción falló y devolvió un DataFrame vacío.
        print("\n--- PROCESO FINALIZADO CON ERRORES: No se subió ningún archivo a GCS. ---")