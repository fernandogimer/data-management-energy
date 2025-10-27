# ============================================================================
# SCRIPT DE PREDICCIÓN DE DEMANDA ENERGÉTICA
# ============================================================================
# Autor: Julio Clavijo
# Versión: 1.0
#
# Descripción:
# Este script carga los modelos segmentados entrenados por 'train_model_final.py'
# y los utiliza para realizar predicciones de demanda para escenarios específicos.
# ============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import pickle
import warnings

warnings.filterwarnings('ignore', category=FutureWarning)

# --- 1. CARGA DE MODELOS ENTRENADOS (PICKLE) ---
print("--- Iniciando Sistema de Predicción de Demanda ---")
print("\nPaso 1: Cargando modelos entrenados...")

MODEL_FILE = 'modelos_entrenados_por_sector.pkl'
try:
    with open(MODEL_FILE, 'rb') as file:
        resultados_cargados = pickle.load(file)
    
    # Extraemos solo el objeto del modelo de la estructura guardada
    modelos_entrenados = {sector: res['modelo'] for sector, res in resultados_cargados.items()}
    print(f"✅ Modelos para los sectores {list(modelos_entrenados.keys())} cargados correctamente.")
    
except FileNotFoundError:
    print(f"❌ ERROR: No se encontró el archivo de modelos '{MODEL_FILE}'.")
    print("   Asegúrate de ejecutar primero 'train_model_final.py' para generar este archivo.")
    exit()

# --- 2. DEFINICIÓN DE LA CLASE PREDICTORA ---

class PredictorDemanda:
    def __init__(self, modelos):
        self.modelos = modelos
        self.tramo_horario_map = {1: '00-06h', 2: '06-12h', 3: '12-18h', 4: '18-00h'}

    def _preparar_features(self, datos_entrada, historicos):
        """Prepara el DataFrame de una fila para la predicción."""
        
        # Copiamos para no modificar el original
        df_pred = datos_entrada.copy()

        # Añadir features temporales (lags) usando los datos históricos
        # ¡IMPORTANTE! Para una predicción real, necesitaríamos los datos reales de las últimas horas/días.
        # Aquí lo SIMULAMOS usando el promedio histórico como una aproximación razonable.
        df_pred['consumo_lag_1_hora'] = historicos['consumo_kwh'].mean()
        df_pred['consumo_lag_2_horas'] = historicos['consumo_kwh'].mean()
        df_pred['consumo_lag_1_dia'] = historicos['consumo_kwh'].mean()
        df_pred['consumo_media_movil_7d'] = historicos['consumo_kwh'].mean()

        # Añadir features no lineales
        df_pred['temp_cuadrado'] = df_pred['temperatura_media_ciudad'] ** 2
        df_pred['dist_confort'] = abs(df_pred['temperatura_media_ciudad'] - 20)

        # Convertir tipos a categóricos (usando los códigos directamente)
        for col in ['id_geografia', 'id_tramo_horario', 'nombre_fiesta', 'dia_de_la_semana_nombre', 'nombre_barrio', 'nombre_distrito']:
            if col in df_pred.columns:
                 df_pred[col] = pd.Categorical(df_pred[col])
        
        return df_pred

    def predecir(self, sector, datos_entrada, historicos):
        """Realiza una predicción para un escenario dado."""
        
        if sector not in self.modelos:
            raise ValueError(f"Sector '{sector}' no válido. Modelos disponibles: {list(self.modelos.keys())}")
        
        modelo = self.modelos[sector]
        
        # Preparar las features para la predicción
        df_pred = self._preparar_features(datos_entrada, historicos)
        
        # Asegurar que las columnas están en el mismo orden que en el entrenamiento
        features_ordenadas = modelo.get_booster().feature_names
        df_pred = df_pred[features_ordenadas]
        
        # Realizar predicción
        prediccion = modelo.predict(df_pred)
        return prediccion[0]

# --- 3. EJEMPLO DE USO ---
if __name__ == "__main__":
    print("\nPaso 2: Realizando una predicción de ejemplo...")
    
    # Creamos una instancia del predictor
    predictor = PredictorDemanda(modelos_entrenados)

    # Definimos un escenario para el que queremos predecir
    # NOTA: Los datos históricos son una simplificación. En producción, se usarían datos reales.
    # Usaremos promedios como placeholder.
    datos_historicos_simulados = pd.DataFrame({'consumo_kwh': [20000, 21000]}) # Lo convertimos a un DataFrame
    
    escenario = pd.DataFrame([{
        'id_geografia': 8001,
        'id_tramo_horario': 4, # Tarde-noche (18-00h)
        'temperatura_media_ciudad': 28.5,
        'humedad_media_ciudad': 65.0,
        'mes': 7,
        'anio': 2025,
        'poblacion': 22000,
        'dia_del_mes': 15,
        'es_fin_de_semana': False,
        'es_festivo': False,
        'nombre_fiesta': 'Sin fiesta',
        'dia_de_la_semana_nombre': 'Tuesday',
        'nombre_barrio': 'El Gòtic',
        'nombre_distrito': 'Ciutat Vella',
        'temp_media_movil_3d': 27.0
    }])

    sector_a_predecir = 'Servicios'

    # Realizar la predicción
    try:
        consumo_predicho = predictor.predecir(
            sector=sector_a_predecir,
            datos_entrada=escenario,
            historicos=datos_historicos_simulados
        )
        print("\n" + "="*50)
        print("⚡ RESULTADO DE LA PREDICCIÓN ⚡")
        print("="*50)
        print(f"Sector: {sector_a_predecir}")
        print(f"Escenario (Temperatura): {escenario['temperatura_media_ciudad'].iloc[0]}°C")
        print(f"Consumo Predicho: {consumo_predicho:,.0f} kWh")
        print("="*50)
    except Exception as e:
        print(f"❌ Error durante la predicción: {e}")