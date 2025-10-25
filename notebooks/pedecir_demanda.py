import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from google.cloud import bigquery
import pickle 

# ============================================================================
# 1. CARGA DE DATOS HISTÓRICOS (DF)
# ============================================================================
# Esta sección carga tu DF histórico desde BigQuery
print("⏳ Conectando a BigQuery y cargando datos históricos...")
client = bigquery.Client(project='datamanagementbi')
query = """
    SELECT *
    FROM `datamanagementbi.gold_data.modelo_final`
"""
df = client.query(query).to_dataframe()
print(f"✅ Datos históricos (df) cargados: {len(df)} filas.")


# ============================================================================
# 2. CARGA DE MODELOS ENTRENADOS (PICKLE)
# ============================================================================
archivo_modelos = 'modelos_entrenados.pkl' # modelo entrenado del archivo anterior
resultados_modelos = {}
modelos_entrenados = {}

try:
    with open(archivo_modelos, 'rb') as file:
        resultados_modelos = pickle.load(file)
    print(f"✅ Modelos cargados correctamente desde {archivo_modelos}")

    # Estructurar los modelos para el constructor de la clase
    modelos_entrenados = {sector: res['modelo'] 
                          for sector, res in resultados_modelos.items()}
    
except FileNotFoundError:
    print(f"❌ ERROR: No se encontró el archivo de modelos '{archivo_modelos}'. "
          "Asegúrate de que está en el directorio correcto y el nombre es el mismo.")
    exit() # Detiene la ejecución si no hay modelos

# ============================================================================
# SISTEMA DE PREDICCIÓN DE DEMANDA ENERGÉTICA
# ============================================================================

class PredictorDemandaEnergetica:
    """
    Sistema de predicción de demanda energética que utiliza modelos
    entrenados por sector económico.
    """
    
    def __init__(self, modelos_entrenados, df_historico):
        """
        Inicializar con los modelos ya entrenados y datos históricos.
        
        Parameters:
        -----------
        modelos_entrenados : dict
            Diccionario con modelos por sector: {'Industrial': modelo, ...}
        df_historico : DataFrame
            DataFrame con datos históricos para estadísticas
        """
        self.modelos = modelos_entrenados
        self.df_historico = df_historico
        
        # Mapeo de sectores
        self.sector_map = {
            'Industrial': 1,
            'Residencial': 2,
            'Servicios': 3
        }
        
        # Mapeo de tramos horarios
        self.tramo_horario_map = {
            'madrugada': 1,  # 00-06
            'mañana': 2,     # 06-12
            'tarde': 3,      # 12-18
            'noche': 4       # 18-00
        }
        
        print("✅ Predictor de Demanda inicializado correctamente")
    
    def _hora_a_tramo(self, hora):
        """Convierte hora (0-23) a tramo horario (1-4)"""
        if 0 <= hora < 6:
            return 1  # Madrugada
        elif 6 <= hora < 12:
            return 2  # Mañana
        elif 12 <= hora < 18:
            return 3  # Tarde
        else:
            return 4  # Noche
    
    def _es_fin_de_semana(self, fecha):
        """Determina si una fecha es fin de semana"""
        return fecha.weekday() >= 5  # 5=sábado, 6=domingo
    
    def _obtener_poblacion(self, codigo_postal, sector):
        """Obtiene la población promedio para un código postal"""
        df_sector = self.df_historico[
            self.df_historico['id_sector_economico'] == self.sector_map[sector]
        ]
        
        poblacion = df_sector[
            df_sector['id_geografia'] == codigo_postal
        ]['poblacion'].mean()
        
        # Si no hay datos, usar promedio del sector
        if pd.isna(poblacion):
            poblacion = df_sector['poblacion'].mean()
        
        return poblacion
    
    def predecir_demanda(self, 
                         codigo_postal, 
                         sector, 
                         fecha, 
                         hora=None,
                         tramo_horario=None,
                         temperatura=None, 
                         humedad=None,
                         es_festivo=False):
        """
        Predice la demanda energética para un escenario específico.
        
        Parameters:
        -----------
        codigo_postal : int
            Código postal de la zona
        sector : str
            'Industrial', 'Residencial' o 'Servicios'
        fecha : str o datetime
            Fecha de la predicción (formato 'YYYY-MM-DD' o datetime)
        hora : int, optional
            Hora del día (0-23). Si se proporciona, se calculará tramo_horario
        tramo_horario : str, optional
            'madrugada', 'mañana', 'tarde', 'noche'. Si no se da, se usa hora
        temperatura : float, optional
            Temperatura media esperada en °C
        humedad : float, optional
            Humedad media esperada en %
        es_festivo : bool
            Si el día es festivo
            
        Returns:
        --------
        dict con predicción, intervalo de confianza y factores
        """
        
        # Validar sector
        if sector not in self.modelos:
            raise ValueError(f"Sector '{sector}' no válido. Opciones: {list(self.modelos.keys())}")
        
        # Convertir fecha a datetime si es string
        if isinstance(fecha, str):
            fecha = datetime.strptime(fecha, '%Y-%m-%d')
        
        # Determinar tramo horario
        if hora is not None:
            id_tramo = self._hora_a_tramo(hora)
        elif tramo_horario is not None:
            id_tramo = self.tramo_horario_map[tramo_horario.lower()]
        else:
            # Por defecto, usar tarde (12-18)
            id_tramo = 3
        
        # Extraer componentes de fecha
        mes = fecha.month
        dia_mes = fecha.day
        es_fds = self._es_fin_de_semana(fecha)
        
        # Obtener población
        poblacion = self._obtener_poblacion(codigo_postal, sector)
        
        # Obtener valores históricos para temperatura/humedad si no se proporcionan
        if temperatura is None or humedad is None:
            df_hist = self.df_historico[
                (self.df_historico['id_geografia'] == codigo_postal) &
                (self.df_historico['id_sector_economico'] == self.sector_map[sector]) &
                (self.df_historico['mes'] == mes)
            ]
            
            if temperatura is None:
                temperatura = df_hist['temperatura_media_ciudad'].mean()
                if pd.isna(temperatura):
                    # Promedio general del mes
                    temperatura = self.df_historico[
                        self.df_historico['mes'] == mes
                    ]['temperatura_media_ciudad'].mean()
            
            if humedad is None:
                humedad = df_hist['humedad_media_ciudad'].mean()
                if pd.isna(humedad):
                    humedad = self.df_historico[
                        self.df_historico['mes'] == mes
                    ]['humedad_media_ciudad'].mean()
        
        # Crear features engineered
        temp_cuadrado = temperatura ** 2
        dist_confort = abs(temperatura - 20)
        temp_x_humedad = temperatura * humedad
        
        # Crear DataFrame con las features
        X_pred = pd.DataFrame([{
            'id_geografia': codigo_postal,
            'id_tramo_horario': id_tramo,
            'temperatura_media_ciudad': temperatura,
            'humedad_media_ciudad': humedad,
            'mes': mes,
            'dia_del_mes': dia_mes,
            'es_fin_de_semana': int(es_fds),
            'es_festivo': int(es_festivo),
            'poblacion': poblacion,
            'temp_cuadrado': temp_cuadrado,
            'dist_confort': dist_confort,
            'temp_x_humedad': temp_x_humedad
        }])
        
        # Realizar predicción
        modelo = self.modelos[sector]
        prediccion = modelo.predict(X_pred)[0]
        
        # Calcular intervalo de confianza basado en error histórico
        df_sector = self.df_historico[
            self.df_historico['id_sector_economico'] == self.sector_map[sector]
        ]
        
        # Usar desviación estándar del consumo como proxy del error
        error_std = df_sector['consumo_kwh'].std() * 0.1  # 10% de la std como margen
        
        intervalo_confianza = {
            'inferior': max(0, prediccion - 1.96 * error_std),
            'superior': prediccion + 1.96 * error_std
        }
        
        # Preparar resultado
        resultado = {
            'prediccion_kw': prediccion,
            'intervalo_95': intervalo_confianza,
            'input': {
                'codigo_postal': codigo_postal,
                'sector': sector,
                'fecha': fecha.strftime('%Y-%m-%d'),
                'hora': hora if hora is not None else f"Tramo {id_tramo}",
                'temperatura': temperatura,
                'humedad': humedad,
                'es_fin_de_semana': es_fds,
                'es_festivo': es_festivo
            },
            'factores': {
                'poblacion_zona': poblacion,
                'mes': mes,
                'tramo_horario': id_tramo
            }
        }
        
        return resultado
    
    def predecir_periodo(self, 
                        codigo_postal, 
                        sector, 
                        fecha_inicio, 
                        fecha_fin,
                        temperatura_promedio=None,
                        humedad_promedio=None):
        """
        Predice la demanda para un período de días.
        
        Returns:
        --------
        DataFrame con predicciones diarias
        """
        
        if isinstance(fecha_inicio, str):
            fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
        if isinstance(fecha_fin, str):
            fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d')
        
        predicciones = []
        fecha_actual = fecha_inicio
        
        while fecha_actual <= fecha_fin:
            # Predecir para cada tramo horario del día
            consumo_dia = 0
            
            for tramo in range(1, 5):
                pred = self.predecir_demanda(
                    codigo_postal=codigo_postal,
                    sector=sector,
                    fecha=fecha_actual,
                    tramo_horario=list(self.tramo_horario_map.keys())[tramo-1],
                    temperatura=temperatura_promedio,
                    humedad=humedad_promedio
                )
                consumo_dia += pred['prediccion_kw']
            
            predicciones.append({
                'fecha': fecha_actual,
                'consumo_total_dia': consumo_dia,
                'es_fin_de_semana': self._es_fin_de_semana(fecha_actual)
            })
            
            fecha_actual += timedelta(days=1)
        
        return pd.DataFrame(predicciones)
    
    def comparar_escenarios(self, codigo_postal, sector, fecha, temperaturas):
        """
        Compara predicciones bajo diferentes escenarios de temperatura.
        
        Parameters:
        -----------
        temperaturas : list
            Lista de temperaturas a comparar
            
        Returns:
        --------
        DataFrame con comparación de escenarios
        """
        
        resultados = []
        
        for temp in temperaturas:
            pred = self.predecir_demanda(
                codigo_postal=codigo_postal,
                sector=sector,
                fecha=fecha,
                temperatura=temp
            )
            
            resultados.append({
                'temperatura': temp,
                'consumo_predicho': pred['prediccion_kw'],
                'intervalo_inferior': pred['intervalo_95']['inferior'],
                'intervalo_superior': pred['intervalo_95']['superior']
            })
        
        return pd.DataFrame(resultados)


# ============================================================================
# EJEMPLO DE USO
# ============================================================================

print("\n" + "=" * 80)
print("📊 EJEMPLO DE USO DEL SISTEMA DE PREDICCIÓN")
print("=" * 80)

# Nota: Este ejemplo asume que ya tienes los modelos entrenados guardados
# en la variable 'resultados_modelos' del script anterior

# Suponiendo que ya ejecutaste el script anterior y tienes:
# - resultados_modelos: dict con los modelos entrenados
# - df: DataFrame con datos históricos

if 'resultados_modelos' in globals() and 'df' in globals():
    
    # Extraer solo los modelos
    modelos = {sector: res['modelo'] for sector, res in resultados_modelos.items()}
    
    # Crear instancia del predictor
    predictor = PredictorDemandaEnergetica(modelos, df)
    
    print("\n" + "=" * 80)
    print("🎯 EJEMPLO 1: Predicción individual")
    print("=" * 80)
    
    # Predicción para un caso específico
    resultado = predictor.predecir_demanda(
        codigo_postal=8001,  # Ajustar a un código postal real de tu dataset
        sector='Residencial',
        fecha='2025-07-15',
        hora=18,  # 6 PM
        temperatura=28,  # Día caluroso de verano
        humedad=65,
        es_festivo=False
    )
    
    print(f"\n📍 Código Postal: {resultado['input']['codigo_postal']}")
    print(f"🏠 Sector: {resultado['input']['sector']}")
    print(f"📅 Fecha: {resultado['input']['fecha']}")
    print(f"🕐 Hora: {resultado['input']['hora']}")
    print(f"🌡️ Temperatura: {resultado['input']['temperatura']}°C")
    print(f"💧 Humedad: {resultado['input']['humedad']}%")
    
    print(f"\n⚡ PREDICCIÓN:")
    print(f"   Consumo esperado: {resultado['prediccion_kw']:,.0f} kW")
    print(f"   Intervalo 95% confianza:")
    print(f"   └─ Inferior: {resultado['intervalo_95']['inferior']:,.0f} kW")
    print(f"   └─ Superior: {resultado['intervalo_95']['superior']:,.0f} kW")
    
    print("\n" + "=" * 80)
    print("📈 EJEMPLO 2: Análisis de sensibilidad a temperatura")
    print("=" * 80)
    
    # Comparar diferentes temperaturas
    temperaturas = [5, 10, 15, 20, 25, 30, 35]
    
    comparacion = predictor.comparar_escenarios(
        codigo_postal=8001,
        sector='Residencial',
        fecha='2025-07-15',
        temperaturas=temperaturas
    )
    
    print("\n🌡️ Impacto de la temperatura en el consumo:")
    print(comparacion.to_string(index=False))
    
    # Visualización
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(comparacion['temperatura'], comparacion['consumo_predicho'], 
            marker='o', linewidth=2, markersize=8, label='Predicción')
    ax.fill_between(comparacion['temperatura'],
                     comparacion['intervalo_inferior'],
                     comparacion['intervalo_superior'],
                     alpha=0.3, label='Intervalo 95%')
    
    ax.set_xlabel('Temperatura (°C)', fontsize=12)
    ax.set_ylabel('Consumo Predicho (kW)', fontsize=12)
    ax.set_title('Sensibilidad del Consumo a la Temperatura\nSector Residencial',
                fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(alpha=0.3)
    
    # Marcar zona de confort
    ax.axvspan(18, 22, alpha=0.2, color='green', label='Zona confort')
    
    plt.tight_layout()
    plt.show()
    
    print("\n" + "=" * 80)
    print("📅 EJEMPLO 3: Predicción para una semana")
    print("=" * 80)
    
    # Predicción semanal
    prediccion_semanal = predictor.predecir_periodo(
        codigo_postal=8001,
        sector='Residencial',
        fecha_inicio='2025-07-14',
        fecha_fin='2025-07-20',
        temperatura_promedio=25,
        humedad_promedio=60
    )
    
    print("\n📊 Predicción semanal:")
    prediccion_semanal['fecha_str'] = prediccion_semanal['fecha'].dt.strftime('%Y-%m-%d')
    prediccion_semanal['dia_semana'] = prediccion_semanal['fecha'].dt.day_name()
    print(prediccion_semanal[['fecha_str', 'dia_semana', 'consumo_total_dia', 'es_fin_de_semana']].to_string(index=False))
    
    # Visualización semanal
    fig, ax = plt.subplots(figsize=(12, 6))
    
    colores = ['steelblue' if not fds else 'orange' 
               for fds in prediccion_semanal['es_fin_de_semana']]
    
    bars = ax.bar(prediccion_semanal['fecha_str'], 
                  prediccion_semanal['consumo_total_dia'],
                  color=colores)
    
    ax.set_xlabel('Fecha', fontsize=12)
    ax.set_ylabel('Consumo Total Diario (kW)', fontsize=12)
    ax.set_title('Predicción de Consumo Semanal\nSector Residencial',
                fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3)
    
    # Leyenda
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='steelblue', label='Día laboral'),
        Patch(facecolor='orange', label='Fin de semana')
    ]
    ax.legend(handles=legend_elements)
    
    plt.tight_layout()
    plt.show()
    
    print("\n" + "=" * 80)
    print("✨ Sistema de predicción listo para producción")
    print("=" * 80)
    
else:
    print("\n⚠️ Primero ejecuta el script de entrenamiento de modelos")
    print("   para tener 'resultados_modelos' y 'df' disponibles")