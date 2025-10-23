# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import os
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient

def extract_contextual_events(text: str) -> list:
    """
    Analiza un texto utilizando Azure AI Language para extraer entidades relevantes.

    Esta función se conecta al servicio de Azure AI, envía un texto para su análisis
    mediante Reconocimiento de Entidades Nombradas (NER), y filtra los resultados
    para devolver únicamente las entidades de tipo 'Evento' y 'Ubicación'.

    Args:
        text (str): El texto a analizar (ej. el titular de una noticia).

    Returns:
        list: Una lista de diccionarios, donde cada diccionario representa un evento
              encontrado y contiene las claves 'text' y 'category'.
              Devuelve una lista vacía si no se encuentran eventos o hay un error.
    """
    try:
        # --- 1. CONFIGURACIÓN Y AUTENTICACIÓN ---
        load_dotenv()
        azure_endpoint = os.getenv("https://lang-dmbi-project.cognitiveservices.azure.com/")
        azure_key = os.getenv("2hpOTws3hFureCPyOtUlCFMu1Dm3gt7KlzJBOBTDRMsNE6rOH90VJQQJ99BJAC5RqLJXJ3w3AAAaACOGbdDW")

        if not all([azure_endpoint, azure_key]):
            print("Error: Asegúrate de que las variables AZURE_ENDPOINT y AZURE_KEY están en el archivo .env")
            return []

        credential = AzureKeyCredential(azure_key)
        text_analytics_client = TextAnalyticsClient(endpoint=azure_endpoint, credential=credential)

        # --- 2. LLAMADA A LA API DE AZURE ---
        documents = [text]
        result = text_analytics_client.recognize_entities(documents=documents)[0]

        # --- 3. FILTRADO Y PROCESAMIENTO DE ENTIDADES ---
        relevant_categories = {"Event", "Location"}
        found_events = []
        for entity in result.entities:
            if entity.category in relevant_categories:
                found_events.append({
                    "text": entity.text,
                    "category": entity.category
                })
        
        return found_events

    except Exception as e:
        print(f"Ha ocurrido un error al procesar el texto: {e}")
        return []

# --- BLOQUE DE EJEMPLO ---
if __name__ == "__main__":
    sample_text = "El FC Barcelona juega la final de la Champions en el Camp Nou este sábado."
    print(f"Analizando texto de ejemplo: '{sample_text}'")
    events = extract_contextual_events(sample_text)
    
    if events:
        print("Eventos encontrados:")
        for event in events:
            print(f"- Texto: {event['text']}, Categoría: {event['category']}")
    else:
        print("No se encontraron eventos relevantes.")