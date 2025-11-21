import stream as st
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# --- CONFIGURACIÓN DE MONGODB (AJUSTA ESTOS VALORES) ---
MONGO_USER = "root"
MONGO_PASS = "example"
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "copa2024" 
EVENTS_COLLECTION = "events" # Colección que contiene la llave 'location'

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
# --------------------------------------------------------

# --- 1. FUNCIÓN DE CONEXIÓN Y CARGA DE DATOS ---
@st.cache_data
def load_events_data():
    """Conecta a MongoDB, extrae eventos con ubicación y los convierte a DataFrame."""
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        coleccion_events = db[EVENTS_COLLECTION]
        
        # Filtramos solo los documentos que tienen la llave 'location'
        # y solo extraemos 'location', 'type' y 'player' para optimizar
        data = coleccion_events.find(
            {"location": {"$exists": True}},
            {"location": 1, "type": 1, "player": 1, "_id": 0}
        )
        
        df = pd.DataFrame(list(data))
        
        # Aseguramos que la columna 'location' sea una lista (coordenadas [x, y])
        if not df.empty and isinstance(df['location'].iloc[0], list):
            # Separar las coordenadas X y Y en columnas separadas
            df[['x', 'y']] = pd.DataFrame(df['location'].tolist(), index=df.index)
        else:
            st.error("Error: Los datos de ubicación no tienen el formato [x, y] esperado o están vacíos.")
            return pd.DataFrame() # Devuelve un DataFrame vacío si falla la extracción

        return df

    except Exception as e:
        st.error(f"Error al conectar o cargar datos de MongoDB: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()

# --- 2. FUNCIÓN DE DIBUJO DEL CAMPO DE JUEGO ---
def draw_pitch(ax):
    """Dibuja los elementos básicos de un campo de fútbol de 120x80."""
    # Líneas del campo (dimensiones de StatsBomb: 120x80)
    ax.plot([0, 0], [0, 80], color="black") # Línea de fondo izquierda
    ax.plot([0, 120], [80, 80], color="black") # Línea lateral superior
    ax.plot([120, 120], [80, 0], color="black") # Línea de fondo derecha
    ax.plot([120, 0], [0, 0], color="black") # Línea lateral inferior
    ax.plot([60, 60], [0, 80], color="black") # Línea de medio campo
    ax.add_patch(plt.Circle((60, 40), 10, color="black", fill=False)) # Círculo central

    # Área de penal (Izquierda)
    ax.plot([18, 18], [18, 62], color="black")
    ax.plot([0, 18], [18, 18], color="black")
    ax.plot([0, 18], [62, 62], color="black")

    # Área chica (Izquierda)
    ax.plot([6, 6], [30, 50], color="black")
    ax.plot([0, 6], [30, 30], color="black")
    ax.plot([0, 6], [50, 50], color="black")

    # Área de penal (Derecha)
    ax.plot([102, 102], [18, 62], color="black")
    ax.plot([120, 102], [18, 18], color="black")
    ax.plot([120, 102], [62, 62], color="black")

    # Área chica (Derecha)
    ax.plot([114, 114], [30, 50], color="black")
    ax.plot([120, 114], [30, 30], color="black")
    ax.plot([120, 114], [50, 50], color="black")
    
    # Puntos de penal
    ax.scatter(12, 40, color="black", marker='o', s=20)
    ax.scatter(108, 40, color="black", marker='o', s=20)

    ax.set_xlim(0, 120)
    ax.set_ylim(0, 80)
    ax.set_aspect('equal', adjustable='box')
    ax.set_facecolor("mediumseagreen") # Fondo verde del campo

# --- 3. LÓGICA DE STREAMLIT ---

st.title("🗺️ Mapeo de Eventos en el Campo de Juego")

# Cargar los datos
df_events = load_events_data()

if not df_events.empty:
    
    # 3a. Filtros de la Interfaz
    # Obtener la lista de tipos de eventos únicos
    event_types = df_events['type'].unique().tolist()
    event_types.sort()

    selected_types = st.multiselect(
        "Filtrar por Tipo de Evento:",
        options=event_types,
        default=event_types # Por defecto, selecciona todos
    )
    
    # Aplicar el filtro
    df_filtered = df_events[df_events['type'].isin(selected_types)]
    
    st.write(f"Mostrando {len(df_filtered)} de {len(df_events)} eventos cargados.")

    # 3b. Generación del Gráfico
    
    fig, ax = plt.subplots(figsize=(10, 7))
    
    # Dibujar el campo de juego
    draw_pitch(ax)

    # 3c. Dibujar los puntos de los eventos
    
    # Usamos scatter para mapear los puntos
    ax.scatter(
        df_filtered['x'], 
        df_filtered['y'], 
        alpha=0.6, 
        s=40, # Tamaño de los puntos
        label='Eventos',
        color='gold' # Color de los puntos
    )

    ax.set_title("Ubicación de Eventos en el Campo de Fútbol (120x80)")
    ax.set_xlabel("Coordenada X (Largo del Campo)")
    ax.set_ylabel("Coordenada Y (Ancho del Campo)")

    # Mostrar la figura en Streamlit
    st.pyplot(fig)

else:
    st.warning("No se pudieron cargar los datos de eventos. Verifica tu conexión a MongoDB y la estructura de la columna 'location'.")