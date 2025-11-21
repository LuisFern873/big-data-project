import pandas as pd
from pymongo import MongoClient
import streamlit as st

# --- CONFIGURACIÓN ---
MONGO_USER = "root"
MONGO_PASS = "example"
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "copa2024" 
EVENTS_COLLECTION = "events"
MATCHES_COLLECTION = "matches" 

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"

@st.cache_data
def load_matches_list():
    """Carga lista de partidos para el selector."""
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        # Traemos match_id y teams para armar el label
        data = db[MATCHES_COLLECTION].find(
            {}, 
            {"match_id": 1, "teams": 1, "_id": 0}
        )
        return list(data)
    except Exception as e:
        st.error(f"Error al cargar partidos: {e}")
        return []
    finally:
        if client:
            client.close()

@st.cache_data
def load_events_for_match(match_id):
    """Extrae eventos de un partido específico filtrando en Mongo."""
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        coleccion_events = db[EVENTS_COLLECTION]
        
        # AJUSTE: Proyección actualizada para incluir 'shot_outcome' directamente
        data = coleccion_events.find(
            {"match_id": match_id, "location": {"$exists": True}},
            {
                "location": 1, 
                "type": 1, 
                "player": 1, 
                "team": 1, 
                "foul_committed_card": 1, 
                "shot_outcome": 1, # Atributo directo según instrucción
                "_id": 0
            }
        )
        
        df = pd.DataFrame(list(data))
        
        if not df.empty:
            # Procesar coordenadas
            if isinstance(df['location'].iloc[0], list):
                df[['x', 'y']] = pd.DataFrame(df['location'].tolist(), index=df.index)
            
            # Procesar nombre del equipo
            if 'team' in df.columns:
                df['team_name'] = df['team'].apply(lambda x: x.get('name') if isinstance(x, dict) else str(x))
            
            # Asegurarnos que las columnas opcionales existan
            if 'foul_committed_card' not in df.columns:
                df['foul_committed_card'] = None
            
            if 'shot_outcome' not in df.columns:
                df['shot_outcome'] = None

        return df

    except Exception as e:
        st.error(f"Error al cargar eventos del partido: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()