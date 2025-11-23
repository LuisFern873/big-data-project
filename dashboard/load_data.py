import pandas as pd
from pymongo import MongoClient
import streamlit as st
from dotenv import load_dotenv
import os

load_dotenv()

# --- CONFIGURACIÓN ---
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
DB_NAME = os.getenv('DB_NAME')
EVENTS_COLLECTION = "events"
MATCHES_COLLECTION = "matches" 

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"

print(MONGO_URI)

# Clasificar eventos
event_categories = {
    "Pressure": "Defensive",
    "Interception": "Defensive",
    "Clearance": "Defensive",
    "Ball Recovery": "Defensive",
    "Block": "Defensive",
    "Duel": "Defensive",
    "Dispossessed": "Defensive",
    "Dribbled Past": "Defensive",
    "Foul Committed": "Defensive",
    "Own Goal Against": "Defensive",
    "50/50": "Defensive",  # Also appears in Control, but categorized as Defensive here
    "Pass": "Control",
    "Ball Receipt": "Control",
    "Carry": "Control",
    "Miscontrol": "Control",
    "Shield": "Control",
    "Dribble": "Offensive",
    "Foul Won": "Offensive",
    "Shot": "Offensive",
    "Own Goal For": "Offensive",
    "Error": "Offensive"  # Error is both Defensive and Offensive, but categorized as Offensive here
}

@st.cache_data
def load_matches_list():
    """Carga lista de partidos para el selector."""
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
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
        
        # Obtenemos eventos que tengan ubicación
        data = coleccion_events.find(
            {"match_id": match_id, "location": {"$exists": True}},
            {
                "location": 1, 
                "type": 1, 
                "player": 1, # Aquí viene el ID
                "player_id": 1,
                "team": 1, 
                "foul_committed_card": 1, 
                "shot_outcome": 1,
                "_id": 0
            }
        )
        
        df = pd.DataFrame(list(data))
        
        if not df.empty:
            # Procesar coordenadas
            if isinstance(df['location'].iloc[0], list):
                df[['x', 'y']] = pd.DataFrame(df['location'].tolist(), index=df.index)
            
            # Procesar nombre del equipo (puede ser dict o string)
            if 'team' in df.columns:
                df['team_name'] = df['team'].apply(lambda x: x.get('name') if isinstance(x, dict) else str(x))
  
            # Columnas opcionales
            if 'foul_committed_card' not in df.columns:
                df['foul_committed_card'] = None
            if 'shot_outcome' not in df.columns:
                df['shot_outcome'] = None

            df['event_category'] = df['type'].apply(lambda x: event_categories.get(x, "Unknown"))
            print(df.head())
            
        return df

    except Exception as e:
        st.error(f"Error al cargar eventos del partido: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()

@st.cache_data
def load_lineups(match_id):
    """Carga la alineación táctica para obtener mapeo ID -> Nombre/Posición."""
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        coleccion_events = db[EVENTS_COLLECTION]
        
        # Buscamos todos los eventos que tengan alineación (Starting XI o Tactical Shift)
        # para capturar a todos los jugadores que participaron.
        cursor = coleccion_events.find({
            "match_id": match_id, 
            "tactics.lineup": {"$exists": True}
        })
        
        # Diccionario para acumular jugadores únicos por equipo
        # Estructura: { "TeamName": { player_id: {data...}, ... } }
        teams_players = {}
        
        for doc in cursor:
            # Determinar nombre del equipo de forma segura
            team_val = doc.get('team')
            if isinstance(team_val, dict):
                team_name = team_val.get('name')
            else:
                team_name = str(team_val)
            
            if team_name not in teams_players:
                teams_players[team_name] = {}
            
            # Recorrer lineup del evento
            for p in doc['tactics']['lineup']:
                p_id = p['player']['id']
                # Guardamos/Sobreescribimos datos del jugador. 
                # Esto asegura que tengamos los datos más recientes o simplemente todos los únicos.
                teams_players[team_name][p_id] = {
                    "player_id": p_id,
                    "player_name": p['player']['name'],
                    "jersey_number": p['jersey_number'],
                    "position": p['position']['name']
                }


        # Convertir a DataFrames
        lineups_dict = {}
        for team, players_dict in teams_players.items():
            if players_dict:
                lineups_dict[team] = pd.DataFrame(list(players_dict.values()))
        return lineups_dict

    except Exception as e:
        st.error(f"Error al cargar alineaciones: {e}")
        return {}
    finally:
        if client:
            client.close()