from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import json
import ast
import pickle
from pathlib import Path
from pymongo import MongoClient, InsertOne

# Configuración
MONGO_URI = "mongodb://root:example@mongo:27017"
DB_NAME = "copa2024"
EVENTS_COLLECTION = "events"
TEAMS_COLLECTION = "teams"
PLAYERS_COLLECTION = "players"
MATCHES_COLLECTION = "matches"

JSON_PATH = Path("/opt/airflow/data/events_clean.json")
TEMP_DIR = Path("/opt/airflow/data/temp")

# Crear directorio temporal si no existe
TEMP_DIR.mkdir(parents=True, exist_ok=True)

LIST_COLS = [
    "location",
    "pass_end_location",
    "carry_end_location",
    "shot_end_location",
    "related_events",
]

DICT_COLS = [
    "tactics",
]

# Argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def parse_maybe_literal(value):
    """Si es string tipo '[1,2]' o '{...}' intenta convertirlo; si no, lo deja igual."""
    if isinstance(value, str):
        v = value.strip()
        if (v.startswith("[") and v.endswith("]")) or \
           (v.startswith("{") and v.endswith("}")) or \
           (v.startswith("(") and v.endswith(")")):
            try:
                return ast.literal_eval(v)
            except (SyntaxError, ValueError):
                return value
    return value


def extract_prefixed_subdoc(ev: dict, prefix: str) -> dict:
    """Extrae subdocumentos con prefijos."""
    subdoc = ev.get(prefix, {})
    prefix_str = prefix + "_"

    for k, v in ev.items():
        if k.startswith(prefix_str):
            subkey = k[len(prefix_str):]
            subdoc[subkey] = parse_maybe_literal(v)

    if subdoc:
        ev[prefix] = subdoc

    return ev


def clean_event(ev: dict) -> dict:
    """Limpia y transforma un evento individual."""
    ev = ev.copy()

    for c in LIST_COLS:
        if c in ev:
            ev[c] = parse_maybe_literal(ev[c])

    for c in DICT_COLS:
        if c in ev:
            ev[c] = parse_maybe_literal(ev[c])

    for prefix in ["pass", "shot", "carry", "dribble", "clearance"]:
        ev = extract_prefixed_subdoc(ev, prefix)

    for id_field in ["player_id", "pass_recipient_id", "team_id", "match_id"]:
        if id_field in ev and isinstance(ev[id_field], float) and ev[id_field].is_integer():
            ev[id_field] = int(ev[id_field])

    if "id" in ev and "event_id" not in ev:
        ev["event_id"] = ev["id"]

    return ev


def extract_data(**context):
    """Fase de extracción: Lee el archivo JSON y lo guarda en archivo temporal."""
    if not JSON_PATH.exists():
        raise FileNotFoundError(f"No se encontró el archivo JSON en {JSON_PATH.resolve()}")

    print(f"Cargando JSON desde: {JSON_PATH.resolve()}")

    with JSON_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        events = data
    elif isinstance(data, dict) and "events" in data:
        events = data["events"]
    else:
        raise ValueError(
            "Formato JSON no reconocido. Se esperaba una lista de eventos "
            "o un dict con clave 'events'."
        )

    print(f"Eventos totales extraídos: {len(events)}")
    
    # Guardar eventos en archivo temporal usando pickle
    temp_file = TEMP_DIR / f"raw_events_{context['run_id']}.pkl"
    with open(temp_file, 'wb') as f:
        pickle.dump(events, f)
    
    print(f"Eventos guardados en: {temp_file}")
    
    # Guardar solo la ruta del archivo en XCom (pequeño)
    context['task_instance'].xcom_push(key='temp_file', value=str(temp_file))
    
    return len(events)


def transform_data(**context):
    """Fase de transformación: Limpia eventos y genera colecciones agregadas."""
    # Recuperar ruta del archivo temporal
    temp_file = context['task_instance'].xcom_pull(key='temp_file', task_ids='extract')
    
    print(f"Cargando eventos desde: {temp_file}")
    
    # Cargar eventos desde archivo temporal
    with open(temp_file, 'rb') as f:
        events = pickle.load(f)
    
    print(f"Transformando {len(events)} eventos...")

    teams_seen = {}
    players_seen = {}
    matches_seen = {}
    events_clean = []

    for ev in events:
        ev_clean = clean_event(ev)

        team_id = ev_clean.get("team_id")
        team_name = ev_clean.get("team")
        player_id = ev_clean.get("player_id")
        player_name = ev_clean.get("player")
        player_position = ev_clean.get("position")
        match_id = ev_clean.get("match_id")

        # Agregar datos de equipos
        if team_id is not None and team_name:
            if team_id not in teams_seen:
                teams_seen[team_id] = {
                    "name": team_name,
                    "players": set(),
                    "matches": set(),
                }
            tdata = teams_seen[team_id]
            if match_id is not None:
                tdata["matches"].add(match_id)
            if player_id is not None:
                tdata["players"].add(player_id)

        # Agregar datos de jugadores
        if player_id is not None and player_name:
            if player_id not in players_seen:
                players_seen[player_id] = {
                    "name": player_name,
                    "teams": {},
                    "positions": set(),
                    "matches": set(),
                }

            pdata = players_seen[player_id]

            if not pdata["name"]:
                pdata["name"] = player_name

            if team_id is not None and team_name:
                pdata["teams"][team_id] = team_name

            if player_position:
                pdata["positions"].add(player_position)

            if match_id is not None:
                pdata["matches"].add(match_id)

        # Agregar datos de partidos
        if match_id is not None:
            if match_id not in matches_seen:
                matches_seen[match_id] = {
                    "teams": {},
                    "player_ids": set()
                }

            mdata = matches_seen[match_id]

            if team_id is not None and team_name:
                mdata["teams"][team_id] = team_name

            if player_id is not None:
                mdata["player_ids"].add(player_id)

        if "id" in ev_clean:
            ev_clean["event_id"] = ev_clean["id"]

        events_clean.append(ev_clean)

    # Convertir sets a listas para serialización
    team_docs = []
    for tid, tdata in teams_seen.items():
        doc = {
            "team_id": tid,
            "name": tdata["name"],
            "players": sorted(tdata["players"]),
            "matches": sorted(tdata["matches"]),
        }
        team_docs.append(doc)

    player_docs = []
    for pid, pdata in players_seen.items():
        teams_list = [
            {"team_id": tid, "name": tname}
            for tid, tname in pdata["teams"].items()
        ]
        doc = {
            "player_id": pid,
            "name": pdata["name"],
            "teams": teams_list,
            "positions": sorted(pdata["positions"]),
            "matches": sorted(pdata["matches"]),
        }
        player_docs.append(doc)

    match_docs = []
    for mid, mdata in matches_seen.items():
        teams_list = [
            {"team_id": tid, "name": tname}
            for tid, tname in mdata["teams"].items()
        ]
        doc = {
            "match_id": mid,
            "teams": teams_list,
            "players": sorted(mdata["player_ids"]),
        }
        match_docs.append(doc)

    print(f"Transformación completada: {len(events_clean)} eventos, "
          f"{len(team_docs)} equipos, {len(player_docs)} jugadores, "
          f"{len(match_docs)} partidos")

    # Guardar datos transformados en archivos temporales
    run_id = context['run_id']
    events_file = TEMP_DIR / f"events_clean_{run_id}.pkl"
    teams_file = TEMP_DIR / f"teams_{run_id}.pkl"
    players_file = TEMP_DIR / f"players_{run_id}.pkl"
    matches_file = TEMP_DIR / f"matches_{run_id}.pkl"
    
    with open(events_file, 'wb') as f:
        pickle.dump(events_clean, f)
    with open(teams_file, 'wb') as f:
        pickle.dump(team_docs, f)
    with open(players_file, 'wb') as f:
        pickle.dump(player_docs, f)
    with open(matches_file, 'wb') as f:
        pickle.dump(match_docs, f)
    
    print(f"Datos transformados guardados en archivos temporales")
    
    # Guardar rutas en XCom
    context['task_instance'].xcom_push(key='events_file', value=str(events_file))
    context['task_instance'].xcom_push(key='teams_file', value=str(teams_file))
    context['task_instance'].xcom_push(key='players_file', value=str(players_file))
    context['task_instance'].xcom_push(key='matches_file', value=str(matches_file))

    return {
        'events': len(events_clean),
        'teams': len(team_docs),
        'players': len(player_docs),
        'matches': len(match_docs)
    }


def load_data(**context):
    """Fase de carga: Inserta los datos transformados en MongoDB."""
    # Recuperar rutas de archivos temporales
    events_file = context['task_instance'].xcom_pull(key='events_file', task_ids='transform')
    teams_file = context['task_instance'].xcom_pull(key='teams_file', task_ids='transform')
    players_file = context['task_instance'].xcom_pull(key='players_file', task_ids='transform')
    matches_file = context['task_instance'].xcom_pull(key='matches_file', task_ids='transform')
    
    print("Cargando datos desde archivos temporales...")
    
    # Cargar datos desde archivos
    with open(events_file, 'rb') as f:
        events_clean = pickle.load(f)
    with open(teams_file, 'rb') as f:
        team_docs = pickle.load(f)
    with open(players_file, 'rb') as f:
        player_docs = pickle.load(f)
    with open(matches_file, 'rb') as f:
        match_docs = pickle.load(f)

    print("Conectando a MongoDB...")
    client = MongoClient(MONGO_URI, authSource="admin")
    db = client[DB_NAME]

    col_events = db[EVENTS_COLLECTION]
    col_teams = db[TEAMS_COLLECTION]
    col_players = db[PLAYERS_COLLECTION]
    col_matches = db[MATCHES_COLLECTION]

    # Limpiar colecciones existentes
    col_events.drop()
    col_teams.drop()
    col_players.drop()
    col_matches.drop()
    print("Colecciones events, teams, players, matches borradas.")

    # Insertar eventos
    if events_clean:
        bulk_ops = [InsertOne(ev) for ev in events_clean]
        result = col_events.bulk_write(bulk_ops, ordered=False)
        print(f"Insertados {result.inserted_count} eventos en '{EVENTS_COLLECTION}'.")
    else:
        print("No hay eventos para insertar.")

    # Insertar equipos
    if team_docs:
        col_teams.insert_many(team_docs)
        print(f"Insertados {len(team_docs)} equipos en '{TEAMS_COLLECTION}'.")

    # Insertar jugadores
    if player_docs:
        col_players.insert_many(player_docs)
        print(f"Insertados {len(player_docs)} jugadores en '{PLAYERS_COLLECTION}'.")

    # Insertar partidos
    if match_docs:
        col_matches.insert_many(match_docs)
        print(f"Insertados {len(match_docs)} partidos en '{MATCHES_COLLECTION}'.")

    print("Carga a MongoDB finalizada.")
    
    # Limpiar archivos temporales
    try:
        Path(events_file).unlink()
        Path(teams_file).unlink()
        Path(players_file).unlink()
        Path(matches_file).unlink()
        
        # También eliminar el archivo de raw_events
        raw_file = context['task_instance'].xcom_pull(key='temp_file', task_ids='extract')
        if raw_file:
            Path(raw_file).unlink()
        
        print("Archivos temporales eliminados.")
    except Exception as e:
        print(f"Advertencia: No se pudieron eliminar archivos temporales: {e}")

    return {
        'events_loaded': len(events_clean) if events_clean else 0,
        'teams_loaded': len(team_docs) if team_docs else 0,
        'players_loaded': len(player_docs) if player_docs else 0,
        'matches_loaded': len(match_docs) if match_docs else 0
    }


# Definición del DAG
with DAG(
    'copa2024_etl',
    default_args=default_args,
    description='ETL para cargar datos de eventos de Copa 2024 a MongoDB',
    schedule_interval=None,  # Ejecutar manualmente o cambiar según necesidad
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['copa2024', 'etl', 'mongodb'],
) as dag:

    # Tarea de extracción
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True,
    )

    # Tarea de transformación
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )

    # Tarea de carga
    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True,
    )

    # Definir el flujo ETL
    extract >> transform >> load