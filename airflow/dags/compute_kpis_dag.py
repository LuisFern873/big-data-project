from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from pymongo import MongoClient
from datetime import datetime, timedelta

# Configuración
MONGO_URI = "mongodb://root:example@mongo:27017"
DB_NAME = "copa2024"
KPIS_COLLECTION = "kpis"


def compute_team_shots_kpis(**context):
    """Calcula KPIs de tiros por equipo."""
    client = MongoClient(MONGO_URI, authSource="admin")
    db = client[DB_NAME]

    events = db["events"]
    col_kpis = db[KPIS_COLLECTION]

    pipeline = [
        {"$match": {"type": "Shot"}},
        {"$group": {
            "_id": {
                "team_id": "$team_id",
                "team": "$team"
            },
            "total_shots": {"$sum": 1},
            "matches": {"$addToSet": "$match_id"}
        }},
        {"$project": {
            "_id": 0,
            "team_id": "$_id.team_id",
            "team": "$_id.team",
            "total_shots": 1,
            "matches_count": {"$size": "$matches"},
            "shots_per_match": {
                "$cond": [
                    {"$gt": [{"$size": "$matches"}, 0]},
                    {"$divide": ["$total_shots", {"$size": "$matches"}]},
                    0
                ]
            }
        }}
    ]

    results = list(events.aggregate(pipeline))
    print(f"KPIs de tiros calculados para {len(results)} equipos")

    # Eliminar KPIs antiguos de tiros por equipo
    col_kpis.delete_many({"level": "team", "kpi_type": "shots"})

    if results:
        docs = []
        for r in results:
            docs.append({
                "level": "team",
                "kpi_type": "shots",
                "team_id": r["team_id"],
                "team": r["team"],
                "metrics": {
                    "total_shots": r["total_shots"],
                    "matches": r["matches_count"],
                    "shots_per_match": round(r["shots_per_match"], 2),
                },
                "updated_at": datetime.utcnow()
            })
        col_kpis.insert_many(docs)
        print(f"Insertados {len(docs)} KPIs de tiros en '{KPIS_COLLECTION}'")

    return len(results)


def compute_team_passes_kpis(**context):
    """Calcula KPIs de pases por equipo."""
    client = MongoClient(MONGO_URI, authSource="admin")
    db = client[DB_NAME]

    events = db["events"]
    col_kpis = db[KPIS_COLLECTION]

    pipeline = [
        {"$match": {"type": "Pass"}},
        {"$group": {
            "_id": {
                "team_id": "$team_id",
                "team": "$team"
            },
            "total_passes": {"$sum": 1},
            "successful_passes": {
                "$sum": {
                    "$cond": [
                        {"$ne": [{"$ifNull": ["$pass.outcome", None]}, None]},
                        0,
                        1
                    ]
                }
            },
            "matches": {"$addToSet": "$match_id"}
        }},
        {"$project": {
            "_id": 0,
            "team_id": "$_id.team_id",
            "team": "$_id.team",
            "total_passes": 1,
            "successful_passes": 1,
            "matches_count": {"$size": "$matches"},
            "pass_accuracy": {
                "$cond": [
                    {"$gt": ["$total_passes", 0]},
                    {"$multiply": [
                        {"$divide": ["$successful_passes", "$total_passes"]},
                        100
                    ]},
                    0
                ]
            },
            "passes_per_match": {
                "$cond": [
                    {"$gt": [{"$size": "$matches"}, 0]},
                    {"$divide": ["$total_passes", {"$size": "$matches"}]},
                    0
                ]
            }
        }}
    ]

    results = list(events.aggregate(pipeline))
    print(f"KPIs de pases calculados para {len(results)} equipos")

    # Eliminar KPIs antiguos de pases por equipo
    col_kpis.delete_many({"level": "team", "kpi_type": "passes"})

    if results:
        docs = []
        for r in results:
            docs.append({
                "level": "team",
                "kpi_type": "passes",
                "team_id": r["team_id"],
                "team": r["team"],
                "metrics": {
                    "total_passes": r["total_passes"],
                    "successful_passes": r["successful_passes"],
                    "pass_accuracy": round(r["pass_accuracy"], 2),
                    "matches": r["matches_count"],
                    "passes_per_match": round(r["passes_per_match"], 2),
                },
                "updated_at": datetime.utcnow()
            })
        col_kpis.insert_many(docs)
        print(f"Insertados {len(docs)} KPIs de pases en '{KPIS_COLLECTION}'")

    return len(results)


def compute_player_performance_kpis(**context):
    """Calcula KPIs de rendimiento por jugador."""
    client = MongoClient(MONGO_URI, authSource="admin")
    db = client[DB_NAME]

    events = db["events"]
    col_kpis = db[KPIS_COLLECTION]

    pipeline = [
        {"$match": {"player_id": {"$ne": None}}},
        {"$group": {
            "_id": {
                "player_id": "$player_id",
                "player": "$player",
                "team_id": "$team_id",
                "team": "$team"
            },
            "total_events": {"$sum": 1},
            "shots": {
                "$sum": {"$cond": [{"$eq": ["$type", "Shot"]}, 1, 0]}
            },
            "passes": {
                "$sum": {"$cond": [{"$eq": ["$type", "Pass"]}, 1, 0]}
            },
            "dribbles": {
                "$sum": {"$cond": [{"$eq": ["$type", "Dribble"]}, 1, 0]}
            },
            "matches": {"$addToSet": "$match_id"}
        }},
        {"$project": {
            "_id": 0,
            "player_id": "$_id.player_id",
            "player": "$_id.player",
            "team_id": "$_id.team_id",
            "team": "$_id.team",
            "total_events": 1,
            "shots": 1,
            "passes": 1,
            "dribbles": 1,
            "matches_count": {"$size": "$matches"},
            "events_per_match": {
                "$cond": [
                    {"$gt": [{"$size": "$matches"}, 0]},
                    {"$divide": ["$total_events", {"$size": "$matches"}]},
                    0
                ]
            }
        }},
        {"$sort": {"total_events": -1}},
        {"$limit": 100}  # Top 100 jugadores más activos
    ]

    results = list(events.aggregate(pipeline))
    print(f"KPIs de rendimiento calculados para {len(results)} jugadores")

    # Eliminar KPIs antiguos de jugadores
    col_kpis.delete_many({"level": "player", "kpi_type": "performance"})

    if results:
        docs = []
        for r in results:
            docs.append({
                "level": "player",
                "kpi_type": "performance",
                "player_id": r["player_id"],
                "player": r["player"],
                "team_id": r["team_id"],
                "team": r["team"],
                "metrics": {
                    "total_events": r["total_events"],
                    "shots": r["shots"],
                    "passes": r["passes"],
                    "dribbles": r["dribbles"],
                    "matches": r["matches_count"],
                    "events_per_match": round(r["events_per_match"], 2),
                },
                "updated_at": datetime.utcnow()
            })
        col_kpis.insert_many(docs)
        print(f"Insertados {len(docs)} KPIs de jugadores en '{KPIS_COLLECTION}'")

    return len(results)


def compute_match_statistics_kpis(**context):
    """Calcula estadísticas agregadas por partido."""
    client = MongoClient(MONGO_URI, authSource="admin")
    db = client[DB_NAME]

    events = db["events"]
    col_kpis = db[KPIS_COLLECTION]

    pipeline = [
        {"$match": {"match_id": {"$ne": None}}},
        {"$group": {
            "_id": "$match_id",
            "total_events": {"$sum": 1},
            "total_shots": {
                "$sum": {"$cond": [{"$eq": ["$type", "Shot"]}, 1, 0]}
            },
            "total_passes": {
                "$sum": {"$cond": [{"$eq": ["$type", "Pass"]}, 1, 0]}
            },
            "total_fouls": {
                "$sum": {"$cond": [{"$eq": ["$type", "Foul Committed"]}, 1, 0]}
            },
            "teams": {"$addToSet": "$team"}
        }},
        {"$project": {
            "_id": 0,
            "match_id": "$_id",
            "total_events": 1,
            "total_shots": 1,
            "total_passes": 1,
            "total_fouls": 1,
            "teams": 1
        }},
        {"$sort": {"match_id": 1}}
    ]

    results = list(events.aggregate(pipeline))
    print(f"KPIs de estadísticas calculados para {len(results)} partidos")

    # Eliminar KPIs antiguos de partidos
    col_kpis.delete_many({"level": "match", "kpi_type": "statistics"})

    if results:
        docs = []
        for r in results:
            docs.append({
                "level": "match",
                "kpi_type": "statistics",
                "match_id": r["match_id"],
                "teams": r["teams"],
                "metrics": {
                    "total_events": r["total_events"],
                    "total_shots": r["total_shots"],
                    "total_passes": r["total_passes"],
                    "total_fouls": r["total_fouls"],
                },
                "updated_at": datetime.utcnow()
            })
        col_kpis.insert_many(docs)
        print(f"Insertados {len(docs)} KPIs de partidos en '{KPIS_COLLECTION}'")

    return len(results)


# Argumentos por defecto del DAG
default_args = {
    "owner": "copa2024",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    dag_id="copa2024_compute_kpis",
    default_args=default_args,
    description="Cálculo de KPIs de Copa América 2024 desde MongoDB",
    schedule_interval="@daily",  # Ejecutar diariamente
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["copa2024", "kpis", "analytics", "mongodb"],
) as dag:

    # Tarea 1: KPIs de tiros por equipo
    kpis_team_shots = PythonOperator(
        task_id="compute_team_shots_kpis",
        python_callable=compute_team_shots_kpis,
        provide_context=True,
    )

    # Tarea 2: KPIs de pases por equipo
    kpis_team_passes = PythonOperator(
        task_id="compute_team_passes_kpis",
        python_callable=compute_team_passes_kpis,
        provide_context=True,
    )

    # Tarea 3: KPIs de rendimiento por jugador
    kpis_player_performance = PythonOperator(
        task_id="compute_player_performance_kpis",
        python_callable=compute_player_performance_kpis,
        provide_context=True,
    )

    # Tarea 4: KPIs de estadísticas por partido
    kpis_match_statistics = PythonOperator(
        task_id="compute_match_statistics_kpis",
        python_callable=compute_match_statistics_kpis,
        provide_context=True,
    )

    # Las tareas pueden ejecutarse en paralelo ya que son independientes
    [kpis_team_shots, kpis_team_passes, kpis_player_performance, kpis_match_statistics]