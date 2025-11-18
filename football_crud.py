from pymongo import MongoClient
from datetime import datetime
import json
from typing import Dict, List, Optional
from bson import ObjectId


class FootballDataCRUD:
    """
    Clase para manejar operaciones CRUD sobre las 4 colecciones principales
    """
    
    def __init__(self, connection_string: str = "mongodb://localhost:27017"):
        """
        Inicializa la conexión a MongoDB
        
        Args:
            connection_string: String de conexión a MongoDB
        """
        self.client = MongoClient(connection_string)
        self.db = self.client['football_analytics']
        
        # Colecciones principales
        self.events = self.db['events']
        self.teams = self.db['teams']
        self.players = self.db['players']
        self.matches = self.db['matches']
        
        print("✓ Conexión establecida con MongoDB")
        print(f"  - Base de datos: football_analytics")
        print(f"  - Colecciones: events, teams, players, matches")
    
    # ==================== EVENTS COLLECTION - CREATE ====================
    
    def create_event(self, event_data: Dict) -> str:
        """
        CREATE: Inserta un nuevo evento
        
        Args:
            event_data: Datos del evento
            
        Example:
            event = {
                "id": "evt-001",
                "match_id": 3764230,
                "type": "Pass",
                "team": "Tottenham Hotspur Women",
                "minute": 15,
                "player": {"id": 18158, "name": "Rebecca Spencer"}
            }
        """
        try:
            event_data['created_at'] = datetime.now()
            event_data['updated_at'] = datetime.now()
            
            result = self.events.insert_one(event_data)
            print(f"✓ Evento creado: ID={result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            print(f"✗ Error creando evento: {e}")
            return None
    
    def create_events_batch(self, events_list: List[Dict]) -> List[str]:
        """
        CREATE: Inserta múltiples eventos en batch
        """
        try:
            for event in events_list:
                event['created_at'] = datetime.now()
                event['updated_at'] = datetime.now()
            
            result = self.events.insert_many(events_list)
            print(f"✓ {len(result.inserted_ids)} eventos creados")
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            print(f"✗ Error creando eventos batch: {e}")
            return []
    
    # ==================== EVENTS COLLECTION - READ ====================
    
    def read_event_by_id(self, event_id: str) -> Optional[Dict]:
        """
        READ: Obtiene un evento por su ID
        """
        try:
            event = self.events.find_one({"id": event_id})
            if event:
                print(f"✓ Evento encontrado: {event.get('type', 'N/A')}")
                return event
            print(f"⚠ Evento no encontrado: {event_id}")
            return None
        except Exception as e:
            print(f"✗ Error leyendo evento: {e}")
            return None
    
    def read_events_by_match(self, match_id: int, limit: int = None) -> List[Dict]:
        """
        READ: Obtiene todos los eventos de un partido
        """
        try:
            query = self.events.find({"match_id": match_id}).sort("minute", 1)
            if limit:
                query = query.limit(limit)
            
            events = list(query)
            print(f"✓ {len(events)} eventos encontrados para partido {match_id}")
            return events
        except Exception as e:
            print(f"✗ Error leyendo eventos del partido: {e}")
            return []
    
    def read_events_by_team(self, team_name: str, limit: int = 50) -> List[Dict]:
        """
        READ: Obtiene eventos de un equipo
        """
        try:
            events = list(self.events.find({"team": team_name}).limit(limit))
            print(f"✓ {len(events)} eventos de {team_name}")
            return events
        except Exception as e:
            print(f"✗ Error leyendo eventos del equipo: {e}")
            return []
    
    def read_events_by_type(self, event_type: str, limit: int = 100) -> List[Dict]:
        """
        READ: Obtiene eventos por tipo (Pass, Shot, Goal, etc.)
        """
        try:
            events = list(self.events.find({"type": event_type}).limit(limit))
            print(f"✓ {len(events)} eventos de tipo '{event_type}'")
            return events
        except Exception as e:
            print(f"✗ Error leyendo eventos por tipo: {e}")
            return []
    
    # ==================== EVENTS COLLECTION - UPDATE ====================
    
    def update_event(self, event_id: str, update_data: Dict) -> bool:
        """
        UPDATE: Actualiza un evento específico
        """
        try:
            update_data['updated_at'] = datetime.now()
            
            result = self.events.update_one(
                {"id": event_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                print(f"✓ Evento {event_id} actualizado")
                return True
            print(f"⚠ Evento no encontrado o sin cambios")
            return False
        except Exception as e:
            print(f"✗ Error actualizando evento: {e}")
            return False
    
    def update_events_by_match(self, match_id: int, update_data: Dict) -> int:
        """
        UPDATE: Actualiza todos los eventos de un partido
        """
        try:
            update_data['updated_at'] = datetime.now()
            
            result = self.events.update_many(
                {"match_id": match_id},
                {"$set": update_data}
            )
            
            print(f"✓ {result.modified_count} eventos actualizados")
            return result.modified_count
        except Exception as e:
            print(f"✗ Error actualizando eventos: {e}")
            return 0
    
    # ==================== EVENTS COLLECTION - DELETE ====================
    
    def delete_event(self, event_id: str) -> bool:
        """
        DELETE: Elimina un evento
        """
        try:
            result = self.events.delete_one({"id": event_id})
            
            if result.deleted_count > 0:
                print(f"✓ Evento {event_id} eliminado")
                return True
            print(f"⚠ Evento no encontrado")
            return False
        except Exception as e:
            print(f"✗ Error eliminando evento: {e}")
            return False
    
    def delete_events_by_match(self, match_id: int) -> int:
        """
        DELETE: Elimina todos los eventos de un partido
        """
        try:
            result = self.events.delete_many({"match_id": match_id})
            print(f"✓ {result.deleted_count} eventos eliminados")
            return result.deleted_count
        except Exception as e:
            print(f"✗ Error eliminando eventos: {e}")
            return 0
    
    # ==================== TEAMS COLLECTION - CREATE ====================
    
    def create_team(self, team_data: Dict) -> str:
        """
        CREATE: Crea un nuevo equipo
        
        Example:
            team = {
                "team_id": 749,
                "name": "Tottenham Hotspur Women",
                "founded": 1985,
                "country": "England",
                "stadium": "The Hive Stadium"
            }
        """
        try:
            team_data['created_at'] = datetime.now()
            team_data['updated_at'] = datetime.now()
            
            result = self.teams.insert_one(team_data)
            print(f"✓ Equipo creado: {team_data.get('name', 'N/A')}")
            return str(result.inserted_id)
        except Exception as e:
            print(f"✗ Error creando equipo: {e}")
            return None
    
    # ==================== TEAMS COLLECTION - READ ====================
    
    def read_team_by_id(self, team_id: int) -> Optional[Dict]:
        """
        READ: Obtiene un equipo por ID
        """
        try:
            team = self.teams.find_one({"team_id": team_id})
            if team:
                print(f"✓ Equipo encontrado: {team.get('name', 'N/A')}")
                return team
            print(f"⚠ Equipo no encontrado: {team_id}")
            return None
        except Exception as e:
            print(f"✗ Error leyendo equipo: {e}")
            return None
    
    def read_team_by_name(self, team_name: str) -> Optional[Dict]:
        """
        READ: Obtiene un equipo por nombre
        """
        try:
            team = self.teams.find_one({"name": team_name})
            if team:
                print(f"✓ Equipo encontrado: {team_name}")
                return team
            print(f"⚠ Equipo no encontrado: {team_name}")
            return None
        except Exception as e:
            print(f"✗ Error leyendo equipo: {e}")
            return None
    
    def read_all_teams(self) -> List[Dict]:
        """
        READ: Obtiene todos los equipos
        """
        try:
            teams = list(self.teams.find().sort("name", 1))
            print(f"✓ {len(teams)} equipos encontrados")
            return teams
        except Exception as e:
            print(f"✗ Error leyendo equipos: {e}")
            return []
    
    def read_team_with_stats(self, team_id: int) -> Optional[Dict]:
        """
        READ: Obtiene equipo con estadísticas agregadas
        """
        try:
            pipeline = [
                {"$match": {"team_id": team_id}},
                {"$lookup": {
                    "from": "events",
                    "localField": "name",
                    "foreignField": "team",
                    "as": "events"
                }},
                {"$lookup": {
                    "from": "matches",
                    "localField": "team_id",
                    "foreignField": "teams.team_id",
                    "as": "matches"
                }},
                {"$project": {
                    "team_id": 1,
                    "name": 1,
                    "country": 1,
                    "total_events": {"$size": "$events"},
                    "total_matches": {"$size": "$matches"}
                }}
            ]
            
            result = list(self.teams.aggregate(pipeline))
            if result:
                print(f"✓ Estadísticas del equipo obtenidas")
                return result[0]
            return None
        except Exception as e:
            print(f"✗ Error obteniendo estadísticas: {e}")
            return None
    
    # ==================== TEAMS COLLECTION - UPDATE ====================
    
    def update_team(self, team_id: int, update_data: Dict) -> bool:
        """
        UPDATE: Actualiza información de un equipo
        """
        try:
            update_data['updated_at'] = datetime.now()
            
            result = self.teams.update_one(
                {"team_id": team_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                print(f"✓ Equipo {team_id} actualizado")
                return True
            print(f"⚠ Equipo no encontrado o sin cambios")
            return False
        except Exception as e:
            print(f"✗ Error actualizando equipo: {e}")
            return False
    
    def update_team_stats(self, team_id: int) -> bool:
        """
        UPDATE: Recalcula y actualiza estadísticas del equipo
        """
        try:
            # Contar eventos
            total_events = self.events.count_documents({"team_id": team_id})
            
            # Contar partidos
            total_matches = self.matches.count_documents({
                "$or": [
                    {"home_team.team_id": team_id},
                    {"away_team.team_id": team_id}
                ]
            })
            
            # Actualizar
            result = self.teams.update_one(
                {"team_id": team_id},
                {
                    "$set": {
                        "total_events": total_events,
                        "total_matches": total_matches,
                        "stats_updated_at": datetime.now()
                    }
                }
            )
            
            if result.modified_count > 0:
                print(f"✓ Estadísticas actualizadas: {total_events} eventos, {total_matches} partidos")
                return True
            return False
        except Exception as e:
            print(f"✗ Error actualizando estadísticas: {e}")
            return False
    
    # ==================== TEAMS COLLECTION - DELETE ====================
    
    def delete_team(self, team_id: int) -> bool:
        """
        DELETE: Elimina un equipo
        """
        try:
            result = self.teams.delete_one({"team_id": team_id})
            
            if result.deleted_count > 0:
                print(f"✓ Equipo {team_id} eliminado")
                return True
            print(f"⚠ Equipo no encontrado")
            return False
        except Exception as e:
            print(f"✗ Error eliminando equipo: {e}")
            return False
    
    # ==================== PLAYERS COLLECTION - CREATE ====================
    
    def create_player(self, player_data: Dict) -> str:
        """
        CREATE: Crea un nuevo jugador
        
        Example:
            player = {
                "player_id": 18158,
                "name": "Rebecca Leigh Spencer",
                "team": "Tottenham Hotspur Women",
                "position": "Goalkeeper",
                "jersey_number": 22,
                "nationality": "England"
            }
        """
        try:
            player_data['created_at'] = datetime.now()
            player_data['updated_at'] = datetime.now()
            
            result = self.players.insert_one(player_data)
            print(f"✓ Jugador creado: {player_data.get('name', 'N/A')}")
            return str(result.inserted_id)
        except Exception as e:
            print(f"✗ Error creando jugador: {e}")
            return None
    
    def create_players_batch(self, players_list: List[Dict]) -> List[str]:
        """
        CREATE: Crea múltiples jugadores
        """
        try:
            for player in players_list:
                player['created_at'] = datetime.now()
                player['updated_at'] = datetime.now()
            
            result = self.players.insert_many(players_list)
            print(f"✓ {len(result.inserted_ids)} jugadores creados")
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            print(f"✗ Error creando jugadores: {e}")
            return []
    
    # ==================== PLAYERS COLLECTION - READ ====================
    
    def read_player_by_id(self, player_id: int) -> Optional[Dict]:
        """
        READ: Obtiene un jugador por ID
        """
        try:
            player = self.players.find_one({"player_id": player_id})
            if player:
                print(f"✓ Jugador encontrado: {player.get('name', 'N/A')}")
                return player
            print(f"⚠ Jugador no encontrado: {player_id}")
            return None
        except Exception as e:
            print(f"✗ Error leyendo jugador: {e}")
            return None
    
    def read_players_by_team(self, team_name: str) -> List[Dict]:
        """
        READ: Obtiene todos los jugadores de un equipo
        """
        try:
            players = list(self.players.find({"team": team_name}).sort("name", 1))
            print(f"✓ {len(players)} jugadores de {team_name}")
            return players
        except Exception as e:
            print(f"✗ Error leyendo jugadores: {e}")
            return []
    
    def read_players_by_position(self, position: str) -> List[Dict]:
        """
        READ: Obtiene jugadores por posición
        """
        try:
            players = list(self.players.find({"position": position}))
            print(f"✓ {len(players)} jugadores en posición {position}")
            return players
        except Exception as e:
            print(f"✗ Error leyendo jugadores por posición: {e}")
            return []
    
    def read_player_with_stats(self, player_id: int) -> Optional[Dict]:
        """
        READ: Obtiene jugador con estadísticas de eventos
        """
        try:
            pipeline = [
                {"$match": {"player_id": player_id}},
                {"$lookup": {
                    "from": "events",
                    "let": {"pid": "$player_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {"$eq": ["$player.id", "$$pid"]}
                        }}
                    ],
                    "as": "player_events"
                }},
                {"$project": {
                    "player_id": 1,
                    "name": 1,
                    "team": 1,
                    "position": 1,
                    "jersey_number": 1,
                    "total_events": {"$size": "$player_events"},
                    "event_types": {
                        "$setUnion": {
                            "$map": {
                                "input": "$player_events",
                                "as": "evt",
                                "in": "$$evt.type"
                            }
                        }
                    }
                }}
            ]
            
            result = list(self.players.aggregate(pipeline))
            if result:
                print(f"✓ Estadísticas del jugador obtenidas")
                return result[0]
            return None
        except Exception as e:
            print(f"✗ Error obteniendo estadísticas: {e}")
            return None
    
    # ==================== PLAYERS COLLECTION - UPDATE ====================
    
    def update_player(self, player_id: int, update_data: Dict) -> bool:
        """
        UPDATE: Actualiza información de un jugador
        """
        try:
            update_data['updated_at'] = datetime.now()
            
            result = self.players.update_one(
                {"player_id": player_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                print(f"✓ Jugador {player_id} actualizado")
                return True
            print(f"⚠ Jugador no encontrado o sin cambios")
            return False
        except Exception as e:
            print(f"✗ Error actualizando jugador: {e}")
            return False
    
    def update_player_stats(self, player_id: int, stats: Dict) -> bool:
        """
        UPDATE: Actualiza estadísticas de un jugador
        
        Example:
            stats = {
                "goals": 5,
                "assists": 3,
                "matches_played": 10,
                "minutes_played": 850
            }
        """
        try:
            stats['stats_updated_at'] = datetime.now()
            
            result = self.players.update_one(
                {"player_id": player_id},
                {"$set": stats}
            )
            
            if result.modified_count > 0:
                print(f"✓ Estadísticas actualizadas para jugador {player_id}")
                return True
            return False
        except Exception as e:
            print(f"✗ Error actualizando estadísticas: {e}")
            return False
    
    def increment_player_stat(self, player_id: int, stat_field: str, value: int = 1) -> bool:
        """
        UPDATE: Incrementa una estadística del jugador
        """
        try:
            result = self.players.update_one(
                {"player_id": player_id},
                {
                    "$inc": {stat_field: value},
                    "$set": {"updated_at": datetime.now()}
                }
            )
            
            if result.modified_count > 0:
                print(f"✓ {stat_field} incrementado en {value}")
                return True
            return False
        except Exception as e:
            print(f"✗ Error incrementando estadística: {e}")
            return False
    
    # ==================== PLAYERS COLLECTION - DELETE ====================
    
    def delete_player(self, player_id: int) -> bool:
        """
        DELETE: Elimina un jugador
        """
        try:
            result = self.players.delete_one({"player_id": player_id})
            
            if result.deleted_count > 0:
                print(f"✓ Jugador {player_id} eliminado")
                return True
            print(f"⚠ Jugador no encontrado")
            return False
        except Exception as e:
            print(f"✗ Error eliminando jugador: {e}")
            return False
    
    def delete_players_by_team(self, team_name: str) -> int:
        """
        DELETE: Elimina todos los jugadores de un equipo
        """
        try:
            result = self.players.delete_many({"team": team_name})
            print(f"✓ {result.deleted_count} jugadores eliminados")
            return result.deleted_count
        except Exception as e:
            print(f"✗ Error eliminando jugadores: {e}")
            return 0
    
    # ==================== MATCHES COLLECTION - CREATE ====================
    
    def create_match(self, match_data: Dict) -> str:
        """
        CREATE: Crea un nuevo partido
        
        Example:
            match = {
                "match_id": 3764230,
                "date": "2023-01-15",
                "home_team": {
                    "team_id": 749,
                    "name": "Tottenham Hotspur Women"
                },
                "away_team": {
                    "team_id": 972,
                    "name": "West Ham United LFC"
                },
                "competition": "FA Women's Super League",
                "season": "2022/2023"
            }
        """
        try:
            match_data['created_at'] = datetime.now()
            match_data['updated_at'] = datetime.now()
            
            result = self.matches.insert_one(match_data)
            print(f"✓ Partido creado: ID={match_data.get('match_id', 'N/A')}")
            return str(result.inserted_id)
        except Exception as e:
            print(f"✗ Error creando partido: {e}")
            return None
    
    # ==================== MATCHES COLLECTION - READ ====================
    
    def read_match_by_id(self, match_id: int) -> Optional[Dict]:
        """
        READ: Obtiene un partido por ID
        """
        try:
            match = self.matches.find_one({"match_id": match_id})
            if match:
                home = match.get('home_team', {}).get('name', 'N/A')
                away = match.get('away_team', {}).get('name', 'N/A')
                print(f"✓ Partido encontrado: {home} vs {away}")
                return match
            print(f"⚠ Partido no encontrado: {match_id}")
            return None
        except Exception as e:
            print(f"✗ Error leyendo partido: {e}")
            return None
    
    def read_matches_by_team(self, team_name: str) -> List[Dict]:
        """
        READ: Obtiene todos los partidos de un equipo
        """
        try:
            matches = list(self.matches.find({
                "$or": [
                    {"home_team.name": team_name},
                    {"away_team.name": team_name}
                ]
            }).sort("date", -1))
            
            print(f"✓ {len(matches)} partidos de {team_name}")
            return matches
        except Exception as e:
            print(f"✗ Error leyendo partidos: {e}")
            return []
    
    def read_matches_by_season(self, season: str) -> List[Dict]:
        """
        READ: Obtiene partidos por temporada
        """
        try:
            matches = list(self.matches.find({"season": season}))
            print(f"✓ {len(matches)} partidos en temporada {season}")
            return matches
        except Exception as e:
            print(f"✗ Error leyendo partidos: {e}")
            return []
    
    def read_match_with_events(self, match_id: int) -> Optional[Dict]:
        """
        READ: Obtiene partido con todos sus eventos
        """
        try:
            pipeline = [
                {"$match": {"match_id": match_id}},
                {"$lookup": {
                    "from": "events",
                    "localField": "match_id",
                    "foreignField": "match_id",
                    "as": "events"
                }},
                {"$project": {
                    "match_id": 1,
                    "home_team": 1,
                    "away_team": 1,
                    "date": 1,
                    "competition": 1,
                    "total_events": {"$size": "$events"},
                    "event_types": {
                        "$setUnion": {
                            "$map": {
                                "input": "$events",
                                "as": "evt",
                                "in": "$$evt.type"
                            }
                        }
                    }
                }}
            ]
            
            result = list(self.matches.aggregate(pipeline))
            if result:
                print(f"✓ Partido con {result[0]['total_events']} eventos")
                return result[0]
            return None
        except Exception as e:
            print(f"✗ Error obteniendo partido con eventos: {e}")
            return None
    
    # ==================== MATCHES COLLECTION - UPDATE ====================
    
    def update_match(self, match_id: int, update_data: Dict) -> bool:
        """
        UPDATE: Actualiza información de un partido
        """
        try:
            update_data['updated_at'] = datetime.now()
            
            result = self.matches.update_one(
                {"match_id": match_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                print(f"✓ Partido {match_id} actualizado")
                return True
            print(f"⚠ Partido no encontrado o sin cambios")
            return False
        except Exception as e:
            print(f"✗ Error actualizando partido: {e}")
            return False
    
    def update_match_result(self, match_id: int, home_score: int, away_score: int) -> bool:
        """
        UPDATE: Actualiza el resultado de un partido
        """
        try:
            result = self.matches.update_one(
                {"match_id": match_id},
                {
                    "$set": {
                        "home_score": home_score,
                        "away_score": away_score,
                        "result_updated_at": datetime.now()
                    }
                }
            )
            
            if result.modified_count > 0:
                print(f"✓ Resultado actualizado: {home_score}-{away_score}")
                return True
            return False
        except Exception as e:
            print(f"✗ Error actualizando resultado: {e}")
            return False
    
    # ==================== MATCHES COLLECTION - DELETE ====================
    
    def delete_match(self, match_id: int) -> bool:
        """
        DELETE: Elimina un partido
        """
        try:
            result = self.matches.delete_one({"match_id": match_id})
            
            if result.deleted_count > 0:
                print(f"✓ Partido {match_id} eliminado")
                return True
            print(f"⚠ Partido no encontrado")
            return False
        except Exception as e:
            print(f"✗ Error eliminando partido: {e}")
            return False
    
    # ==================== UTILIDADES GENERALES ====================
    
    def get_database_stats(self) -> Dict:
        """
        Obtiene estadísticas generales de la base de datos
        """
        try:
            stats = {
                'events': self.events.count_documents({}),
                'teams': self.teams.count_documents({}),
                'players': self.players.count_documents({}),
                'matches': self.matches.count_documents({})
            }
            
            print("\n=== ESTADÍSTICAS DE LA BASE DE DATOS ===")
            print(f"Events:   {stats['events']:,}")
            print(f"Teams:    {stats['teams']:,}")
            print(f"Players:  {stats['players']:,}")
            print(f"Matches:  {stats['matches']:,}")
            print("=" * 42)
            
            return stats
        except Exception as e:
            print(f"✗ Error obteniendo estadísticas: {e}")
            return {}
    
    def close_connection(self):
        """
        Cierra la conexión a MongoDB
        """
        self.client.close()
        print("✓ Conexión cerrada")


# ==================== EJEMPLO DE USO ====================

if __name__ == "__main__":
    # Inicializar CRUD
    crud = FootballDataCRUD()
    
    print("=" * 60)
    print("DEMOSTRACIÓN DE OPERACIONES CRUD")
    print("=" * 60)
    
    # CREATE - Crear un nuevo evento
    print("\n--- CREATE ---")
    new_event = {
        "id": "test-event-001",
        "match_id": 3764230,
        "type": "Pass",
        "team": "Tottenham Hotspur Women",
        "minute": 25,
        "period": 1,
        "player": {
            "id": 18158,
            "name": "Rebecca Leigh Spencer"
        }
    }
    event_id = crud.create_event(new_event)
    
    # READ - Leer el evento creado
    print("\n--- READ ---")
    event = crud.read_event_by_id("test-event-001")
    
    # # UPDATE - Actualizar el evento
    # print("\n--- UPDATE ---")
    # crud.update_event("test-event-001", {"minute": 26, "corrected": True})
    
    # # DELETE - Eliminar el evento
    # print("\n--- DELETE ---")
    # crud.delete_event("test-event-001")


    # Cerrar conexión
    crud.close_connection()