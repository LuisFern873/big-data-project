import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from load_data import load_events_for_match, load_matches_list, load_lineups
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()



MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
DB_NAME = os.getenv('DB_NAME')
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
REFRESH_EVERY_SECONDS = 60 * 5

@st.cache_data(ttl=REFRESH_EVERY_SECONDS)
def load_match_kpis_from_db(match_id):
    print("load_match_kpis_from_db")
    """Carga los KPIs del dashboard para un partido específico desde MongoDB."""
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col_kpis = db["kpis"]
        
        # Buscar KPIs específicos del dashboard para este partido
        kpis_data = list(col_kpis.find({
            "level": "match",
            "kpi_type": "dashboard",
            "match_id": match_id
        }))
        
        # Convertir a diccionario con team_name como clave
        kpis_dict = {}
        for kpi in kpis_data:
            team_name = kpi.get("team_name")
            if team_name:
                kpis_dict[team_name] = kpi.get("metrics", {})
        
        return kpis_dict
    except Exception as e:
        st.error(f"Error al cargar KPIs: {e}")
        return {}
    finally:
        if client:
            client.close()


# --- FUNCIONES DE DIBUJO ---

def draw_pitch(ax):
    """Dibuja el campo de juego (120x80) en un eje de Matplotlib dado."""
    ax.plot([0, 0], [0, 80], color="black")
    ax.plot([0, 120], [80, 80], color="black")
    ax.plot([120, 120], [80, 0], color="black")
    ax.plot([120, 0], [0, 0], color="black")
    ax.plot([60, 60], [0, 80], color="black")
    ax.add_patch(plt.Circle((60, 40), 10, color="black", fill=False))
    ax.plot([18, 18], [18, 62], color="black")
    ax.plot([0, 18], [18, 18], color="black")
    ax.plot([0, 18], [62, 62], color="black")
    ax.plot([6, 6], [30, 50], color="black")
    ax.plot([0, 6], [30, 30], color="black")
    ax.plot([0, 6], [50, 50], color="black")
    ax.plot([102, 102], [18, 62], color="black")
    ax.plot([120, 102], [18, 18], color="black")
    ax.plot([120, 102], [62, 62], color="black")
    ax.plot([114, 114], [30, 50], color="black")
    ax.plot([120, 114], [30, 30], color="black")
    ax.plot([120, 114], [50, 50], color="black")
    ax.scatter(12, 40, color="black", marker='o', s=20)
    ax.scatter(108, 40, color="black", marker='o', s=20)
    ax.set_xlim(0, 120)
    ax.set_ylim(0, 80)
    ax.set_aspect('equal', adjustable='box')
    ax.set_facecolor("mediumseagreen")

def get_plotly_pitch():
    """Retorna las formas (shapes) para dibujar una cancha en Plotly."""
    shapes = [
        # Borde exterior
        dict(type="rect", x0=0, y0=0, x1=120, y1=80, line=dict(color="white", width=2)),
        # Línea central
        dict(type="line", x0=60, y0=0, x1=60, y1=80, line=dict(color="white", width=2)),
        # Círculo central
        dict(type="circle", x0=50, y0=30, x1=70, y1=50, line=dict(color="white", width=2)),
        # Área izquierda (grande)
        dict(type="rect", x0=0, y0=18, x1=18, y1=62, line=dict(color="white", width=2)),
        # Área izquierda (chica)
        dict(type="rect", x0=0, y0=30, x1=6, y1=50, line=dict(color="white", width=2)),
        # Área derecha (grande)
        dict(type="rect", x0=102, y0=18, x1=120, y1=62, line=dict(color="white", width=2)),
        # Área derecha (chica)
        dict(type="rect", x0=114, y0=30, x1=120, y1=50, line=dict(color="white", width=2)),
    ]
    return shapes

# --- FUNCIONES DE LÓGICA Y CÁLCULO ---

def get_match_result(df_events, teams_info):
    """Calcula los goles y determina el ganador."""
    team_goals = {}
    for team in teams_info:
        t_name = team['name']
        count = len(df_events[
            (df_events['team_name'] == t_name) & 
            (df_events['type'] == 'Shot') & 
            (df_events['shot_outcome'] == 'Goal')
        ])
        team_goals[t_name] = count

    winner = None
    t1_name = teams_info[0]['name']
    t2_name = teams_info[1]['name']

    if team_goals[t1_name] > team_goals[t2_name]:
        winner = t1_name
    elif team_goals[t2_name] > team_goals[t1_name]:
        winner = t2_name
        
    return team_goals, winner

# --- FUNCIONES DE UI (SECCIONES) ---

def render_kpis(kpis_dict, teams_info, team_colors, winner):
    """Renderiza la sección de estadísticas clave desde los KPIs precalculados."""
    st.markdown("### 📊 Estadísticas Clave")
    col1, col2 = st.columns(2)

    for i, col in enumerate([col1, col2]):
        team_name = teams_info[i]['name']
        
        metrics = kpis_dict.get(team_name, {
            "goals": 0,
            "shots": 0,
            "fouls": 0,
            "cards": 0
        })
        
        goals_count = metrics.get("goals", 0)
        shots_count = metrics.get("shots", 0)
        fouls_count = metrics.get("fouls", 0)
        cards_count = metrics.get("cards", 0)
        
        with col:
            header_text = f":{team_colors[team_name]}[{team_name}]"
            if team_name == winner:
                header_text += " 🏆"
            
            st.markdown(f"### {header_text}")
            
            kpi_a, kpi_b = st.columns(2)
            kpi_a.metric("Goles", goals_count)
            kpi_b.metric("Tiros", shots_count)
            
            kpi_c, kpi_d = st.columns(2)
            kpi_c.metric("Faltas", fouls_count)
            kpi_d.metric("Tarjetas", cards_count)

def render_player_positions(df_events, teams_info, team_colors, match_id):
    """Calcula y grafica la posición promedio de los jugadores usando Player ID."""
    st.markdown("### 📍 Posición Promedio de Jugadoras")
    
    # 1. Cargar Alineaciones (que tienen ID, Nombre, Dorsal)
    lineups = load_lineups(match_id)
    
    if not lineups:
        st.warning("No se encontró información de alineación para este partido.")
        return
    
    # 2. Calcular promedio de posiciones agrupando por TEAM y PLAYER_ID
    # IMPORTANTE: Usamos player_id porque los eventos no siempre tienen el nombre
    if 'player_id' not in df_events.columns:
        st.error("No se encontraron IDs de jugadores en los eventos.")
        return

    avg_positions = df_events.groupby(['team_name', 'player_id'])[['x', 'y']].mean().reset_index()
    # print("AVG POSITIONS: ", avg_positions)
    
    all_players_data = []

    for team in teams_info:
        t_name = team['name']
        
        # Obtener DF de alineación y DF de promedios para este equipo
        df_lineup = lineups.get(t_name)
        df_avg = avg_positions[avg_positions['team_name'] == t_name]
        
        if df_lineup is not None and not df_avg.empty:
            # 3. MERGE usando 'player_id'
            merged = pd.merge(df_lineup, df_avg, on='player_id', how='inner')
            merged['team'] = t_name
            merged['color'] = team_colors[t_name]
            all_players_data.append(merged)
            
    if all_players_data:
        full_df = pd.concat(all_players_data)
        
        # Crear gráfico Plotly
        fig = go.Figure()

        # Agregar formas de la cancha
        fig.update_layout(
            shapes=get_plotly_pitch(),
            xaxis=dict(range=[0, 120], showgrid=False, visible=False),
            # Invertimos Y (80 -> 0) porque en coordenadas de fútbol (0,0) suele ser esquina superior izq
            # pero depende del proveedor. StatsBomb suele ser (0,80) abajo-izq. 
            # Si se ven invertidos los equipos, cambiar a range=[0, 80].
            yaxis=dict(range=[80, 0], showgrid=False, visible=False), 
            height=600,
            plot_bgcolor='mediumseagreen',
            margin=dict(l=20, r=20, t=20, b=20),
        )
        
        # Agregar jugadores por equipo
        for team_name in full_df['team'].unique():
            team_data = full_df[full_df['team'] == team_name]
            
            fig.add_trace(go.Scatter(
                x=team_data['x'],
                y=team_data['y'],
                mode='markers+text',
                marker=dict(size=28, color=team_data['color'], line=dict(width=2, color='white')),
                text=team_data['jersey_number'],
                textfont=dict(color='white', size=14, family="Arial Black"),
                hovertext=team_data['player_name'] + " (" + team_data['position'] + ")",
                name=team_name
            ))

        st.plotly_chart(fig, use_container_width=True)
        st.caption("Nota: Se muestra la ubicación promedio de todas las acciones realizadas por cada jugador.")
    else:
        st.info("No hay suficientes datos cruzados para generar el mapa de jugadores.")

def render_pitch_map(df_events, teams_info, team_colors, match_label):
    """Renderiza el mapa del campo con los eventos filtrados (Matplotlib)."""
    st.divider()
    st.markdown("### 🗺️ Mapeo de Eventos en el Campo de Juego")

    event_types = df_events['type'].unique().tolist()
    event_types.sort()
    selected_types = st.multiselect("Filtrar Eventos:", event_types, default=event_types)
    
    if not selected_types:
        st.warning("Selecciona al menos un tipo de evento.")
        return

    df_filtered = df_events[df_events['type'].isin(selected_types)]
    
    fig, ax = plt.subplots(figsize=(10, 7))
    draw_pitch(ax)
    
    for team in teams_info:
        t_name = team['name']
        t_data = df_filtered[df_filtered['team_name'] == t_name]
        
        ax.scatter(
            t_data['x'], 
            t_data['y'], 
            alpha=0.7, 
            s=40, 
            label=t_name,
            color=team_colors[t_name]
        )

    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=2)
    ax.set_title(f"Eventos: {match_label}")
    st.pyplot(fig)

def render_event_distribution(df_events, teams_info):
    """Renderiza los gráficos de torta/donut con la categorización de eventos."""
    st.divider()
    st.markdown("### 📈 Categorización de Eventos")

    col_p1, col_p2 = st.columns(2)

    for i, col in enumerate([col_p1, col_p2]):
        t_name = teams_info[i]['name']
        
        df_team_pie = df_events[df_events['team_name'] == t_name].copy()
        
        pie_data = df_team_pie.groupby('event_category').size().reset_index(name='count')
        
        # Filtrar categoría 'Unknown' si existe
        pie_data = pie_data[pie_data['event_category'] != 'Unknown']
        
        # Definir escala de color según el equipo (Blue vs Red)
        color_scale = px.colors.sequential.Blues_r if i == 0 else px.colors.sequential.Reds_r
        
        fig_pie = px.pie(
            pie_data, 
            values='count', 
            names='event_category',
            title=t_name,
            color_discrete_sequence=color_scale,
            hole=0.5
        )
        
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0))
        
        col.plotly_chart(fig_pie, use_container_width=True)

# --- MAIN APP ---

def main():
    st.set_page_config(page_title="Dashboard Táctico", layout="wide")


    if st.button("🔄 Actualizar datos", help="Borrar caché y recargar datos"):
    # El botón se dibuja en la columna derecha
        st.cache_data.clear()
        st.rerun()

    st.title("⚽ Superliga Femenina de Inglaterra - Dashboard Táctico por Partido")
        # st.title("🏆 Resumen de Temporada")


    matches_list = load_matches_list()

    if matches_list:
        # Selector de Partido
        match_options = {
            f"{m['teams'][0]['name']} vs {m['teams'][1]['name']} (ID: {m['match_id']})": m 
            for m in matches_list
        }
        
        selected_match_label = st.selectbox("Selecciona un Partido:", list(match_options.keys()))
        selected_match_data = match_options[selected_match_label]
        selected_match_id = selected_match_data['match_id']

        # Cargar Eventos
        df_events = load_events_for_match(selected_match_id)

        if not df_events.empty:
            teams_info = selected_match_data['teams']
            
            # Configuración de colores
            team_colors = {
                teams_info[0]['name']: 'blue',
                teams_info[1]['name']: 'red'
            }
            
            kpis_dict = load_match_kpis_from_db(selected_match_id)
            
            team1_name = teams_info[0]['name']
            team2_name = teams_info[1]['name']
            team1_goals = kpis_dict.get(team1_name, {}).get("goals", 0)
            team2_goals = kpis_dict.get(team2_name, {}).get("goals", 0)
            
            winner = None
            if team1_goals > team2_goals:
                winner = team1_name
            elif team2_goals > team1_goals:
                winner = team2_name

            # Renderizado de secciones
            render_kpis(kpis_dict, teams_info, team_colors, winner)
            render_event_distribution(df_events, teams_info)
            render_player_positions(df_events, teams_info, team_colors, selected_match_id)
            render_pitch_map(df_events, teams_info, team_colors, selected_match_label)

        else:
            st.info("No hay datos de eventos con ubicación para este partido.")
    else:
        st.warning("No se encontraron partidos en la colección 'matches'.")

if __name__ == "__main__":
    main()