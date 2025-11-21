import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from load_data import load_events_for_match, load_matches_list

def draw_pitch(ax):
    """Dibuja el campo de juego (120x80)."""
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

# --- UI ---

st.title("⚽ Dashboard Táctico por Partido")

matches_list = load_matches_list()

if matches_list:
    match_options = {
        f"{m['teams'][0]['name']} vs {m['teams'][1]['name']} (ID: {m['match_id']})": m 
        for m in matches_list
    }
    
    selected_match_label = st.selectbox("Selecciona un Partido:", list(match_options.keys()))
    selected_match_data = match_options[selected_match_label]
    selected_match_id = selected_match_data['match_id']

    # Cargar eventos
    df_events = load_events_for_match(selected_match_id)

    if not df_events.empty:
        
        teams_info = selected_match_data['teams']
        
        # 1. Asignación de Colores Fija (Team 1 = Blue, Team 2 = Red)
        team_colors = {
            teams_info[0]['name']: 'blue',
            teams_info[1]['name']: 'red'
        }

        # 2. Calcular Goles y Ganador
        team_goals = {}
        for team in teams_info:
            t_name = team['name']
            # Filtro por Type=Shot y Outcome=Goal
            count = len(df_events[
                (df_events['team_name'] == t_name) & 
                (df_events['type'] == 'Shot') & 
                (df_events['shot_outcome'] == 'Goal')
            ])
            team_goals[t_name] = count

        winner = None
        if team_goals[teams_info[0]['name']] > team_goals[teams_info[1]['name']]:
            winner = teams_info[0]['name']
        elif team_goals[teams_info[1]['name']] > team_goals[teams_info[0]['name']]:
            winner = teams_info[1]['name']

        # --- SECCIÓN DE KPIs ---
        st.markdown("### 📊 Estadísticas Clave")
        col1, col2 = st.columns(2)
        
        card_values = ["Yellow Card", "Red Card", "Second Yellow"]

        for i, col in enumerate([col1, col2]):
            team_name = teams_info[i]['name']
            df_team = df_events[df_events['team_name'] == team_name]
            
            # KPI 1: Goles
            goals_count = team_goals[team_name]

            # KPI 2: Faltas
            fouls_count = len(df_team[df_team['type'] == "Foul Committed"])
            
            # KPI 3: Tarjetas
            if 'foul_committed_card' in df_team.columns:
                cards_count = len(df_team[df_team['foul_committed_card'].isin(card_values)])
            else:
                cards_count = 0
            
            # KPI 4: Tiros
            shots_count = len(df_team[df_team['type'] == "Shot"])

            with col:
                # Header con color y marcador de ganador
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
                
                st.divider()

        # --- SECCIÓN MAPA ---
        st.markdown("### 🗺️ Mapeo de Eventos en el Campo de Juego")

        event_types = df_events['type'].unique().tolist()
        event_types.sort()
        selected_types = st.multiselect("Filtrar Eventos:", event_types, default=event_types)
        
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
                color=team_colors[t_name] # Usar color asignado
            )

        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=2)
        ax.set_title(f"Eventos: {selected_match_label}")
        st.pyplot(fig)

    else:
        st.info("No hay datos de eventos con ubicación para este partido.")
else:
    st.warning("No se encontraron partidos en la colección 'matches'.")