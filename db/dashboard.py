import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import json

# Set page config
st.set_page_config(
    page_title="Fantasy Football Data Explorer",
    page_icon="🏈",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-row {
        display: flex;
        flex-wrap: wrap;
        gap: 16px;
        margin-bottom: 24px;
    }
    .metric-container {
        background-color: white;
        padding: 16px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        flex: 1;
        min-width: 150px;
        text-align: center;
    }
    .metric-value {
        font-size: 20px;
        font-weight: bold;
        color: #0063dc;
        margin: 4px 0;
    }
    .metric-label {
        font-size: 12px;
        color: #666;
        text-transform: uppercase;
    }
    .small-metric-container {
        background-color: white;
        padding: 12px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        flex: 1;
        min-width: 120px;
        text-align: center;
    }
    .small-metric-value {
        font-size: 16px;
        font-weight: 500;
        color: #0063dc;
        margin: 2px 0;
    }
    .small-metric-label {
        font-size: 11px;
        color: #666;
        text-transform: uppercase;
    }
    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: white !important;
        border-radius: 8px !important;
        margin-bottom: 16px !important;
    }
    .streamlit-expanderContent {
        border: none !important;
        background-color: transparent !important;
    }
    .team-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 20px;
        background: white;
        border-radius: 12px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 24px;
    }
    .team-name {
        display: flex;
        align-items: center;
        gap: 16px;
    }
    .team-stats {
        text-align: center;
    }
    .matchup {
        display: flex;
        align-items: center;
        gap: 12px;
    }
    .team-logo {
        width: 48px;
        height: 48px;
        border-radius: 50%;
        object-fit: cover;
    }
    .record {
        font-size: 24px;
        font-weight: 600;
        color: #2c3e50;
    }
    .points {
        font-size: 20px;
        color: #666;
    }
    .rank {
        font-size: 14px;
        color: #666;
    }
    .matchup-score {
        font-size: 20px;
        font-weight: 500;
        color: #2c3e50;
    }
    .vs {
        font-size: 14px;
        color: #666;
        margin: 0 8px;
    }
    .team-box {
        background-color: #f8f9fa;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 20px;
    }
    .week-nav {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 20px;
    }
    .week-nav button {
        background: none;
        border: none;
        cursor: pointer;
        padding: 4px 8px;
        color: #0063dc;
        font-size: 14px;
    }
    .week-nav button:hover {
        background-color: #f0f0f0;
        border-radius: 4px;
    }
    .week-nav span {
        font-size: 14px;
        font-weight: 500;
        color: #333;
    }
    .stat-tabs {
        display: flex;
        gap: 24px;
        margin-bottom: 16px;
        border-bottom: 1px solid #e0e0e0;
        padding-bottom: 8px;
    }
    .stat-tab {
        color: #666;
        font-size: 13px;
        cursor: pointer;
        padding: 4px 0;
        position: relative;
    }
    .stat-tab.active {
        color: #0063dc;
        font-weight: 500;
    }
    .stat-tab.active:after {
        content: '';
        position: absolute;
        bottom: -9px;
        left: 0;
        right: 0;
        height: 2px;
        background-color: #0063dc;
    }
    </style>
    """, unsafe_allow_html=True)

# Data loading functions
@st.cache_data
def load_seasons():
    conn = sqlite3.connect('fantasy_data.db')
    query = "SELECT DISTINCT season FROM leagues ORDER BY season DESC"
    seasons = pd.read_sql_query(query, conn)
    conn.close()
    return seasons['season'].tolist()

@st.cache_data
def load_leagues(season):
    conn = sqlite3.connect('fantasy_data.db')
    query = f"SELECT league_key, name FROM leagues WHERE season = '{season}'"
    leagues = pd.read_sql_query(query, conn)
    conn.close()
    return leagues

@st.cache_data
def load_table_data(query, params=None):
    conn = sqlite3.connect('fantasy_data.db')
    if params:
        data = pd.read_sql_query(query, conn, params=params)
    else:
        data = pd.read_sql_query(query, conn)
    conn.close()
    return data

def format_json(json_str):
    if pd.isna(json_str):
        return None
    try:
        return json.dumps(json.loads(json_str), indent=2)
    except:
        return json_str

def format_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d, %Y")
    except:
        return date_str

def display_player_stats(league_key: str, week: int):
    """Display player stats in a table format matching Yahoo Fantasy interface"""
    # Query to get player stats with all necessary information
    query = """
        SELECT 
            p.position as Pos,
            p.name as Player,
            p.team as Team,
            p.status as Status,
            p.headshot_url as HeadshotUrl,
            ps.bye_week as Bye,
            ROUND(ps.fantasy_points, 2) as "Fan Pts",
            ROUND(ps.projected_points, 2) as "Proj Pts",
            ps.start_percentage as "% Start",
            ps.roster_percentage as "% Ros",
            ps.passing_yards as "Pass Yds",
            ps.passing_touchdowns as "Pass TD",
            ps.passing_interceptions as Int,
            ps.passing_attempts as "Pass Att",
            ps.rushing_yards as "Rush Yds",
            ps.rushing_touchdowns as "Rush TD",
            ps.receptions as Rec,
            ps.receiving_yards as "Rec Yds",
            ps.receiving_touchdowns as "Rec TD",
            ps.targets as Tgt,
            ps.misc_touchdowns as "Misc TD",
            ps.two_point_conversions as "2PT",
            ps.fumbles_lost as Lost,
            ps.final_status as "Final",
            ps.opponent as Opp
        FROM players p
        JOIN player_stats ps ON p.player_key = ps.player_key
        WHERE ps.league_key = ? AND ps.week = ?
        ORDER BY ps.fantasy_points DESC
    """
    
    try:
        conn = sqlite3.connect('fantasy_data.db')
        df = pd.read_sql_query(query, conn, params=(league_key, week))
        
        # Format player info column
        def format_player_info(row):
            headshot = f'<img src="{row["HeadshotUrl"]}" style="width:30px;height:30px;border-radius:50%;margin-right:10px;">' if pd.notna(row["HeadshotUrl"]) else ""
            status = f' - {row["Final"]} {row["Opp"]}' if pd.notna(row["Final"]) else ""
            return f"""
                <div style="display:flex;align-items:center">
                    {headshot}
                    <div>
                        <div style="font-weight:bold">{row["Player"]}</div>
                        <div style="color:gray;font-size:12px">{row["Team"]} - {row["Pos"]}{status}</div>
                    </div>
                </div>
            """
        
        df["Player Info"] = df.apply(format_player_info, axis=1)
        
        # Select and reorder columns for display
        display_columns = [
            "Pos", "Player Info", "Bye", "Fan Pts", "Proj Pts", "% Start", "% Ros",
            "Pass Yds", "Pass TD", "Int", "Pass Att",
            "Rush Yds", "Rush TD",
            "Rec", "Rec Yds", "Rec TD", "Tgt",
            "Misc TD", "2PT", "Lost"
        ]
        
        df_display = df[display_columns]
        
        # Apply custom styling
        st.markdown("""
        <style>
        .player-stats-table {
            font-size: 14px;
        }
        .player-stats-table th {
            background-color: #f8f9fa;
            font-weight: 600;
            text-align: center;
        }
        .player-stats-table td {
            text-align: center;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Display the table
        st.write(
            df_display.to_html(
                escape=False,
                index=False,
                classes=["player-stats-table"],
                float_format=lambda x: '{:.2f}'.format(x) if pd.notna(x) else '-'
            ),
            unsafe_allow_html=True
        )
        
    except Exception as e:
        st.error(f"Error displaying player stats: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    # Filters row
    col1, col2 = st.columns([1, 2])
    
    with col1:
        seasons = load_seasons()
        selected_season = st.selectbox("Season", seasons)
    
    with col2:
        leagues = load_leagues(selected_season)
        selected_league = st.selectbox(
            "League",
            leagues['league_key'].tolist(),
            format_func=lambda x: leagues[leagues['league_key'] == x]['name'].iloc[0]
        )
    
    # Navigation tabs
    tabs = st.tabs([
        "League Info",
        "Standings",
        "Team Data",
        "Player Data",
        "Game Data",
        "Draft Results",
        "Rosters"
    ])
    
    # League Info Tab
    with tabs[0]:
        # Load league data
        league_query = f"""
        SELECT league_key, sport_code, name, season, settings, last_updated 
        FROM leagues 
        WHERE league_key = '{selected_league}'
        """
        league_data = load_table_data(league_query)
        
        if not league_data.empty:
            settings = json.loads(league_data['settings'].iloc[0]) if not pd.isna(league_data['settings'].iloc[0]) else {}
            
            # First Row: Main League Details
            st.markdown("""
            <div class="metric-row">
                <div class="metric-container">
                    <div class="metric-label">League Name</div>
                    <div class="metric-value">{}</div>
                </div>
                <div class="metric-container">
                    <div class="metric-label">Season</div>
                    <div class="metric-value">{}</div>
                </div>
                <div class="metric-container">
                    <div class="metric-label">Teams</div>
                    <div class="metric-value">{}</div>
                </div>
                <div class="metric-container">
                    <div class="metric-label">Total Weeks</div>
                    <div class="metric-value">{}</div>
                </div>
            </div>
            """.format(
                league_data['name'].iloc[0],
                league_data['season'].iloc[0],
                settings.get('num_teams', 'N/A'),
                settings.get('end_week', 'N/A')
            ), unsafe_allow_html=True)
            
            # Settings in expander
            with st.expander("League Settings"):
                # Second Row: League Settings as small metrics
                st.markdown("""
                <div class="metric-row">
                    <div class="small-metric-container">
                        <div class="small-metric-label">League Type</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Scoring Type</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Draft Status</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Start Week</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">End Week</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">League Status</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                </div>
                """.format(
                    settings.get('league_type', 'N/A'),
                    settings.get('scoring_type', 'N/A'),
                    settings.get('draft_status', 'N/A'),
                    settings.get('start_week', 'N/A'),
                    settings.get('end_week', 'N/A'),
                    "Active" if settings.get('is_finished') == 0 else "Finished"
                ), unsafe_allow_html=True)
                
                # Third Row: Additional Settings
                st.markdown("""
                <div class="metric-row">
                    <div class="small-metric-container">
                        <div class="small-metric-label">Start Date</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">End Date</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Pro League</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Cash League</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Entry Fee</div>
                        <div class="small-metric-value">{}</div>
                    </div>
                </div>
                """.format(
                    format_date(settings.get('start_date', 'N/A')),
                    format_date(settings.get('end_date', 'N/A')),
                    "Yes" if settings.get('is_pro_league') == "1" else "No",
                    "Yes" if settings.get('is_cash_league') == "1" else "No",
                    settings.get('entry_fee', 'N/A')
                ), unsafe_allow_html=True)
            
            # League Standings
            standings_query = f"""
            SELECT 
                team_name,
                rank,
                wins || '-' || losses || '-' || ties as record,
                ROUND(points_for, 2) as points_for,
                ROUND(points_against, 2) as points_against,
                ROUND(percentage * 100, 1) as win_percentage
            FROM league_standings 
            WHERE league_key = '{selected_league}'
            ORDER BY rank
            """
            standings_data = load_table_data(standings_query)
            st.dataframe(
                standings_data,
                column_config={
                    "team_name": "Team",
                    "rank": st.column_config.NumberColumn("Rank", format="%d"),
                    "record": "Record",
                    "points_for": st.column_config.NumberColumn("Points For", format="%.2f"),
                    "points_against": st.column_config.NumberColumn("Points Against", format="%.2f"),
                    "win_percentage": st.column_config.NumberColumn("Win %", format="%.1f%%")
                },
                hide_index=True,
                use_container_width=True
            )
            
            # League Performance Analysis
            st.subheader("League Performance Analysis")
            
            # Get weekly scoring data
            weekly_scoring_query = f"""
            WITH RankedGames AS (
                SELECT DISTINCT
                    lg.week,
                    t.name as team_name,
                    CASE 
                        WHEN lg.home_team_key = t.team_key THEN lg.home_team_points
                        ELSE lg.away_team_points
                    END as points,
                    CASE 
                        WHEN lg.home_team_key = t.team_key THEN t2.name
                        ELSE t1.name
                    END as opponent,
                    CASE 
                        WHEN lg.home_team_key = t.team_key THEN lg.away_team_points
                        ELSE lg.home_team_points
                    END as opponent_points,
                    ROW_NUMBER() OVER (PARTITION BY lg.week, t.name ORDER BY lg.game_start_time DESC) as rn
                FROM league_games lg
                JOIN teams t ON (t.team_key = lg.home_team_key OR t.team_key = lg.away_team_key)
                JOIN teams t1 ON lg.home_team_key = t1.team_key
                JOIN teams t2 ON lg.away_team_key = t2.team_key
                WHERE lg.league_key = '{selected_league}'
                  AND lg.home_team_points IS NOT NULL  -- Only include completed games
                  AND lg.away_team_points IS NOT NULL
            )
            SELECT 
                week,
                team_name,
                points,
                opponent,
                opponent_points
            FROM RankedGames
            WHERE rn = 1
            ORDER BY week ASC, team_name ASC
            """
            weekly_scoring_data = load_table_data(weekly_scoring_query)
            
            # Weekly Scores with Week filter
            col1, col2 = st.columns([4, 1])
            with col1:
                st.subheader("Weekly Scores")
            with col2:
                all_weeks = sorted(weekly_scoring_data['week'].unique())
                week_options = ["All"] + [f"Week {w}" for w in all_weeks]
                selected_week = st.selectbox("", week_options, key="week_filter", label_visibility="collapsed")
            
            # Filter data based on selected week
            filtered_data = weekly_scoring_data
            if selected_week != "All":
                week_num = int(selected_week.replace("Week ", ""))
                filtered_data = weekly_scoring_data[weekly_scoring_data['week'] == week_num]
                
                # Weekly aggregation metrics
                week_stats = filtered_data.agg({
                    'points': ['mean', 'min', 'max', 'std']
                }).round(1)
                
                # Get highest scoring team for the week
                highest_score_idx = filtered_data['points'].idxmax()
                highest_scorer = filtered_data.loc[highest_score_idx]
                
                # Get closest matchup
                filtered_data['point_diff'] = abs(filtered_data['points'] - filtered_data['opponent_points'])
                closest_game_idx = filtered_data['point_diff'].idxmin()
                closest_game = filtered_data.loc[closest_game_idx]
                
                # Display weekly stats in a nice format
                st.markdown("""
                <div class="metric-row">
                    <div class="small-metric-container">
                        <div class="small-metric-label">Average Score</div>
                        <div class="small-metric-value">{:.1f}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Highest Score</div>
                        <div class="small-metric-value">{:.1f}</div>
                        <div class="small-metric-label">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Lowest Score</div>
                        <div class="small-metric-value">{:.1f}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Score Spread</div>
                        <div class="small-metric-value">{:.1f}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Closest Matchup</div>
                        <div class="small-metric-value">{:.1f} - {:.1f}</div>
                        <div class="small-metric-label">{} vs {}</div>
                    </div>
                </div>
                """.format(
                    week_stats['points']['mean'],
                    highest_scorer['points'],
                    highest_scorer['team_name'],
                    week_stats['points']['min'],
                    week_stats['points']['std'],
                    closest_game['points'],
                    closest_game['opponent_points'],
                    closest_game['team_name'],
                    closest_game['opponent']
                ), unsafe_allow_html=True)
            
            else:
                # Season-wide aggregations when "All" is selected
                season_stats = weekly_scoring_data.agg({
                    'points': ['mean', 'min', 'max', 'std']
                }).round(1)
                
                # Highest scoring week for each team
                team_high_scores = weekly_scoring_data.groupby('team_name')['points'].max().sort_values(ascending=False)
                highest_team_score = team_high_scores.index[0]
                highest_score = team_high_scores.iloc[0]
                
                # Most consistent team (lowest std dev)
                team_consistency = weekly_scoring_data.groupby('team_name')['points'].std().sort_values()
                most_consistent_team = team_consistency.index[0]
                consistency_score = team_consistency.iloc[0]
                
                st.markdown("""
                <div class="metric-row">
                    <div class="small-metric-container">
                        <div class="small-metric-label">Season Average</div>
                        <div class="small-metric-value">{:.1f}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Highest Single Score</div>
                        <div class="small-metric-value">{:.1f}</div>
                        <div class="small-metric-label">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Most Consistent Team</div>
                        <div class="small-metric-value">{:.1f}</div>
                        <div class="small-metric-label">{}</div>
                    </div>
                    <div class="small-metric-container">
                        <div class="small-metric-label">Score Range</div>
                        <div class="small-metric-value">{:.1f} - {:.1f}</div>
                    </div>
                </div>
                """.format(
                    season_stats['points']['mean'],
                    highest_score,
                    highest_team_score,
                    consistency_score,
                    most_consistent_team,
                    season_stats['points']['min'],
                    season_stats['points']['max']
                ), unsafe_allow_html=True)
            
            # Weekly Scores Table
            st.dataframe(
                filtered_data,
                column_config={
                    "week": "Week",
                    "team_name": "Team",
                    "points": st.column_config.NumberColumn("Points", format="%.1f"),
                    "opponent": "Opponent",
                    "opponent_points": st.column_config.NumberColumn("Opp. Points", format="%.1f")
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Team Performance Summary
            st.subheader("Team Performance Summary")
            team_stats = weekly_scoring_data.groupby('team_name').agg({
                'points': ['count', 'mean', 'min', 'max', 'std'],
                'opponent_points': 'mean'
            }).round(1)
            
            team_stats.columns = ['Games Played', 'Avg Points', 'Min Points', 'Max Points', 'Std Dev', 'Avg Against']
            team_stats = team_stats.reset_index()
            
            # Add point differential
            team_stats['Point Diff'] = (team_stats['Avg Points'] - team_stats['Avg Against']).round(1)
            
            # Sort by average points descending
            team_stats = team_stats.sort_values('Avg Points', ascending=False)
            
            st.dataframe(
                team_stats,
                column_config={
                    "team_name": "Team",
                    "Games Played": st.column_config.NumberColumn("Games", format="%d"),
                    "Avg Points": st.column_config.NumberColumn("Avg Points", format="%.1f"),
                    "Min Points": st.column_config.NumberColumn("Min Points", format="%.1f"),
                    "Max Points": st.column_config.NumberColumn("Max Points", format="%.1f"),
                    "Std Dev": st.column_config.NumberColumn("Consistency", format="%.1f", help="Lower value means more consistent scoring"),
                    "Avg Against": st.column_config.NumberColumn("Avg Against", format="%.1f"),
                    "Point Diff": st.column_config.NumberColumn("Point Diff", format="%.1f", help="Average point differential per game")
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Most Competitive Matchups
            st.subheader("Closest Matchups")
            competitive_query = f"""
            SELECT 
                lg.week,
                t1.name as home_team,
                lg.home_team_points,
                t2.name as away_team,
                lg.away_team_points,
                ABS(lg.home_team_points - lg.away_team_points) as point_difference,
                CASE 
                    WHEN lg.home_team_points > lg.away_team_points THEN t1.name
                    ELSE t2.name
                END as winner
            FROM league_games lg
            JOIN teams t1 ON lg.home_team_key = t1.team_key
            JOIN teams t2 ON lg.away_team_key = t2.team_key
            WHERE lg.league_key = '{selected_league}'
            {f"AND lg.week = {selected_week}" if selected_week != 'All' else ''}
            ORDER BY point_difference ASC
            LIMIT 5
            """
            competitive_data = load_table_data(competitive_query)
            st.dataframe(
                competitive_data,
                column_config={
                    "week": "Week",
                    "home_team": "Home Team",
                    "home_team_points": st.column_config.NumberColumn("Home Points", format="%.1f"),
                    "away_team": "Away Team",
                    "away_team_points": st.column_config.NumberColumn("Away Points", format="%.1f"),
                    "point_difference": st.column_config.NumberColumn("Margin", format="%.1f"),
                    "winner": "Winner"
                },
                hide_index=True,
                use_container_width=True
            )
    
    # Standings Tab
    with tabs[1]:
        # Create subtabs
        standings_subtabs = st.tabs(["Standings", "Schedule", "Playoffs & Ratings & Levels"])
        
        # Standings subtab
        with standings_subtabs[0]:
            # Add week selector in the top right
            col1, col2 = st.columns([4, 1])
            with col2:
                weeks_query = f"""
                SELECT DISTINCT week 
                FROM rosters 
                WHERE game_id = {selected_league.split('.')[0]}
                AND league_id = {selected_league.split('.')[2]}
                ORDER BY week DESC
                """
                weeks_data = load_table_data(weeks_query)
                selected_week = st.selectbox(
                    "Week",
                    weeks_data['week'].tolist(),
                    key="standings_week_selector"
                )

            # Get all teams for the selected week
            teams_query = f"""
            SELECT DISTINCT t.name as team_name, t.team_key
            FROM teams t
            WHERE t.team_key LIKE '{selected_league}.t.%'
            ORDER BY t.name
            """
            teams_data = load_table_data(teams_query)

            # Create grid layout for teams (3 teams per row)
            num_teams = len(teams_data)
            num_rows = (num_teams + 2) // 3  # Calculate number of rows needed

            for row in range(num_rows):
                cols = st.columns(3)  # Create 3 columns for each row
                for col in range(3):
                    team_idx = row * 3 + col
                    if team_idx < num_teams:
                        team_name = teams_data['team_name'].iloc[team_idx]
                        team_key = teams_data['team_key'].iloc[team_idx]
                        team_id = int(team_key.split('.')[-1])

                        with cols[col]:
                            # Get roster data for the team
                            roster_query = f"""
                            SELECT 
                                r.selected_position as pos,
                                p.name as player,
                                p.team as nfl_team,
                                p.position as actual_position,
                                p.status,
                                p.headshot_url,
                                r.is_starting,
                                p.status as game_status,
                                NULL as opponent
                            FROM rosters r
                            JOIN players p ON r.player_key = p.player_key
                            LEFT JOIN player_stats ps ON r.player_key = ps.player_key 
                                AND ps.week = {selected_week}
                                AND ps.league_key = '{selected_league}'
                            WHERE r.team_id = {team_id}
                            AND r.week = {selected_week}
                            AND r.game_id = {selected_league.split('.')[0]}
                            AND r.league_id = {selected_league.split('.')[2]}
                            AND r.is_starting = 1 -- Only show starters
                            ORDER BY 
                                CASE r.selected_position
                                    WHEN 'QB' THEN 1
                                    WHEN 'WR' THEN 2
                                    WHEN 'RB' THEN 3
                                    WHEN 'TE' THEN 4
                                    WHEN 'W/R/T' THEN 5
                                    WHEN 'K' THEN 6
                                    WHEN 'DEF' THEN 7
                                    ELSE 8
                                END,
                                p.name
                            """
                            roster_data = load_table_data(roster_query)

                            # Display team name as header
                            st.markdown(f"""
                            <div style="
                                background-color: #f8f9fa;
                                padding: 10px;
                                border-radius: 8px;
                                margin-bottom: 10px;
                                font-weight: 600;
                                color: #2c3e50;
                                text-align: center;
                            ">
                                {team_name}
                            </div>
                            """, unsafe_allow_html=True)

                            # Display roster
                            for _, player in roster_data.iterrows():
                                headshot = f'<img src="{player["headshot_url"]}" style="width:40px;height:40px;border-radius:50%;margin-right:10px;">' if pd.notna(player["headshot_url"]) else ""
                                status = f" - {player['game_status']} {player['opponent']}" if pd.notna(player['game_status']) else ""
                                
                                st.markdown(f"""
                                <div style="
                                    display: flex;
                                    align-items: center;
                                    padding: 8px;
                                    background-color: white;
                                    border-radius: 8px;
                                    margin-bottom: 8px;
                                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                                ">
                                    <div style="
                                        min-width: 40px;
                                        font-weight: 500;
                                        color: #666;
                                        text-align: center;
                                    ">
                                        {player['pos']}
                                    </div>
                                    <div style="
                                        display: flex;
                                        align-items: center;
                                        flex-grow: 1;
                                        margin-left: 10px;
                                    ">
                                        {headshot}
                                        <div>
                                            <div style="font-weight: 500;">{player['player']}</div>
                                            <div style="font-size: 12px; color: #666;">
                                                {player['nfl_team']} - {player['actual_position']}{status}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)

        # Schedule subtab
        with standings_subtabs[1]:
            # Add team selector
            col1, col2 = st.columns([4, 1])
            with col2:
                # Get all teams for dropdown
                teams_query = f"""
                SELECT DISTINCT t.name as team_name, t.team_key
                FROM teams t
                WHERE t.team_key LIKE '{selected_league}.t.%'
                ORDER BY t.name
                """
                teams_data = load_table_data(teams_query)
                selected_team = st.selectbox(
                    "Team",
                    teams_data['team_name'].tolist(),
                    key="schedule_team_selector"
                )
            
            # Get team's schedule data
            schedule_query = f"""
            WITH team_games AS (
                SELECT 
                    lg.week,
                    t.name as team_name,
                    CASE 
                        WHEN lg.home_team_key = t.team_key THEN t2.name
                        ELSE t1.name
                    END as opponent,
                    CASE 
                        WHEN lg.home_team_key = t.team_key THEN lg.home_team_points
                        ELSE lg.away_team_points
                    END as score,
                    CASE 
                        WHEN lg.home_team_key = t.team_key THEN lg.away_team_points
                        ELSE lg.home_team_points
                    END as opp_score,
                    CASE
                        WHEN (lg.home_team_key = t.team_key AND lg.home_team_points > lg.away_team_points) OR
                             (lg.away_team_key = t.team_key AND lg.away_team_points > lg.home_team_points) THEN 'Win'
                        WHEN lg.home_team_points = lg.away_team_points THEN 'Tie'
                        ELSE 'Loss'
                    END as result,
                    lg.status
                FROM league_games lg
                JOIN teams t ON (t.team_key = lg.home_team_key OR t.team_key = lg.away_team_key)
                JOIN teams t1 ON lg.home_team_key = t1.team_key
                JOIN teams t2 ON lg.away_team_key = t2.team_key
                WHERE t.name = ?
                AND lg.league_key = '{selected_league}'
                ORDER BY lg.week
            )
            SELECT * FROM team_games
            """
            schedule_data = load_table_data(schedule_query, params=(selected_team,))
            
            # Custom CSS for compact schedule display
            st.markdown("""
            <style>
            .compact-schedule {
                display: grid;
                grid-template-columns: repeat(1, 1fr);
                gap: 4px;
                margin-top: 10px;
            }
            .schedule-card {
                background: white;
                border-radius: 6px;
                padding: 8px 12px;
                box-shadow: 0 1px 2px rgba(0,0,0,0.1);
            }
            .schedule-card:hover {
                box-shadow: 0 2px 4px rgba(0,0,0,0.15);
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Start the grid container
            st.markdown('<div class="compact-schedule">', unsafe_allow_html=True)
            
            # Display schedule as compact cards
            for _, game in schedule_data.iterrows():
                st.markdown(f"""
                <div class="schedule-card">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div style="flex: 1; text-align: right; padding-right: 12px;">
                            <div style="font-size: 14px; font-weight: 600; color: {
                                '#2ecc71' if game['result'] == 'Win'
                                else '#666'}">{game['team_name']}</div>
                            <div style="font-size: 16px; font-weight: 500; color: #2c3e50;">
                                {game['score'] if pd.notna(game['score']) else '-'}
                            </div>
                        </div>
                        <div style="padding: 0 12px; text-align: center;">
                            <div style="font-size: 12px; color: #666;">Week {game['week']}</div>
                            <div style="font-size: 11px; margin-top: 2px; color: {
                                '#2ecc71' if game['result'] == 'Win'
                                else '#e74c3c' if game['result'] == 'Loss'
                                else '#666'
                            };">
                                {game['result']}
                            </div>
                        </div>
                        <div style="flex: 1; text-align: left; padding-left: 12px;">
                            <div style="font-size: 14px; font-weight: 600;">{game['opponent']}</div>
                            <div style="font-size: 16px; font-weight: 500; color: #2c3e50;">
                                {game['opp_score'] if pd.notna(game['opp_score']) else '-'}
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Close the grid container
            st.markdown('</div>', unsafe_allow_html=True)

        # Playoffs & Ratings & Levels subtab
        with standings_subtabs[2]:
            st.markdown("### Playoffs & Ratings")
            
            # Get playoff data
            playoff_query = f"""
            WITH team_stats AS (
                SELECT 
                    t.name as team_name,
                    t.stats,
                    ls.rank,
                    ls.wins,
                    ls.losses,
                    ls.ties,
                    ls.points_for
                FROM teams t
                JOIN league_standings ls ON t.team_key = ls.team_key
                WHERE ls.league_key = '{selected_league}'
            )
            SELECT * FROM team_stats
            ORDER BY rank
            """
            playoff_data = load_table_data(playoff_query)
            
            for _, team in playoff_data.iterrows():
                stats = json.loads(team['stats']) if pd.notna(team['stats']) else {}
                playoff_status = stats.get('clinched_playoffs', None)
                felo_score = stats.get('manager', {}).get('felo_score', 'N/A')
                felo_tier = stats.get('manager', {}).get('felo_tier', 'N/A')
                
                st.markdown(f"""
                <div style="
                    background: white;
                    border-radius: 8px;
                    padding: 16px;
                    margin-bottom: 12px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                ">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div style="flex: 2;">
                            <div style="font-weight: 600; font-size: 16px;">{team['team_name']}</div>
                            <div style="font-size: 14px; color: #666;">
                                Rank: {team['rank']} • Record: {team['wins']}-{team['losses']}-{team['ties']}
                            </div>
                        </div>
                        <div style="flex: 1; text-align: center;">
                            <div style="font-size: 14px; color: #666;">Playoff Status</div>
                            <div style="font-weight: 500; color: {
                                '#2ecc71' if playoff_status == 1
                                else '#e74c3c' if playoff_status == 0
                                else '#666'
                            };">
                                {
                                    '🏆 Clinched' if playoff_status == 1
                                    else '❌ Eliminated' if playoff_status == 0
                                    else '⏳ TBD'
                                }
                            </div>
                        </div>
                        <div style="flex: 1; text-align: right;">
                            <div style="font-size: 14px; color: #666;">FELO Rating</div>
                            <div style="font-weight: 500;">
                                {felo_score} • {felo_tier.upper() if pd.notna(felo_tier) else 'N/A'}
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
    
    # Team Data Tab
    with tabs[2]:
        # Teams with Managers
        st.subheader("Teams & Managers")
        teams_query = f"""
        WITH RankedTeams AS (
            SELECT 
                t.team_key,
                t.name as team_name,
                t.logo_url,
                t.stats,
                tm.nickname as manager_name,
                tm.guid,
                ls.rank,
                ls.wins || '-' || ls.losses || '-' || ls.ties as record,
                ROUND(ls.points_for, 1) as points_for,
                ROW_NUMBER() OVER (PARTITION BY t.team_key ORDER BY ls.rank) as rn
            FROM teams t
            JOIN league_standings ls ON t.team_key = ls.team_key
            LEFT JOIN team_managers tm ON t.team_key = tm.team_key
            WHERE ls.league_key = '{selected_league}'
        )
        SELECT 
            team_key,
            team_name,
            logo_url,
            stats,
            manager_name,
            guid,
            rank,
            record,
            points_for
        FROM RankedTeams
        WHERE rn = 1
        ORDER BY rank
        """
        teams_data = load_table_data(teams_query)
        
        # Initialize session state for selected team if not exists
        if 'selected_team' not in st.session_state:
            st.session_state.selected_team = None
        
        # Create a grid layout for teams
        cols = st.columns(3)
        for idx, row in teams_data.iterrows():
            col_idx = idx % 3
            with cols[col_idx]:
                # Create a unique key for each team's button
                team_key = f"team_{row['team_key']}"
                
                # Create the clickable card using a button with custom styling
                if st.button(
                    label="",
                    key=team_key,
                    use_container_width=True,
                    type="secondary",
                    help="Click to view team details"
                ):
                    st.session_state.selected_team = row['team_name']
                
                # Style the card content
                st.markdown(f"""
                    <div style="
                        margin-top: -60px;
                        background-color: {'#f0f8ff' if st.session_state.selected_team == row['team_name'] else 'white'};
                        padding: 20px;
                        border-radius: 12px;
                        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                        border: {'2px solid #0063dc' if st.session_state.selected_team == row['team_name'] else '1px solid #e0e0e0'};
                        transition: all 0.3s ease;
                        margin-bottom: 20px;
                    ">
                        <div style="display: flex; align-items: center; margin-bottom: 15px; gap: 15px;">
                            <div style="
                                width: 60px;
                                height: 60px;
                                border-radius: 8px;
                                overflow: hidden;
                                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                            ">
                                <img src="{row['logo_url'] if pd.notna(row['logo_url']) else 'https://s.yimg.com/cv/apiv2/default/nfl/nfl_2_z.png'}" 
                                     style="width: 100%; height: 100%; object-fit: cover;">
                            </div>
                            <div style="flex-grow: 1;">
                                <div style="
                                    font-weight: 600;
                                    font-size: 18px;
                                    color: #2c3e50;
                                    margin-bottom: 4px;
                                ">{row['team_name']}</div>
                                <div style="
                                    color: #666;
                                    font-size: 14px;
                                ">Manager: {row['manager_name'] if pd.notna(row['manager_name']) else 'Unknown'}</div>
                            </div>
                        </div>
                        <div style="
                            display: grid;
                            grid-template-columns: repeat(2, 1fr);
                            gap: 12px;
                            background-color: {'#e6f3ff' if st.session_state.selected_team == row['team_name'] else '#f8f9fa'};
                            padding: 12px;
                            border-radius: 8px;
                        ">
                            <div style="
                                display: flex;
                                flex-direction: column;
                                gap: 4px;
                            ">
                                <div style="color: #666; font-size: 12px;">Rank</div>
                                <div style="font-weight: 500; font-size: 14px; color: #2c3e50;">#{row['rank']}</div>
                            </div>
                            <div style="
                                display: flex;
                                flex-direction: column;
                                gap: 4px;
                            ">
                                <div style="color: #666; font-size: 12px;">Record</div>
                                <div style="font-weight: 500; font-size: 14px; color: #2c3e50;">{row['record']}</div>
                            </div>
                            <div style="
                                display: flex;
                                flex-direction: column;
                                gap: 4px;
                            ">
                                <div style="color: #666; font-size: 12px;">Points</div>
                                <div style="font-weight: 500; font-size: 14px; color: #2c3e50;">{row['points_for']}</div>
                            </div>
                            <div style="
                                display: flex;
                                flex-direction: column;
                                gap: 4px;
                            ">
                                <div style="color: #666; font-size: 12px;">GUID</div>
                                <div style="font-weight: 500; font-size: 14px; color: #2c3e50;">{row['guid'][:8] if pd.notna(row['guid']) else 'N/A'}</div>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        
        # Show selected team's stats
        if st.session_state.selected_team:
            selected_team_data = teams_data[teams_data['team_name'] == st.session_state.selected_team].iloc[0]
            
            st.markdown("---")
            st.markdown(f"### {st.session_state.selected_team} Details")
            
            if not pd.isna(selected_team_data['stats']):
                stats = json.loads(selected_team_data['stats'])
                manager_info = stats.get('manager', {})
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Team Info**")
                    st.markdown(f"Manager: {manager_info.get('nickname', 'Unknown')}")
                    st.markdown(f"Rank: {selected_team_data['rank']}")
                    st.markdown(f"Record: {selected_team_data['record']}")
                    
                    st.markdown("**Draft & Transactions**")
                    st.markdown(f"Draft Grade: {stats.get('draft_grade', 'N/A')}")
                    st.markdown(f"Moves Made: {stats.get('number_of_moves', '0')}")
                    st.markdown(f"Trades: {stats.get('number_of_trades', '0')}")
                    st.markdown(f"Waiver Priority: {stats.get('waiver_priority', 'N/A')}")
                
                with col2:
                    st.markdown("**Manager Performance**")
                    st.markdown(f"FELO Score: {manager_info.get('felo_score', 'N/A')}")
                    st.markdown(f"Tier: {manager_info.get('felo_tier', 'N/A').upper()}")
                    
                    st.markdown("**Playoff Status**")
                    playoff_status = "🏆 Clinched" if stats.get('clinched_playoffs') == 1 else "❌ Eliminated" if stats.get('clinched_playoffs') == 0 else "⏳ TBD"
                    st.markdown(f"Status: {playoff_status}")
                
                # Add roster information
                st.markdown("**Current Roster**")
                roster_query = f"""
                SELECT 
                    p.name as player,
                    p.position,
                    lr.selected_position,
                    CASE WHEN lr.is_starting = 1 THEN 'Starter' ELSE 'Bench' END as role
                FROM league_rosters lr
                JOIN players p ON lr.player_key = p.player_key
                JOIN teams t ON lr.team_key = t.team_key
                WHERE t.name = '{st.session_state.selected_team}'
                AND lr.week = (SELECT MAX(week) FROM league_rosters)
                ORDER BY lr.is_starting DESC, lr.selected_position
                """
                roster_data = load_table_data(roster_query)
                st.dataframe(
                    roster_data,
                    hide_index=True,
                    use_container_width=True
                )
    
    # Player Data Tab
    with tabs[3]:
        # Players
        st.markdown("""
        <style>
        .team-box {
            background-color: #f8f9fa;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 20px;
        }
        .week-nav {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 20px;
        }
        .week-nav button {
            background: none;
            border: none;
            cursor: pointer;
            padding: 4px 8px;
            color: #0063dc;
            font-size: 14px;
        }
        .week-nav button:hover {
            background-color: #f0f0f0;
            border-radius: 4px;
        }
        .week-nav span {
            font-size: 14px;
            font-weight: 500;
            color: #333;
        }
        .stat-tabs {
            display: flex;
            gap: 24px;
            margin-bottom: 16px;
            border-bottom: 1px solid #e0e0e0;
            padding-bottom: 8px;
        }
        .stat-tab {
            color: #666;
            font-size: 13px;
            cursor: pointer;
            padding: 4px 0;
            position: relative;
        }
        .stat-tab.active {
            color: #0063dc;
            font-weight: 500;
        }
        .stat-tab.active:after {
            content: '';
            position: absolute;
            bottom: -9px;
            left: 0;
            right: 0;
            height: 2px;
            background-color: #0063dc;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Get all teams for the dropdown
        teams_query = f"""
        SELECT DISTINCT t.name as team_name, t.stats, t.logo_url, ls.rank, ls.wins, ls.losses, ls.ties, 
               ROUND(ls.points_for, 2) as points_for
        FROM teams t
        JOIN league_standings ls ON t.team_key = ls.team_key
        WHERE ls.league_key = '{selected_league}'
        ORDER BY ls.rank
        """
        teams_list = load_table_data(teams_query)
        
        # Create layout with team box and week navigation
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Team box with record and points
            selected_team = st.selectbox(
                "Select Team",
                teams_list['team_name'].tolist(),
                index=0,
                label_visibility="collapsed"
            )
            
            # Get team details
            team_data = teams_list[teams_list['team_name'] == selected_team].iloc[0]
            record = f"{team_data['wins']}-{team_data['losses']}-{team_data['ties']}"
            points = team_data['points_for']
            rank = team_data['rank']
            
            # Get opponent team details (for now using placeholder)
            opponent_logo = 'https://s.yimg.com/cv/apiv2/default/nfl/nfl_12_z.png'
            
            # Display team box
            st.markdown(f"""
            <div class="team-box">
                <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px;">
                    <div style="flex: 1;">
                        <div style="font-size: 24px; font-weight: 700; color: #2c3e50; margin-bottom: 4px; display: flex; align-items: center; gap: 12px;">
                            <img src="{team_data['logo_url'] if pd.notna(team_data['logo_url']) else 'https://s.yimg.com/cv/apiv2/default/nfl/nfl_2_z.png'}" 
                                 style="width: 80px; height: 80px; border-radius: 50%; border: 1px solid #eee;">
                            <div style="display: flex; flex-direction: column;">
                                <div style="display: flex; align-items: center;">
                                    {selected_team} ▾
                                </div>
                                <div style="font-size: 16px; color: #666; margin-top: 4px;">
                                    {record}
                                </div>
                                <div style="font-size: 16px; color: #666;">
                                    {points}
                                </div>
                            </div>
                        </div>
                        <div style="margin-top: 12px;">
                            <div style="font-size: 13px; color: #666;">caleb (Since '02) • Silver 671 • Edit Team Settings</div>
                        </div>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-size: 13px; color: #666;">Week {selected_week} vs The Osito • 3rd</div>
                        <div style="display: flex; align-items: center; gap: 12px; margin-top: 8px;">
                            <div style="display: flex; flex-direction: column; align-items: center;">
                                <img src="{team_data['logo_url'] if pd.notna(team_data['logo_url']) else 'https://s.yimg.com/cv/apiv2/default/nfl/nfl_2_z.png'}" 
                                     style="width: 45px; height: 45px; border-radius: 50%; margin-bottom: 4px; border: 1px solid #eee;">
                                <div style="font-size: 16px; font-weight: 600; color: #2c3e50;">115.28</div>
                                <div style="font-size: 13px; color: #666;">112.74</div>
                            </div>
                            <div style="font-size: 13px; color: #666; margin: 0 4px;">vs</div>
                            <div style="display: flex; flex-direction: column; align-items: center;">
                                <img src="{opponent_logo}" 
                                     style="width: 45px; height: 45px; border-radius: 50%; margin-bottom: 4px; border: 1px solid #eee;">
                                <div style="font-size: 16px; font-weight: 600; color: #2c3e50;">117.58</div>
                                <div style="font-size: 13px; color: #666;">91.44</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            # Add week navigation
            weeks_query = f"""
            SELECT DISTINCT week 
            FROM player_stats 
            WHERE league_key = '{selected_league}'
            ORDER BY week DESC
            """
            weeks = load_table_data(weeks_query)
            
            if not weeks.empty:
                # Create week selector with navigation buttons
                selected_week = st.selectbox(
                    "Week",
                    weeks['week'].tolist(),
                    label_visibility="collapsed"
                )
            else:
                selected_week = None
                st.warning("No stats available for this league")
                return
        
        # Parse league key components
        game_id = int(selected_league.split('.')[0])
        league_id = int(selected_league.split('.')[2])
        
        # Modified query to properly join with league and team data
        players_query = f"""
        WITH team_data AS (
            SELECT DISTINCT 
                team as team_abbrev,
                headshot_url as team_logo
            FROM players 
            WHERE position = 'DEF'
        )
        SELECT DISTINCT 
            p.name as player_name,
            p.team as player_team,
            p.position as player_position,
            p.status,
            p.injury_note,
            p.headshot_url as player_headshot,
            td.team_logo,
            r.selected_position,
            r.is_starting,
            ROUND(ps.points, 1) as fan_pts,
            ROUND(ps.projected_points, 1) as proj_pts,
            ps.passing_attempts as pass_att,
            ps.passing_completions as pass_comp,
            ps.passing_yards as pass_yds,
            ps.passing_touchdowns as pass_td,
            ps.passing_interceptions as int,
            ps.passing_2pt_conversions as pass_2pt,
            ps.rushing_attempts as rush_att,
            ps.rushing_yards as rush_yds,
            ps.rushing_touchdowns as rush_td,
            ps.rushing_2pt_conversions as rush_2pt,
            ps.receptions as rec,
            ps.receiving_yards as rec_yds,
            ps.receiving_touchdowns as rec_td,
            ps.receiving_2pt_conversions as rec_2pt,
            ps.targets as tgt,
            ps.fumbles_lost as fum
        FROM players p
        JOIN rosters r ON p.player_key = r.player_key
        JOIN teams t ON t.name = ?
        LEFT JOIN team_data td ON td.team_abbrev = p.team
        LEFT JOIN player_stats ps ON p.player_key = ps.player_key 
            AND ps.week = {selected_week}
            AND ps.league_key = '{selected_league}'
        WHERE r.week = {selected_week}
        AND r.game_id = {game_id}
        AND r.league_id = {league_id}
        AND r.team_id = (
            SELECT CAST(SUBSTR(team_key, INSTR(team_key, '.t.') + 3) as INTEGER)
            FROM teams 
            WHERE name = ?
            LIMIT 1
        )
        ORDER BY r.is_starting DESC, r.selected_position, ps.points DESC NULLS LAST
        """
        players_data = load_table_data(players_query, params=(selected_team, selected_team))
        
        # Convert is_starting to a more readable format
        players_data['is_starting'] = players_data['is_starting'].map({1: 'Starter', 0: 'Bench'})
        
        # Create a custom column for player info with headshot and team logo
        def player_info_with_headshot(row):
            team_logo_html = f'''<img src="{row['team_logo']}" style="width: 25px; height: 25px; border-radius: 50%; position: absolute; bottom: -4px; right: -4px; border: 1px solid #fff; box-shadow: 0 1px 2px rgba(0,0,0,0.1);">''' if pd.notna(row['team_logo']) else ''
            
            # Format status to include game result if available
            status_text = row['status'] if pd.notna(row['status']) else ''
            injury_text = f" - {row['injury_note']}" if pd.notna(row['injury_note']) else ''
            
            return f'''<div style="display: flex; align-items: center; gap: 12px; padding: 4px;">
                <div style="position: relative; width: 45px; height: 45px;">
                    <img src="{row['player_headshot']}" style="width: 45px; height: 45px; border-radius: 50%; border: 1px solid #eee; object-fit: cover;">
                    {team_logo_html}
                </div>
                <div style="display: flex; flex-direction: column; gap: 2px;">
                    <div style="font-weight: 600; font-size: 13px; color: #2c3e50;">{row['player_name']}</div>
                    <div style="color: #666; font-size: 12px;">{row['player_team']} - {row['player_position']}</div>
                    <div style="color: #e74c3c; font-size: 11px;">{status_text}{injury_text}</div>
                </div>
            </div>'''
        
        if not players_data.empty:
            # Apply the player info formatter
            players_data['Player'] = players_data.apply(player_info_with_headshot, axis=1)
            
            # Clean up any remaining newlines or extra spaces in the HTML
            players_data['Player'] = players_data['Player'].str.replace('\n', '').str.replace('  ', ' ')
            
            # Define column display configuration to match Yahoo's layout
            display_columns = {
                'Player': 'Player',
                'selected_position': 'Pos',
                'fan_pts': 'Fan Pts',
                'proj_pts': 'Proj Pts',
                'pass_yds': 'Yds',
                'pass_td': 'TD',
                'int': 'Int',
                'pass_att': 'Att*',
                'rush_yds': 'Yds',
                'rush_td': 'TD',
                'rec': 'Rec',
                'rec_yds': 'Yds',
                'rec_td': 'TD',
                'tgt': 'Tgt*',
                'rush_2pt': '2PT',
                'fum': 'Lost'
            }
            
            # Create display DataFrame with renamed columns
            display_df = players_data.rename(columns=display_columns)
            display_df = display_df[display_columns.values()]
            
            # Add custom CSS for table styling
            st.markdown("""
            <style>
            #players_table {
                width: 100%;
                border-collapse: collapse;
                margin: 1em 0;
                font-size: 13px;
            }
            #players_table th {
                background-color: #f8f9fa;
                padding: 4px 8px;
                text-align: left;
                font-weight: 600;
                color: #333;
                position: sticky;
                top: 0;
                z-index: 1;
                border-bottom: 1px solid #dee2e6;
            }
            #players_table td {
                padding: 4px 8px;
                border-bottom: 1px solid #eee;
                vertical-align: middle;
            }
            #players_table tr:hover {
                background-color: #f5f5f5;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Display the table
            st.write(
                display_df.to_html(
                    escape=False,
                    index=False,
                    table_id='players_table'
                ),
                unsafe_allow_html=True
            )
        else:
            st.warning("No players found for the selected week.")
    
    # Game Data Tab
    with tabs[4]:
        st.subheader("League Games")
        
        # Add week filter
        weeks_query = f"""
        SELECT DISTINCT week, 
               MIN(game_start_time) as week_start,
               COUNT(*) as games_count
        FROM league_games 
        WHERE league_key = '{selected_league}'
        GROUP BY week
        ORDER BY week
        """
        weeks = load_table_data(weeks_query)
        
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_weeks = st.multiselect(
                "Select Weeks",
                options=weeks['week'].tolist(),
                default=weeks['week'].tolist()[-4:] if not weeks.empty else None,  # Default to last 4 weeks
                key="game_weeks",
                format_func=lambda x: f"Week {x} ({weeks[weeks['week'] == x]['games_count'].iloc[0]} games)"
            )
        
        with col2:
            sort_order = st.selectbox(
                "Sort Order",
                options=["Latest First", "Earliest First"],
                index=0
            )
        
        if selected_weeks:
            # Get games data with more details
            games_query = f"""
            WITH game_stats AS (
                SELECT 
                    lg.week,
                    lg.home_team_key,
                    lg.away_team_key,
                    t1.name as home_team,
                    t2.name as away_team,
                    lg.home_team_points,
                    lg.away_team_points,
                    lg.status,
                    lg.game_start_time,
                    CASE 
                        WHEN lg.home_team_points > lg.away_team_points THEN t1.name
                        WHEN lg.away_team_points > lg.home_team_points THEN t2.name
                        ELSE 'Tie'
                    END as winner,
                    ABS(lg.home_team_points - lg.away_team_points) as point_difference,
                    CASE 
                        WHEN lg.status = 'postevent' THEN 'Final'
                        WHEN lg.status = 'midevent' THEN 'In Progress'
                        ELSE 'Scheduled'
                    END as game_status
                FROM league_games lg
                JOIN teams t1 ON lg.home_team_key = t1.team_key
                JOIN teams t2 ON lg.away_team_key = t2.team_key
                WHERE lg.league_key = '{selected_league}'
                AND lg.week IN ({','.join(map(str, selected_weeks))})
            )
            SELECT 
                *,
                RANK() OVER (PARTITION BY week ORDER BY point_difference) as closest_rank,
                RANK() OVER (PARTITION BY week ORDER BY home_team_points + away_team_points DESC) as highest_scoring_rank
            FROM game_stats
            ORDER BY week {' DESC' if sort_order == 'Latest First' else 'ASC'}, game_start_time
            """
            games_data = load_table_data(games_query)
            
            if not games_data.empty:
                # Group games by week
                for week in sorted(games_data['week'].unique(), reverse=(sort_order == "Latest First")):
                    week_games = games_data[games_data['week'] == week]
                    
                    st.markdown(f"""
                    <div style="
                        background-color: #f8f9fa;
                        padding: 12px 20px;
                        border-radius: 8px;
                        margin: 20px 0 10px 0;
                        font-weight: 600;
                        color: #2c3e50;
                        font-size: 18px;
                    ">
                        Week {week}
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Display week statistics
                    total_points = week_games['home_team_points'].sum() + week_games['away_team_points'].sum()
                    avg_points = total_points / (len(week_games) * 2)
                    closest_game = week_games[week_games['closest_rank'] == 1].iloc[0]
                    highest_scoring = week_games[week_games['highest_scoring_rank'] == 1].iloc[0]
                    
                    st.markdown(f"""
                    <div class="metric-row">
                        <div class="small-metric-container">
                            <div class="small-metric-label">Average Score</div>
                            <div class="small-metric-value">{avg_points:.1f}</div>
                        </div>
                        <div class="small-metric-container">
                            <div class="small-metric-label">Closest Game</div>
                            <div class="small-metric-value">{closest_game['point_difference']:.1f} pts</div>
                            <div style="font-size: 11px; color: #666;">
                                {closest_game['home_team']} vs {closest_game['away_team']}
                            </div>
                        </div>
                        <div class="small-metric-container">
                            <div class="small-metric-label">Highest Scoring</div>
                            <div class="small-metric-value">
                                {highest_scoring['home_team_points'] + highest_scoring['away_team_points']:.1f} pts
                            </div>
                            <div style="font-size: 11px; color: #666;">
                                {highest_scoring['home_team']} vs {highest_scoring['away_team']}
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Display games for the week
                    for _, game in week_games.iterrows():
                        st.markdown(f"""
                        <div style="
                            background: white;
                            border-radius: 8px;
                            padding: 16px;
                            margin-bottom: 12px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        ">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div style="flex: 1; text-align: right; padding-right: 20px;">
                                    <div style="font-weight: 600; font-size: 16px; color: {
                                        '#2ecc71' if game['winner'] == game['home_team'] 
                                        else '#666'}">{game['home_team']}</div>
                                    <div style="font-size: 20px; font-weight: 500; color: #2c3e50;">
                                        {game['home_team_points']:.1f}
                                    </div>
                                </div>
                                <div style="padding: 0 20px; text-align: center;">
                                    <div style="font-size: 14px; color: #666;">VS</div>
                                    <div style="font-size: 12px; margin-top: 4px; color: {
                                        '#2ecc71' if game['game_status'] == 'Final'
                                        else '#e67e22' if game['game_status'] == 'In Progress'
                                        else '#666'
                                    };">
                                        {game['game_status']}
                                    </div>
                                </div>
                                <div style="flex: 1; text-align: left; padding-left: 20px;">
                                    <div style="font-weight: 600; font-size: 16px; color: {
                                        '#2ecc71' if game['winner'] == game['away_team']
                                        else '#666'}">{game['away_team']}</div>
                                    <div style="font-size: 20px; font-weight: 500; color: #2c3e50;">
                                        {game['away_team_points']:.1f}
                                    </div>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.warning("No games found for the selected weeks.")
        else:
            st.warning("Please select at least one week to view games.")
    
    # Draft Results Tab
    with tabs[5]:
        st.subheader("Draft Results")
        
        # Load draft data
        draft_query = f"""
        SELECT 
            ldr.round,
            ldr.pick,
            t.name as team_name,
            p.name as player_name,
            p.position,
            p.team as nfl_team,
            p.status
        FROM league_draft_results ldr
        JOIN teams t ON ldr.team_key = t.team_key
        JOIN players p ON ldr.player_key = p.player_key
        WHERE ldr.league_key = '{selected_league}'
        ORDER BY ldr.round, ldr.pick
        """
        draft_data = load_table_data(draft_query)
        
        if not draft_data.empty:
            # Create a custom CSS for the draft board
            st.markdown("""
            <style>
            .draft-round {
                background: #f8f9fa;
                border-radius: 8px;
                padding: 16px;
                height: 100%;
            }
            .round-header {
                background: #e9ecef;
                padding: 8px 12px;
                border-radius: 6px;
                margin-bottom: 12px;
                font-weight: 600;
                color: #2c3e50;
                text-align: center;
            }
            .draft-pick {
                background: white;
                border-radius: 6px;
                padding: 12px;
                margin-bottom: 8px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }
            .pick-number {
                font-size: 13px;
                color: #666;
                margin-bottom: 4px;
            }
            .team-name {
                font-weight: 600;
                color: #2c3e50;
                margin-bottom: 4px;
            }
            .player-name {
                color: #0063dc;
                margin-bottom: 2px;
                font-weight: 500;
            }
            .player-info {
                font-size: 12px;
                color: #666;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Group data by round
            rounds = sorted(draft_data['round'].unique())
            
            # Calculate number of rows needed (3 rounds per row)
            num_rows = (len(rounds) + 2) // 3
            
            # Create grid layout using Streamlit columns
            for row in range(num_rows):
                cols = st.columns(3)
                for col in range(3):
                    round_idx = row * 3 + col
                    if round_idx < len(rounds):
                        round_num = rounds[round_idx]
                        round_picks = draft_data[draft_data['round'] == round_num].sort_values('pick')
                        
                        with cols[col]:
                            st.markdown(f"""
                            <div class="draft-round">
                                <div class="round-header">Round {round_num}</div>
                            """, unsafe_allow_html=True)
                            
                            for _, pick in round_picks.iterrows():
                                status_text = f" - {pick['status']}" if pd.notna(pick['status']) else ""
                                st.markdown(f"""
                                <div class="draft-pick">
                                    <div class="pick-number">{pick['pick']}.</div>
                                    <div class="team-name">{pick['team_name']}</div>
                                    <div class="player-name">{pick['player_name']}</div>
                                    <div class="player-info">({pick['nfl_team']} - {pick['position']}){status_text}</div>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            st.markdown('</div>', unsafe_allow_html=True)
            
            # Add draft summary below the board
            st.markdown("---")
            st.subheader("Draft Summary")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Position distribution
                position_counts = draft_data['position'].value_counts()
                st.markdown("**Position Distribution**")
                position_df = pd.DataFrame({
                    'Position': position_counts.index,
                    'Count': position_counts.values
                })
                st.dataframe(position_df, hide_index=True)
            
            with col2:
                # Team pick counts
                team_counts = draft_data['team_name'].value_counts()
                st.markdown("**Picks by Team**")
                team_df = pd.DataFrame({
                    'Team': team_counts.index,
                    'Picks': team_counts.values
                })
                st.dataframe(team_df, hide_index=True)
        else:
            st.warning("No draft data available for this league.")

    # Rosters Tab
    with tabs[6]:
        st.subheader("Starting Rosters")
        
        # Add week selector in the top right
        col1, col2 = st.columns([4, 1])
        with col2:
            weeks_query = f"""
            SELECT DISTINCT week 
            FROM rosters 
            WHERE game_id = {selected_league.split('.')[0]}
            AND league_id = {selected_league.split('.')[2]}
            ORDER BY week DESC
            """
            weeks_data = load_table_data(weeks_query)
            selected_week = st.selectbox(
                "Week",
                weeks_data['week'].tolist(),
                key="roster_week_selector"
            )

        # Get all teams for the selected week
        teams_query = f"""
        SELECT DISTINCT t.name as team_name, t.team_key
        FROM teams t
        WHERE t.team_key LIKE '{selected_league}.t.%'
        ORDER BY t.name
        """
        teams_data = load_table_data(teams_query)

        # Create grid layout for teams (3 teams per row)
        num_teams = len(teams_data)
        num_rows = (num_teams + 2) // 3  # Calculate number of rows needed

        for row in range(num_rows):
            cols = st.columns(3)  # Create 3 columns for each row
            for col in range(3):
                team_idx = row * 3 + col
                if team_idx < num_teams:
                    team_name = teams_data['team_name'].iloc[team_idx]
                    team_key = teams_data['team_key'].iloc[team_idx]
                    team_id = int(team_key.split('.')[-1])

                    with cols[col]:
                        # Get roster data for the team
                        roster_query = f"""
                        SELECT 
                            r.selected_position as pos,
                            p.name as player,
                            p.team as nfl_team,
                            p.position as actual_position,
                            p.status,
                            p.headshot_url,
                            r.is_starting,
                            p.status as game_status,
                            NULL as opponent
                        FROM rosters r
                        JOIN players p ON r.player_key = p.player_key
                        LEFT JOIN player_stats ps ON r.player_key = ps.player_key 
                            AND ps.week = {selected_week}
                            AND ps.league_key = '{selected_league}'
                        WHERE r.team_id = {team_id}
                        AND r.week = {selected_week}
                        AND r.game_id = {selected_league.split('.')[0]}
                        AND r.league_id = {selected_league.split('.')[2]}
                        AND r.is_starting = 1 -- Only show starters
                        ORDER BY 
                            CASE r.selected_position
                                WHEN 'QB' THEN 1
                                WHEN 'WR' THEN 2
                                WHEN 'RB' THEN 3
                                WHEN 'TE' THEN 4
                                WHEN 'W/R/T' THEN 5
                                WHEN 'K' THEN 6
                                WHEN 'DEF' THEN 7
                                ELSE 8
                            END,
                            p.name
                        """
                        roster_data = load_table_data(roster_query)

                        # Display team name as header
                        st.markdown(f"""
                        <div style="
                            background-color: #f8f9fa;
                            padding: 10px;
                            border-radius: 8px;
                            margin-bottom: 10px;
                            font-weight: 600;
                            color: #2c3e50;
                            text-align: center;
                        ">
                            {team_name}
                        </div>
                        """, unsafe_allow_html=True)

                        # Display roster
                        for _, player in roster_data.iterrows():
                            headshot = f'<img src="{player["headshot_url"]}" style="width:40px;height:40px;border-radius:50%;margin-right:10px;">' if pd.notna(player["headshot_url"]) else ""
                            status = f" - {player['game_status']} {player['opponent']}" if pd.notna(player['game_status']) else ""
                            
                            st.markdown(f"""
                            <div style="
                                display: flex;
                                align-items: center;
                                padding: 8px;
                                background-color: white;
                                border-radius: 8px;
                                margin-bottom: 8px;
                                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                            ">
                                <div style="
                                    min-width: 40px;
                                    font-weight: 500;
                                    color: #666;
                                    text-align: center;
                                ">
                                    {player['pos']}
                                </div>
                                <div style="
                                    display: flex;
                                    align-items: center;
                                    flex-grow: 1;
                                    margin-left: 10px;
                                ">
                                    {headshot}
                                    <div>
                                        <div style="font-weight: 500;">{player['player']}</div>
                                        <div style="font-size: 12px; color: #666;">
                                            {player['nfl_team']} - {player['actual_position']}{status}
                                        </div>
                                    </div>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()

