import sqlite3
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path='fantasy_data.db'):
        """Initialize database connection and create tables if they don't exist"""
        try:
            self.db_path = db_path
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.cursor = self.conn.cursor()
            self._create_tables()
            self._init_sport_codes()
        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")
            raise

    def _create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            # Create players table with sport_code
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS players (
                    player_key TEXT PRIMARY KEY,
                    sport_code TEXT NOT NULL,
                    name TEXT,
                    team TEXT,
                    position TEXT,
                    status TEXT,
                    injury_note TEXT,
                    headshot_url TEXT,
                    stats TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create teams table with sport_code
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    team_key TEXT PRIMARY KEY,
                    sport_code TEXT NOT NULL,
                    name TEXT,
                    logo_url TEXT,
                    stats TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create leagues table with sport_code
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS leagues (
                    league_key TEXT PRIMARY KEY,
                    sport_code TEXT NOT NULL,
                    name TEXT,
                    season TEXT,
                    settings TEXT,
                    standings TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create games table with all fields
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS games (
                    game_key TEXT PRIMARY KEY,
                    game_id TEXT,
                    sport_code TEXT NOT NULL,
                    name TEXT,
                    season TEXT,
                    game_code TEXT,
                    game_type TEXT,
                    url TEXT,
                    is_registration_over INTEGER,
                    is_game_over INTEGER,
                    is_offseason INTEGER,
                    editorial_season TEXT,
                    picks_status TEXT,
                    scenario_generator INTEGER,
                    contest_group_id TEXT,
                    alternate_start_deadline TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create league_standings table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS league_standings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_key TEXT,
                    team_key TEXT,
                    team_name TEXT,
                    rank INTEGER,
                    playoff_seed INTEGER,
                    wins INTEGER,
                    losses INTEGER,
                    ties INTEGER,
                    percentage REAL,
                    points_for REAL,
                    points_against REAL,
                    streak_type TEXT,
                    streak_value INTEGER,
                    season TEXT,
                    sport_code TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (league_key) REFERENCES leagues (league_key),
                    FOREIGN KEY (team_key) REFERENCES teams (team_key)
                )
            ''')

            # Create league_settings table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS league_settings (
                    league_key TEXT PRIMARY KEY,
                    settings_json TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create league_scoreboard table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS league_scoreboard (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_key TEXT,
                    week INTEGER,
                    scoreboard_json TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create league_rosters table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS league_rosters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_key TEXT,
                    team_key TEXT,
                    player_key TEXT,
                    selected_position TEXT,
                    is_starting INTEGER,
                    week INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (league_key) REFERENCES leagues (league_key),
                    FOREIGN KEY (team_key) REFERENCES teams (team_key),
                    FOREIGN KEY (player_key) REFERENCES players (player_key)
                )
            ''')

            # Create league_points table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS league_points (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_key TEXT,
                    team_key TEXT,
                    week INTEGER,
                    points REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (league_key) REFERENCES leagues (league_key),
                    FOREIGN KEY (team_key) REFERENCES teams (team_key)
                )
            ''')

            # Create league_games table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS league_games (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_key TEXT,
                    week INTEGER,
                    home_team_key TEXT,
                    away_team_key TEXT,
                    home_team_points REAL,
                    away_team_points REAL,
                    home_team_projected_points REAL,
                    away_team_projected_points REAL,
                    status TEXT,
                    game_start_time TIMESTAMP,
                    is_playoffs INTEGER,
                    is_consolation INTEGER,
                    is_tied INTEGER,
                    winner_team_key TEXT,
                    matchup_recap_title TEXT,
                    home_team_manager TEXT,
                    away_team_manager TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (league_key) REFERENCES leagues (league_key),
                    FOREIGN KEY (home_team_key) REFERENCES teams (team_key),
                    FOREIGN KEY (away_team_key) REFERENCES teams (team_key)
                )
            ''')

            # Create league_schedules table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS league_schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_key TEXT,
                    week INTEGER,
                    team_key TEXT,
                    opponent_team_key TEXT,
                    is_home INTEGER,
                    week_start DATE,
                    week_end DATE,
                    is_playoffs INTEGER,
                    is_consolation INTEGER,
                    is_tied INTEGER,
                    status TEXT,
                    points REAL,
                    projected_points REAL,
                    is_winner INTEGER,
                    manager_name TEXT,
                    opponent_manager_name TEXT,
                    matchup_recap_title TEXT,
                    division_id TEXT,
                    opponent_division_id TEXT,
                    clinched_playoffs INTEGER,
                    opponent_clinched_playoffs INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (league_key) REFERENCES leagues (league_key),
                    FOREIGN KEY (team_key) REFERENCES teams (team_key),
                    FOREIGN KEY (opponent_team_key) REFERENCES teams (team_key)
                )
            ''')

            # Create league_draft_results table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS league_draft_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_key TEXT,
                    season TEXT,
                    round INTEGER,
                    pick INTEGER,
                    team_key TEXT,
                    player_key TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (league_key) REFERENCES leagues (league_key),
                    FOREIGN KEY (team_key) REFERENCES teams (team_key),
                    FOREIGN KEY (player_key) REFERENCES players (player_key)
                )
            ''')

            # Create team_managers table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS team_managers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team_key TEXT NOT NULL,
                    manager_id TEXT,
                    nickname TEXT,
                    guid TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (team_key) REFERENCES teams (team_key)
                )
            ''')

            # Create team_stats table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS team_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team_key TEXT NOT NULL,
                    stat_key TEXT NOT NULL,
                    stat_value TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (team_key) REFERENCES teams (team_key)
                )
            ''')

            # Create sport_codes table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sport_codes (
                    game_code TEXT PRIMARY KEY,
                    sport TEXT NOT NULL,
                    season INTEGER NOT NULL,
                    season_type TEXT NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create player_stats table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_key TEXT NOT NULL,
                    league_key TEXT NOT NULL,
                    week INTEGER,
                    fantasy_points REAL,
                    projected_points REAL,
                    start_percentage REAL,
                    roster_percentage REAL,
                    bye_week INTEGER,
                    
                    -- Passing Stats
                    passing_yards INTEGER,
                    passing_touchdowns INTEGER,
                    passing_interceptions INTEGER,
                    passing_attempts INTEGER,
                    
                    -- Rushing Stats
                    rushing_yards INTEGER,
                    rushing_touchdowns INTEGER,
                    
                    -- Receiving Stats
                    receptions INTEGER,
                    receiving_yards INTEGER,
                    receiving_touchdowns INTEGER,
                    targets INTEGER,
                    
                    -- Misc Stats
                    misc_touchdowns INTEGER,
                    two_point_conversions INTEGER,
                    fumbles_lost INTEGER,
                    
                    -- Status
                    final_status TEXT,
                    opponent TEXT,
                    game_status TEXT,
                    
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_key) REFERENCES players (player_key),
                    FOREIGN KEY (league_key) REFERENCES leagues (league_key)
                )
            ''')

            self.conn.commit()
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise

    def _init_sport_codes(self):
        """Initialize sport codes table with default values"""
        try:
            # Create sport_codes table if it doesn't exist
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sport_codes (
                    game_code TEXT PRIMARY KEY,
                    sport TEXT NOT NULL,
                    season INTEGER NOT NULL,
                    season_type TEXT NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Insert default NFL sport codes if they don't exist
            default_codes = [
                ('449', 'nfl', 2023, 'regular'),
                ('423', 'nfl', 2022, 'regular'),
                ('406', 'nfl', 2021, 'regular'),
                ('399', 'nfl', 2020, 'regular'),
                ('390', 'nfl', 2019, 'regular')
            ]
            
            self.cursor.executemany('''
                INSERT OR IGNORE INTO sport_codes (game_code, sport, season, season_type)
                VALUES (?, ?, ?, ?)
            ''', default_codes)
            
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error initializing sport codes: {str(e)}")
            # Continue execution even if initialization fails
            pass

    def save_player(self, player_data: Dict[str, Any]):
        """Save player data to database"""
        try:
            stats = json.dumps(player_data.get('stats', {}))
            self.cursor.execute('''
                INSERT OR REPLACE INTO players 
                (player_key, sport_code, name, team, position, status, injury_note, headshot_url, stats, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                player_data['player_key'],
                player_data['sport_code'],
                player_data.get('name'),
                player_data.get('team'),
                player_data.get('position'),
                player_data.get('status'),
                player_data.get('injury_note'),
                player_data.get('headshot_url'),
                stats
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving player: {str(e)}")
            self.conn.rollback()

    def save_team(self, team_data: Dict[str, Any]):
        """Save team data to database"""
        try:
            # Begin transaction
            self.conn.execute("BEGIN TRANSACTION")
            
            # Save main team data with stats
            stats = json.dumps(team_data.get('stats', {}))
            self.cursor.execute('''
                INSERT OR REPLACE INTO teams 
                (team_key, sport_code, name, logo_url, stats, last_updated)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                team_data['team_key'],
                team_data['sport_code'],
                team_data.get('name'),
                team_data.get('logo_url'),
                stats
            ))
            
            # Save manager data if present
            if 'stats' in team_data and 'manager' in team_data['stats']:
                manager = team_data['stats']['manager']
                self.cursor.execute('''
                    INSERT OR REPLACE INTO team_managers 
                    (team_key, manager_id, nickname, guid, last_updated)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    team_data['team_key'],
                    manager.get('manager_id'),
                    manager.get('nickname'),
                    manager.get('guid')
                ))
            
            # Save other stats if present
            if 'stats' in team_data:
                for stat_key, stat_value in team_data['stats'].items():
                    if stat_key != 'manager':  # Skip manager as it's handled separately
                        if isinstance(stat_value, (dict, list)):
                            stat_value = json.dumps(stat_value)
                        self.cursor.execute('''
                            INSERT OR REPLACE INTO team_stats 
                            (team_key, stat_key, stat_value, last_updated)
                            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                        ''', (
                            team_data['team_key'],
                            stat_key,
                            str(stat_value)
                        ))
            
            # Commit transaction
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error saving team: {str(e)}")
            self.conn.rollback()
            raise

    def save_league(self, league_data: Dict[str, Any]):
        """Save league data to database"""
        try:
            settings = json.dumps(league_data.get('settings', {}))
            standings = json.dumps(league_data.get('standings', {}))
            self.cursor.execute('''
                INSERT OR REPLACE INTO leagues 
                (league_key, sport_code, name, season, settings, standings, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                league_data['league_key'],
                league_data['sport_code'],
                league_data.get('name'),
                league_data.get('season'),
                settings,
                standings
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league: {str(e)}")
            self.conn.rollback()

    def save_game(self, game_data: Dict[str, Any]):
        """Save game data to database"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO games 
                (game_key, game_id, sport_code, name, season, game_code, game_type, url,
                is_registration_over, is_game_over, is_offseason, editorial_season,
                picks_status, scenario_generator, contest_group_id, alternate_start_deadline,
                last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                game_data['game_key'],
                game_data.get('game_id'),
                game_data['sport_code'],
                game_data.get('name'),
                game_data.get('season'),
                game_data.get('game_code'),
                game_data.get('game_type'),
                game_data.get('url'),
                game_data.get('is_registration_over'),
                game_data.get('is_game_over'),
                game_data.get('is_offseason'),
                game_data.get('editorial_season'),
                game_data.get('picks_status'),
                game_data.get('scenario_generator'),
                game_data.get('contest_group_id'),
                game_data.get('alternate_start_deadline')
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving game: {str(e)}")
            self.conn.rollback()

    def save_league_standings(self, standings_data: Dict[str, Any]):
        """Save league standings data to database"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO league_standings 
                (league_key, team_key, team_name, rank, playoff_seed, wins, losses, ties,
                percentage, points_for, points_against, streak_type, streak_value,
                season, sport_code, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                standings_data['league_key'],
                standings_data['team_key'],
                standings_data['team_name'],
                standings_data.get('rank'),
                standings_data.get('playoff_seed'),
                standings_data.get('wins'),
                standings_data.get('losses'),
                standings_data.get('ties'),
                standings_data.get('percentage'),
                standings_data.get('points_for'),
                standings_data.get('points_against'),
                standings_data.get('streak_type'),
                standings_data.get('streak_value'),
                standings_data.get('season'),
                standings_data.get('sport_code')
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league standings: {str(e)}")
            self.conn.rollback()

    def save_league_settings(self, settings_data: Dict[str, Any]):
        """Save league settings data to database"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO league_settings 
                (league_key, settings_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (
                settings_data['league_key'],
                json.dumps(settings_data.get('settings', {}))
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league settings: {str(e)}")
            self.conn.rollback()

    def save_league_scoreboard(self, scoreboard_data: Dict[str, Any]):
        """Save league scoreboard data to database"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO league_scoreboard 
                (league_key, week, scoreboard_json, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                scoreboard_data['league_key'],
                scoreboard_data.get('week'),
                json.dumps(scoreboard_data.get('scoreboard', {}))
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league scoreboard: {str(e)}")
            self.conn.rollback()

    def get_all_players(self, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get all players for a specific sport"""
        try:
            self.cursor.execute('''
                SELECT * FROM players 
                WHERE sport_code = ?
                ORDER BY name
            ''', (sport_code,))
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting players: {str(e)}")
            return []

    def get_league_stats(self, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get league statistics for a specific sport"""
        try:
            self.cursor.execute('''
                SELECT l.*, COUNT(DISTINCT t.team_key) as team_count, COUNT(DISTINCT p.player_key) as player_count
                FROM leagues l
                LEFT JOIN teams t ON t.sport_code = l.sport_code
                LEFT JOIN players p ON p.sport_code = l.sport_code
                WHERE l.sport_code = ?
                GROUP BY l.league_key
            ''', (sport_code,))
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting league stats: {str(e)}")
            return []

    def get_player_stats(self, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get player statistics for a specific sport"""
        try:
            self.cursor.execute('''
                SELECT * FROM players 
                WHERE sport_code = ? 
                ORDER BY last_updated DESC
            ''', (sport_code,))
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting player stats: {str(e)}")
            return []

    def get_leaderboard(self, sport_code: str = 'nfl', season: int = None, week: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get leaderboard for a specific sport, season and week"""
        try:
            query = '''
                SELECT p.*, t.name as team_name
                FROM players p
                LEFT JOIN teams t ON t.team_key = p.team
                WHERE p.sport_code = ?
            '''
            params = [sport_code]
            
            if season:
                query += ' AND p.stats LIKE ?'
                params.append(f'%"season":{season}%')
            
            if week:
                query += ' AND p.stats LIKE ?'
                params.append(f'%"week":{week}%')
                
            query += ' ORDER BY p.stats->>"$.fantasy_points" DESC LIMIT 100'
            
            self.cursor.execute(query, params)
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting leaderboard: {str(e)}")
            return []

    def get_player_performance(self, player_name: str, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get performance data for a specific player in a specific sport"""
        try:
            self.cursor.execute('''
                SELECT p.*, t.name as team_name
                FROM players p
                LEFT JOIN teams t ON t.team_key = p.team
                WHERE p.name LIKE ? AND p.sport_code = ?
                ORDER BY p.last_updated DESC
            ''', (f'%{player_name}%', sport_code))
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting player performance: {str(e)}")
            return []

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_all_games(self) -> List[Dict[str, Any]]:
        """Get all games from the database"""
        conn = self.conn
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM games ORDER BY created_at DESC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get_all_leagues(self) -> List[Dict[str, Any]]:
        """Get all leagues from the database"""
        conn = self.conn
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM leagues ORDER BY created_at DESC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get_all_teams(self) -> List[Dict[str, Any]]:
        """Get all teams from the database"""
        conn = self.conn
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM teams ORDER BY created_at DESC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get_last_update_times(self) -> Dict[str, datetime]:
        """Get the last update time for each table"""
        conn = self.conn
        cursor = conn.cursor()
        
        tables = ['games', 'leagues', 'teams', 'players']
        update_times = {}
        
        for table in tables:
            cursor.execute(f"SELECT MAX(updated_at) as last_update FROM {table}")
            result = cursor.fetchone()
            if result and result['last_update']:
                update_times[table] = datetime.strptime(result['last_update'], '%Y-%m-%d %H:%M:%S')
        
        return update_times

    def test_connection(self) -> Dict[str, Any]:
        """Test database connection and return status"""
        try:
            conn = self.conn
            cursor = conn.cursor()
            
            # Check tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in cursor.fetchall()]
            
            # Get row counts
            counts = {}
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
            
            return {
                "status": "connected",
                "database_path": self.db_path,
                "tables": tables,
                "row_counts": counts
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "database_path": self.db_path
            }

    def get_database_size(self) -> str:
        """Get the current size of the database file"""
        try:
            size_bytes = os.path.getsize(self.db_path)
            if size_bytes < 1024:
                return f"{size_bytes} bytes"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes/1024:.2f} KB"
            else:
                return f"{size_bytes/(1024*1024):.2f} MB"
        except Exception as e:
            logger.error(f"Error getting database size: {str(e)}")
            return "Unknown"

    def query_players(self, filters: Dict[str, Any] = None, order_by: str = None, limit: int = None) -> List[Dict[str, Any]]:
        """Query players with optional filters, ordering, and limit"""
        conn = self.conn
        cursor = conn.cursor()
        
        query = "SELECT * FROM players"
        params = []
        
        if filters:
            conditions = []
            for key, value in filters.items():
                if value is not None:
                    conditions.append(f"{key} = ?")
                    params.append(value)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        if order_by:
            query += f" ORDER BY {order_by}"
            
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def query_teams(self, filters: Dict[str, Any] = None, order_by: str = None, limit: int = None) -> List[Dict[str, Any]]:
        """Query teams with optional filters, ordering, and limit"""
        conn = self.conn
        cursor = conn.cursor()
        
        query = "SELECT * FROM teams"
        params = []
        
        if filters:
            conditions = []
            for key, value in filters.items():
                if value is not None:
                    conditions.append(f"{key} = ?")
                    params.append(value)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        if order_by:
            query += f" ORDER BY {order_by}"
            
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def search_players(self, search_term: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search players by name or team"""
        conn = self.conn
        cursor = conn.cursor()
        
        query = """
            SELECT * FROM players 
            WHERE name LIKE ? 
            OR editorial_team_full_name LIKE ? 
            OR display_position LIKE ?
            LIMIT ?
        """
        search_pattern = f"%{search_term}%"
        cursor.execute(query, (search_pattern, search_pattern, search_pattern, limit))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get_team_players(self, team_key: str) -> List[Dict[str, Any]]:
        """Get all players for a specific team"""
        conn = self.conn
        cursor = conn.cursor()
        
        query = "SELECT * FROM players WHERE editorial_team_key = ?"
        cursor.execute(query, (team_key,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get_players_by_position(self, position: str, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get players for a specific position"""
        try:
            self.cursor.execute('''
                SELECT player_key, name, team, position, status, stats
                FROM players 
                WHERE sport_code = ? AND position = ?
                ORDER BY name
            ''', (sport_code, position))
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting players by position: {str(e)}")
            return []

    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table.
        
        Args:
            table_name: Name of the table to count rows from
            
        Returns:
            Number of rows in the table
        """
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return self.cursor.fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"Error getting row count for table {table_name}: {str(e)}")
            return 0

    def get_players_by_status(self, status: str, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get players with a specific status"""
        try:
            self.cursor.execute('''
                SELECT player_key, name, team, position, status, injury_note 
                FROM players 
                WHERE sport_code = ? AND status = ?
                ORDER BY name
            ''', (sport_code, status))
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting players by status: {str(e)}")
            return []

    def search_players_by_name(self, search_term: str, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Search players by name"""
        try:
            search_pattern = f"%{search_term}%"
            self.cursor.execute('''
                SELECT player_key, name, team, position, status, injury_note
                FROM players 
                WHERE sport_code = ? AND name LIKE ?
                ORDER BY name
            ''', (sport_code, search_pattern))
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error searching players: {str(e)}")
            return []

    def get_top_players(self, sport_code: str = 'nfl', limit: int = 10) -> List[Dict[str, Any]]:
        """Get top players based on stats"""
        try:
            self.cursor.execute('''
                SELECT player_key, name, team, position, status, stats
                FROM players 
                WHERE sport_code = ? 
                    AND json_extract(stats, '$.fantasy_points') IS NOT NULL
                ORDER BY CAST(json_extract(stats, '$.fantasy_points') AS FLOAT) DESC
                LIMIT ?
            ''', (sport_code, limit))
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting top players: {str(e)}")
            return []

    def save_league_roster(self, roster_data: Dict[str, Any]):
        """Save league roster data to database"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO league_rosters 
                (league_key, team_key, player_key, selected_position, is_starting, week, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                roster_data['league_key'],
                roster_data['team_key'],
                roster_data['player_key'],
                roster_data.get('selected_position'),
                roster_data.get('is_starting', 0),
                roster_data.get('week')
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league roster: {str(e)}")
            self.conn.rollback()

    def save_league_points(self, points_data: Dict):
        """Save league points data to the database."""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO league_points 
                (league_key, team_key, week, points, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                points_data['league_key'], 
                points_data['team_key'], 
                points_data['week'], 
                points_data['points']
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league points: {str(e)}")
            self.conn.rollback()

    def save_league_game(self, game_data: Dict[str, Any]):
        """Save league game data to database"""
        try:
            # Check if game already exists
            self.cursor.execute('''
                SELECT id FROM league_games 
                WHERE league_key = ? 
                AND week = ? 
                AND home_team_key = ? 
                AND away_team_key = ?
            ''', (
                game_data['league_key'],
                game_data.get('week'),
                game_data.get('home_team_key'),
                game_data.get('away_team_key')
            ))
            existing_game = self.cursor.fetchone()
            
            if existing_game:
                # Update existing game
                self.cursor.execute('''
                    UPDATE league_games 
                    SET home_team_points = ?,
                        away_team_points = ?,
                        home_team_projected_points = ?,
                        away_team_projected_points = ?,
                        status = ?,
                        game_start_time = ?,
                        is_playoffs = ?,
                        is_consolation = ?,
                        is_tied = ?,
                        winner_team_key = ?,
                        matchup_recap_title = ?,
                        home_team_manager = ?,
                        away_team_manager = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE league_key = ? 
                    AND week = ? 
                    AND home_team_key = ? 
                    AND away_team_key = ?
                ''', (
                    game_data.get('home_team_points'),
                    game_data.get('away_team_points'),
                    game_data.get('home_team_projected_points'),
                    game_data.get('away_team_projected_points'),
                    game_data.get('status'),
                    game_data.get('game_start_time'),
                    game_data.get('is_playoffs'),
                    game_data.get('is_consolation'),
                    game_data.get('is_tied'),
                    game_data.get('winner_team_key'),
                    game_data.get('matchup_recap_title'),
                    game_data.get('home_team_manager'),
                    game_data.get('away_team_manager'),
                    game_data['league_key'],
                    game_data.get('week'),
                    game_data.get('home_team_key'),
                    game_data.get('away_team_key')
                ))
            else:
                # Insert new game
                self.cursor.execute('''
                    INSERT INTO league_games 
                    (league_key, week, home_team_key, away_team_key, 
                    home_team_points, away_team_points, home_team_projected_points,
                    away_team_projected_points, status, game_start_time, is_playoffs,
                    is_consolation, is_tied, winner_team_key, matchup_recap_title,
                    home_team_manager, away_team_manager, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    game_data['league_key'],
                    game_data.get('week'),
                    game_data.get('home_team_key'),
                    game_data.get('away_team_key'),
                    game_data.get('home_team_points'),
                    game_data.get('away_team_points'),
                    game_data.get('home_team_projected_points'),
                    game_data.get('away_team_projected_points'),
                    game_data.get('status'),
                    game_data.get('game_start_time'),
                    game_data.get('is_playoffs'),
                    game_data.get('is_consolation'),
                    game_data.get('is_tied'),
                    game_data.get('winner_team_key'),
                    game_data.get('matchup_recap_title'),
                    game_data.get('home_team_manager'),
                    game_data.get('away_team_manager')
                ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league game: {str(e)}")
            self.conn.rollback()

    def save_league_schedule(self, schedule_data: Dict[str, Any]) -> None:
        """Save league schedule data to database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO league_schedules (
                    league_key, week, team_key, opponent_team_key, is_home,
                    week_start, week_end, is_playoffs, is_consolation, is_tied,
                    status, points, projected_points, is_winner, manager_name,
                    opponent_manager_name, matchup_recap_title, division_id,
                    opponent_division_id, clinched_playoffs, opponent_clinched_playoffs,
                    updated_at
                ) VALUES (
                    :league_key, :week, :team_key, :opponent_team_key, :is_home,
                    :week_start, :week_end, :is_playoffs, :is_consolation, :is_tied,
                    :status, :points, :projected_points, :is_winner, :manager_name,
                    :opponent_manager_name, :matchup_recap_title, :division_id,
                    :opponent_division_id, :clinched_playoffs, :opponent_clinched_playoffs,
                    CURRENT_TIMESTAMP
                )
            """, schedule_data)
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league schedule: {str(e)}")
            self.conn.rollback()

    def save_league_draft_results(self, draft_results_data: List[Dict]):
        """Save league draft results data to the database."""
        try:
            for draft_result in draft_results_data:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO league_draft_results 
                    (league_key, season, round, pick, team_key, player_key, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    draft_result['league_key'], 
                    draft_result['season'], 
                    draft_result['round'], 
                    draft_result['pick'], 
                    draft_result['team_key'], 
                    draft_result['player_key']
                ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving league draft results: {str(e)}")
            self.conn.rollback()

    def save_sport_code(self, code_data: Dict[str, Any]):
        """Save sport code mapping to database
        
        Args:
            code_data (Dict[str, Any]): Dictionary containing:
                - game_code: Yahoo's game code
                - sport: Sport identifier (e.g., 'nfl', 'mlb')
                - season: Season year
                - season_type: Type of season (e.g., 'regular', 'playoff')
                - start_date: Optional season start date
                - end_date: Optional season end date
        """
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO sport_codes 
                (game_code, sport, season, season_type, start_date, end_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                code_data['game_code'],
                code_data['sport'],
                code_data['season'],
                code_data['season_type'],
                code_data.get('start_date'),
                code_data.get('end_date')
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving sport code: {str(e)}")
            self.conn.rollback()

    def get_sport_codes(self, sport: str = None, season: int = None) -> List[Dict[str, Any]]:
        """Get sport codes from database with optional filters
        
        Args:
            sport (str, optional): Filter by sport (e.g., 'nfl')
            season (int, optional): Filter by season year
            
        Returns:
            List[Dict[str, Any]]: List of sport code mappings
        """
        try:
            query = "SELECT * FROM sport_codes WHERE 1=1"
            params = []
            
            if sport:
                query += " AND sport = ?"
                params.append(sport)
            if season:
                query += " AND season = ?"
                params.append(season)
                
            query += " ORDER BY season DESC, sport"
            
            self.cursor.execute(query, params)
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting sport codes: {str(e)}")
            return []

    def get_player_stats_by_league(self, league_key: str, player_keys: List[str]) -> Dict:
        """Get detailed stats for specific players in a league
        
        Args:
            league_key (str): The league key
            player_keys (List[str]): List of player keys to get stats for
            
        Returns:
            Dictionary containing player stats
        """
        try:
            player_keys_str = ','.join(player_keys)
            endpoint = f"/league/{league_key}/players;player_keys={player_keys_str}/stats"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting player stats: {str(e)}")
            raise

    def save_player_stats(self, stats_data: Dict[str, Any]):
        """Save player statistics to database"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO player_stats (
                    player_key, league_key, week,
                    fantasy_points, projected_points,
                    start_percentage, roster_percentage, bye_week,
                    passing_yards, passing_touchdowns, passing_interceptions, passing_attempts,
                    rushing_yards, rushing_touchdowns,
                    receptions, receiving_yards, receiving_touchdowns, targets,
                    misc_touchdowns, two_point_conversions, fumbles_lost,
                    final_status, opponent, game_status,
                    updated_at
                ) VALUES (
                    :player_key, :league_key, :week,
                    :fantasy_points, :projected_points,
                    :start_percentage, :roster_percentage, :bye_week,
                    :passing_yards, :passing_touchdowns, :passing_interceptions, :passing_attempts,
                    :rushing_yards, :rushing_touchdowns,
                    :receptions, :receiving_yards, :receiving_touchdowns, :targets,
                    :misc_touchdowns, :two_point_conversions, :fumbles_lost,
                    :final_status, :opponent, :game_status,
                    CURRENT_TIMESTAMP
                )
            ''', stats_data)
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving player stats: {str(e)}")
            self.conn.rollback()
