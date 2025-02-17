import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def migrate_schedules():
    """Migrate league_games and league_schedules tables to add new fields"""
    try:
        # Connect to database
        conn = sqlite3.connect('fantasy_data.db')
        cursor = conn.cursor()
        
        # Begin transaction
        cursor.execute("BEGIN TRANSACTION")
        
        # Backup existing tables
        logger.info("Creating backup of existing tables...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS league_games_backup AS
            SELECT * FROM league_games
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS league_schedules_backup AS
            SELECT * FROM league_schedules
        """)
        
        # Drop existing tables
        logger.info("Dropping existing tables...")
        cursor.execute("DROP TABLE league_games")
        cursor.execute("DROP TABLE league_schedules")
        
        # Create new league_games table with additional fields
        logger.info("Creating new league_games table...")
        cursor.execute("""
            CREATE TABLE league_games (
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
                FOREIGN KEY (away_team_key) REFERENCES teams (team_key),
                FOREIGN KEY (winner_team_key) REFERENCES teams (team_key)
            )
        """)
        
        # Create new league_schedules table with additional fields
        logger.info("Creating new league_schedules table...")
        cursor.execute("""
            CREATE TABLE league_schedules (
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
                division_id INTEGER,
                opponent_division_id INTEGER,
                clinched_playoffs INTEGER,
                opponent_clinched_playoffs INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (league_key) REFERENCES leagues (league_key),
                FOREIGN KEY (team_key) REFERENCES teams (team_key),
                FOREIGN KEY (opponent_team_key) REFERENCES teams (team_key)
            )
        """)
        
        # Copy data from backup tables to new tables
        logger.info("Copying data from backup tables...")
        cursor.execute("""
            INSERT INTO league_games (
                id, league_key, week, home_team_key, away_team_key,
                home_team_points, away_team_points, status, game_start_time,
                updated_at
            )
            SELECT 
                id, league_key, week, home_team_key, away_team_key,
                home_team_points, away_team_points, status, game_start_time,
                updated_at
            FROM league_games_backup
        """)
        
        cursor.execute("""
            INSERT INTO league_schedules (
                id, league_key, week, team_key, opponent_team_key,
                is_home, week_start, week_end, is_playoffs, is_consolation,
                status, points, projected_points, is_winner, manager_name,
                updated_at
            )
            SELECT 
                id, league_key, week, team_key, opponent_team_key,
                is_home, week_start, week_end, is_playoffs, is_consolation,
                status, points, projected_points, is_winner, manager_name,
                updated_at
            FROM league_schedules_backup
        """)
        
        # Drop backup tables
        logger.info("Dropping backup tables...")
        cursor.execute("DROP TABLE league_games_backup")
        cursor.execute("DROP TABLE league_schedules_backup")
        
        # Commit transaction
        cursor.execute("COMMIT")
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Error during migration: {str(e)}")
        cursor.execute("ROLLBACK")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    migrate_schedules() 