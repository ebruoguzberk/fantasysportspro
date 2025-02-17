import logging
from database import Database
import json

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_teams_in_db():
    """Check what teams are in the database"""
    try:
        db = Database(db_path='fantasy_data.db')
        cursor = db.conn.cursor()
        
        # Check teams table
        logger.info("\nChecking teams table:")
        cursor.execute("""
            SELECT t.team_key, t.name, l.name as league_name, t.sport_code
            FROM teams t
            LEFT JOIN leagues l ON SUBSTR(t.team_key, 1, INSTR(t.team_key, '.t.') - 1) = l.league_key
        """)
        teams = cursor.fetchall()
        if teams:
            for team in teams:
                logger.info(f"Team Key: {team[0]}, Name: {team[1]}, League: {team[2]}, Sport: {team[3]}")
        else:
            logger.warning("No teams found in database")
            
        # Check league_draft_results table
        logger.info("\nChecking league_draft_results table:")
        cursor.execute("""
            SELECT DISTINCT league_key, team_key
            FROM league_draft_results
            ORDER BY league_key, team_key
        """)
        draft_teams = cursor.fetchall()
        if draft_teams:
            for team in draft_teams:
                logger.info(f"League Key: {team[0]}, Team Key: {team[1]}")
        else:
            logger.warning("No draft results found in database")
            
        # Check if specific team exists
        team_key = "101.l.166056.t.1"
        logger.info(f"\nChecking for specific team {team_key}:")
        
        cursor.execute("SELECT * FROM teams WHERE team_key = ?", (team_key,))
        team = cursor.fetchone()
        if team:
            logger.info(f"Team found in teams table: {team}")
        else:
            logger.warning("Team not found in teams table")
            
        cursor.execute("SELECT * FROM league_draft_results WHERE team_key = ?", (team_key,))
        draft = cursor.fetchone()
        if draft:
            logger.info(f"Team found in draft results: {draft}")
        else:
            logger.warning("Team not found in draft results")
            
    except Exception as e:
        logger.error(f"Error checking teams: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    check_teams_in_db() 