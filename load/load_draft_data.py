import logging
from typing import Dict, List, Optional
from yahoo_api import YahooFantasyAPI
from database import Database
import sqlite3
import time
import json

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DraftDataLoader:
    def __init__(self, api: YahooFantasyAPI, db: Database):
        """
        Initialize the DraftDataLoader.
        
        Args:
            api (YahooFantasyAPI): Instance of the Yahoo Fantasy API client
            db (Database): Instance of the database connection
        """
        self.api = api
        self.db = db

    def load_draft_results(self, league_key: str, season: str) -> bool:
        """
        Load draft results for a specific league into the database.
        
        Args:
            league_key (str): The league key to load draft results for
            season (str): The season year for the draft results
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get draft results from the API
            draft_data = self.api.get_draft_results(league_key)
            
            if not draft_data or 'fantasy_content' not in draft_data:
                logger.warning(f"No draft data found for league {league_key}")
                return False
            
            # Log the entire draft_data response
            logger.debug("Full draft data response:")
            logger.debug(json.dumps(draft_data, indent=2))
            
            # Extract draft results from the response
            league_data = draft_data['fantasy_content'].get('league', [])
            if len(league_data) > 1 and isinstance(league_data[1], dict):
                draft_results = league_data[1].get('draft_results', {})
                
                if not isinstance(draft_results, dict):
                    logger.warning(f"Invalid draft results format for league {league_key}")
                    return False
                
                # Log the extracted draft_results
                logger.debug("Extracted draft results:")
                logger.debug(json.dumps(draft_results, indent=2))
                
                # Process and store draft results
                cursor = self.db.conn.cursor()
                
                for pick_num, pick in draft_results.items():
                    if isinstance(pick, dict) and 'draft_result' in pick:
                        result = pick['draft_result']
                        
                        cursor.execute('''
                            INSERT OR REPLACE INTO league_draft_results 
                            (league_key, season, round, pick, team_key, player_key)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            league_key,
                            season,
                            result.get('round'),
                            result.get('pick'),
                            result.get('team_key'),
                            result.get('player_key')
                        ))
                
                self.db.conn.commit()
                logger.info(f"Successfully loaded draft results for league {league_key}")
                return True
                
            logger.warning(f"No draft results data found in response for league {league_key}")
            return False
            
        except Exception as e:
            logger.error(f"Error loading draft results: {e}")
            return False

    def get_draft_results(self, league_key: str) -> List[Dict]:
        """
        Retrieve draft results for a specific league from the database.
        
        Args:
            league_key (str): The league key to get draft results for
            
        Returns:
            List[Dict]: List of draft results
        """
        try:
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT * FROM league_draft_results 
                WHERE league_key = ? 
                ORDER BY round, pick
            ''', (league_key,))
            
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return results
                
        except sqlite3.Error as e:
            logger.error(f"Error retrieving draft results: {e}")
            return []

def load_draft_data():
    """Load draft data from Yahoo Fantasy API into the database"""
    try:
        # Initialize Yahoo API directly since auth is already loaded
        yahoo_api = YahooFantasyAPI()
        
        # Initialize database
        db = Database(db_path='fantasy_data.db')
        
        # Get all league keys from database
        cursor = db.conn.cursor()
        cursor.execute("SELECT league_key, name FROM leagues")
        leagues = cursor.fetchall()
        
        if not leagues:
            logger.error("No leagues found in database")
            return
            
        logger.info(f"Found {len(leagues)} leagues to process")
        
        # Create draft loader
        draft_loader = DraftDataLoader(yahoo_api, db)
        
        # Process each league
        for league_key, league_name in leagues:
            logger.info(f"Processing draft data for league: {league_name} ({league_key})")
            
            # Extract season from league key (format: <game_key>.l.<league_id>)
            game_key = league_key.split('.')[0] if '.' in league_key else None
            
            # Convert game_key to season year
            season_map = {
                '49': '2011', '79': '2012', '101': '2013', '175': '2014', 
                '199': '2015', '238': '2016', '242': '2016', '257': '2017',
                '314': '2018', '328': '2018', '331': '2018', '348': '2019',
                '359': '2019', '371': '2020', '380': '2020', '390': '2021',
                '399': '2021', '406': '2022', '414': '2022', '423': '2023',
                '449': '2024'
            }
            season = season_map.get(game_key, None)
            
            if not season:
                logger.warning(f"Could not determine season for league {league_name} ({league_key})")
                continue
            
            # Load draft results
            success = draft_loader.load_draft_results(league_key, season)
            if success:
                logger.info(f"Successfully loaded draft results for league {league_name}")
            else:
                logger.warning(f"Failed to load draft results for league {league_name}")
            
            # Add a small delay to avoid rate limiting
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"Error during draft data load: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_draft_data()
