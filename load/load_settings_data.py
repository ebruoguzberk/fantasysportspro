import logging
import time
import random
from datetime import datetime
from yahoo_api import YahooFantasyAPI
from dotenv import load_dotenv
from database import Database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize database
db = Database()

# Constants for retry logic
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 30  # seconds
MAX_RETRY_DELAY = 300  # seconds

def load_settings_data():
    """Load league settings data from Yahoo Fantasy API into database"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.access_token:
            logger.error("No access token available")
            return
            
        logger.info("Starting league settings load...")
        
        # Get all leagues first
        leagues = yahoo_api.get_leagues(sport_code='nfl')
        total_leagues = len(leagues)
        logger.info(f"Found {total_leagues} leagues to process")
        
        processed_settings = 0
        retry_count = 0
        current_delay = INITIAL_RETRY_DELAY
        
        for league in leagues:
            league_key = league.get('league_key')
            if not league_key:
                continue
                
            logger.info(f"Processing settings for league: {league.get('name', league_key)}")
            
            try:
                # Get league settings
                league_data = yahoo_api.get_league(league_key)
                if league_data and 'fantasy_content' in league_data:
                    league_info = league_data['fantasy_content'].get('league', [])
                    if len(league_info) > 1 and isinstance(league_info[1], dict):
                        settings = league_info[1].get('settings', [])
                        if settings and isinstance(settings[0], dict):
                            settings_data = {
                                'league_key': league_key,
                                'draft_type': settings[0].get('draft_type'),
                                'scoring_type': settings[0].get('scoring_type'),
                                'uses_playoff': settings[0].get('uses_playoff'),
                                'playoff_start_week': settings[0].get('playoff_start_week'),
                                'uses_playoff_reseeding': settings[0].get('uses_playoff_reseeding'),
                                'uses_lock_eliminated_teams': settings[0].get('uses_lock_eliminated_teams'),
                                'num_playoff_teams': settings[0].get('num_playoff_teams'),
                                'num_playoff_consolation_teams': settings[0].get('num_playoff_consolation_teams'),
                                'has_multiweek_championship': settings[0].get('has_multiweek_championship'),
                                'waiver_type': settings[0].get('waiver_type'),
                                'waiver_rule': settings[0].get('waiver_rule'),
                                'uses_faab': settings[0].get('uses_faab'),
                                'draft_time': settings[0].get('draft_time'),
                                'draft_pick_time': settings[0].get('draft_pick_time'),
                                'post_draft_players': settings[0].get('post_draft_players'),
                                'max_teams': settings[0].get('max_teams'),
                                'waiver_time': settings[0].get('waiver_time'),
                                'trade_end_date': settings[0].get('trade_end_date'),
                                'trade_ratify_type': settings[0].get('trade_ratify_type'),
                                'trade_reject_time': settings[0].get('trade_reject_time'),
                                'roster_positions': str(settings[0].get('roster_positions', [])),
                                'stat_categories': str(settings[0].get('stat_categories', [])),
                                'stat_modifiers': str(settings[0].get('stat_modifiers', []))
                            }
                            
                            try:
                                db.save_league_settings(settings_data)
                                processed_settings += 1
                                logger.info(f"Saved settings for league: {league.get('name')}")
                            except Exception as e:
                                logger.error(f"Error saving league settings: {str(e)}")
                
                # Reset retry count on successful processing
                retry_count = 0
                # Only reset delay if we haven't hit rate limits recently
                if current_delay > INITIAL_RETRY_DELAY:
                    current_delay = max(current_delay / 2, INITIAL_RETRY_DELAY)
                    
            except Exception as e:
                retry_count += 1
                if retry_count > MAX_RETRIES:
                    logger.error(f"Maximum retries ({MAX_RETRIES}) exceeded for league {league_key}")
                    continue
                    
                # Exponential backoff with jitter
                current_delay = min(current_delay * 2, MAX_RETRY_DELAY)
                actual_delay = current_delay + (current_delay * 0.1 * random.random())
                logger.warning(f"Error processing league {league_key}: {str(e)}")
                logger.info(f"Retry attempt {retry_count} of {MAX_RETRIES}. Waiting {actual_delay:.1f} seconds...")
                time.sleep(actual_delay)
                continue
        
        logger.info(f"League settings load complete:")
        logger.info(f"Processed {processed_settings} league settings")
        
        # Verify data in database
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM league_settings")
        settings_count = cursor.fetchone()[0]
        
        logger.info(f"Final database counts:")
        logger.info(f"League settings in database: {settings_count}")
        
    except Exception as e:
        logger.error(f"Error in load_settings_data: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    try:
        load_settings_data()
    except Exception as e:
        logger.error(f"Script error: {str(e)}") 