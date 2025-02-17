import logging
import os
from dotenv import load_dotenv
from yahoo_api import YahooFantasyAPI
from database import Database
from data_manager import DataManager
from datetime import datetime

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_leagues_data():
    """Load leagues data from Yahoo Fantasy API into the database"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.access_token:
            logger.info("No access token available, starting authorization flow...")
            auth_url = yahoo_api.get_authorization_url()
            logger.info(f"Please visit this URL to authorize the application: {auth_url}")
            auth_code = input("Enter the authorization code: ")
            if yahoo_api.handle_authorization(auth_code):
                logger.info("Successfully authorized and obtained tokens")
            else:
                logger.error("Failed to authorize")
                return
        logger.info("Using access token")

        # Initialize database with the correct path
        db = Database(db_path='fantasy_data.db')

        # Initialize DataManager with Yahoo API and database
        data_manager = DataManager(yahoo_api=yahoo_api, use_db=True, db_path='fantasy_data.db')

        logger.info("Starting leagues data fetch...")
        
        # Get leagues from Yahoo API
        response = yahoo_api.make_request('/users;use_login=1/games/leagues')
        logger.debug(f"Raw API response: {response}")
        
        if not response or 'fantasy_content' not in response:
            logger.error("No fantasy content in response")
            return
            
        users = response['fantasy_content'].get('users', {})
        if '0' not in users:
            logger.error("No user data found")
            return
            
        user_data = users['0'].get('user', [])
        if len(user_data) < 2:
            logger.error("Incomplete user data")
            return
            
        games = user_data[1].get('games', {})
        games_count = int(games.get('count', 0))
        logger.info(f"Found {games_count} games with leagues")
        
        # Process leagues from each game
        processed_leagues = 0
        total_leagues = 0
        
        for i in range(games_count):
            idx = str(i)
            if idx not in games:
                logger.debug(f"Skipping game index {idx} - not found in games")
                continue
                
            game_data = games[idx].get('game', [])
            if not game_data:
                logger.debug(f"Skipping game index {idx} - no game data")
                continue
                
            logger.debug(f"Game data for index {idx}: {game_data}")
            
            # Handle both list and dict formats for game data
            if isinstance(game_data, list):
                if len(game_data) == 0:
                    logger.debug(f"Skipping game index {idx} - empty game data list")
                    continue
                game = game_data[0]
                leagues_container = game_data[1].get('leagues', {}) if len(game_data) > 1 else {}
            else:
                game = game_data
                leagues_container = {}
            
            if not leagues_container:
                logger.debug(f"No leagues found for game in index {idx}")
                continue
                
            leagues_count = int(leagues_container.get('count', 0))
            total_leagues += leagues_count
            
            sport_code = game.get('code')
            season = game.get('season')
            
            logger.info(f"Processing {leagues_count} leagues for {sport_code} season {season}")
            
            # Process each league in the game
            for j in range(leagues_count):
                league_idx = str(j)
                if league_idx not in leagues_container:
                    logger.debug(f"Skipping league index {league_idx} - not found in leagues container")
                    continue
                    
                league_data = leagues_container[league_idx].get('league', [])
                if not league_data:
                    logger.debug(f"Skipping league index {league_idx} - no league data")
                    continue
                    
                if isinstance(league_data, list):
                    if len(league_data) == 0:
                        logger.debug(f"Skipping league index {league_idx} - empty league data list")
                        continue
                    league = league_data[0]
                else:
                    league = league_data
                
                # Prepare league data for database
                league_info = {
                    'league_key': league.get('league_key'),
                    'sport_code': sport_code,
                    'name': league.get('name'),
                    'season': season,
                    'settings': {
                        'num_teams': league.get('num_teams'),
                        'draft_status': league.get('draft_status'),
                        'scoring_type': league.get('scoring_type'),
                        'league_type': league.get('league_type'),
                        'is_pro_league': league.get('is_pro_league'),
                        'is_cash_league': league.get('is_cash_league'),
                        'current_week': league.get('current_week'),
                        'start_week': league.get('start_week'),
                        'end_week': league.get('end_week'),
                        'start_date': league.get('start_date'),
                        'end_date': league.get('end_date'),
                        'is_finished': league.get('is_finished'),
                        'is_plus_league': league.get('is_plus_league'),
                        'entry_fee': league.get('entry_fee'),
                        'weekly_deadline': league.get('weekly_deadline'),
                        'league_update_timestamp': league.get('league_update_timestamp'),
                        'allow_add_to_dl_extra_pos': league.get('allow_add_to_dl_extra_pos')
                    }
                }
                
                try:
                    db.save_league(league_info)
                    processed_leagues += 1
                    logger.info(f"Saved league: {league_info['name']} (Season: {league_info['season']}, Sport: {league_info['sport_code']})")
                except Exception as e:
                    logger.error(f"Failed to save league: {str(e)}")
                
        logger.info(f"Successfully processed {processed_leagues} out of {total_leagues} leagues")

        # Verify leagues in database
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM leagues")
        count = cursor.fetchone()[0]
        logger.info(f"Leagues in database: {count}")
        
        # Print league details
        cursor.execute("SELECT league_key, name, season, sport_code FROM leagues ORDER BY season DESC")
        leagues = cursor.fetchall()
        for league in leagues:
            logger.info(f"League: {league[1]} (Season: {league[2]}, Key: {league[0]}, Sport: {league[3]})")

    except Exception as e:
        logger.error(f"Error during leagues data load: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_leagues_data() 