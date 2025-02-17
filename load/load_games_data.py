import logging
import os
from dotenv import load_dotenv
from yahoo_api import YahooFantasyAPI
from database import Database
from data_manager import DataManager
from datetime import datetime

# Load environment variables
load_dotenv()

# Allow OAuth2 to work with HTTP for development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_games_data():
    """Load games data from Yahoo Fantasy API into the database"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.token:
            logger.info("No token available, starting authorization flow...")
            auth_url = yahoo_api.get_authorization_url()
            logger.info(f"Please visit this URL to authorize the application: {auth_url}")
            auth_code = input("Enter the authorization code: ")
            if yahoo_api.handle_authorization(auth_code):
                logger.info("Successfully authorized and obtained tokens")
            else:
                logger.error("Failed to authorize")
                return
        logger.info("Using existing token")

        # Initialize database with the correct path
        db = Database(db_path='fantasy_data.db')

        # Initialize DataManager with Yahoo API and database
        data_manager = DataManager(yahoo_api=yahoo_api, use_db=True, db_path='fantasy_data.db')

        logger.info("Starting games data fetch test...")
        
        # Get games from Yahoo API
        response = yahoo_api.make_request('/users;use_login=1/games?format=json')
        print("response: ", response)
        
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
            
        games_data = user_data[1].get('games', {})
        games_count = int(games_data.get('count', 0))
        logger.info(f"Found {games_count} games")
        
        # Process and save each game to database
        processed_games = 0
        for i in range(games_count):
            idx = str(i)
            if idx not in games_data:
                logger.warning(f"No game data found for index {i}")
                continue
                
            game_container = games_data[idx].get('game', [])
            if not game_container:
                logger.warning(f"No game container found for index {i}")
                continue
                
            # Handle both list and dict formats
            game = game_container[0] if isinstance(game_container, list) else game_container
            
            # Skip games without required data
            if not game or not all(game.get(field) for field in ['game_key', 'code', 'name', 'season']):
                logger.warning(f"Skipping game at index {i} due to missing required fields")
                continue
                
            game_data = {
                'game_key': game.get('game_key'),
                'game_id': game.get('game_id'),
                'sport_code': game.get('code'),
                'name': game.get('name'),
                'season': game.get('season'),
                'game_code': game.get('code'),
                'game_type': game.get('type'),
                'url': game.get('url'),
                'is_registration_over': game.get('is_registration_over'),
                'is_game_over': game.get('is_game_over'),
                'is_offseason': game.get('is_offseason'),
                'editorial_season': game.get('editorial_season'),
                'picks_status': game.get('picks_status'),
                'scenario_generator': game.get('scenario_generator'),
                'contest_group_id': game.get('contest_group_id'),
                'alternate_start_deadline': game.get('alternate_start_deadline')
            }
            
            try:
                db.save_game(game_data)
                processed_games += 1
                logger.info(f"Saved game: {game_data['name']} (Season: {game_data['season']}, Sport: {game_data['sport_code']})")
            except Exception as e:
                logger.error(f"Failed to save game: {str(e)}")
                
        logger.info(f"Successfully processed {processed_games} out of {games_count} games")

        # Verify games in database
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM games")
        count = cursor.fetchone()[0]
        logger.info(f"Games in database: {count}")
        
        # Print game details
        cursor.execute("SELECT game_key, name, season, sport_code FROM games ORDER BY season DESC")
        games = cursor.fetchall()
        for game in games:
            logger.info(f"Game: {game[1]} (Season: {game[2]}, Key: {game[0]}, Sport: {game[3]})")

    except Exception as e:
        logger.error(f"Error during leagues data test: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_games_data() 