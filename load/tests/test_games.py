import logging
from yahoo_api import YahooFantasyAPI
from dotenv import load_dotenv
from types import SimpleNamespace

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_games():
    """Test getting available games from Yahoo Fantasy API"""
    load_dotenv()
    session_state = SimpleNamespace()
    api = YahooFantasyAPI(session_state=session_state)
    
    try:
        # Get all games
        response = api.make_request('/users;use_login=1/games')
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
        
        # Process each game
        for i in range(games_count):
            idx = str(i)
            if idx not in games_data:
                continue
                
            game = games_data[idx].get('game', [])
            if not game:
                continue
                
            # Handle both list and dict formats
            game_info = game[0] if isinstance(game, list) else game
            
            logger.info(f"Game: {game_info.get('name')} (Key: {game_info.get('game_key')}, Code: {game_info.get('code')}, Season: {game_info.get('season')})")
            
    except Exception as e:
        logger.error(f"Error testing games: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        test_games()
    except Exception as e:
        logger.error(f"Script error: {str(e)}") 