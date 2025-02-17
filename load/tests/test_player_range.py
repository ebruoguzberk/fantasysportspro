import logging
import time
from yahoo_api import YahooFantasyAPI
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_player_range():
    """Test different starting positions for game key 49"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.access_token:
            logger.error("No access token available")
            return

        game_key = '49'
        # Test a range of starting positions around and after 1425
        start_positions = [1400, 1425, 1450, 1500, 1600]
        count = 25  # Smaller batch size for testing

        for start in start_positions:
            logger.info(f"\nTesting position {start}")
            endpoint = f'/game/{game_key}/players;start={start};count={count}'
            
            try:
                response = yahoo_api.make_request(endpoint)
                
                if response and 'fantasy_content' in response:
                    game_data = response['fantasy_content'].get('game', [{}])
                    if len(game_data) > 1:
                        players = game_data[1].get('players', {})
                        batch_count = int(players.get('count', 0))
                        
                        logger.info(f"Found {batch_count} players at position {start}")
                        
                        # If we found players, print the first one as example
                        if batch_count > 0:
                            first_player = players.get('0', {}).get('player', [])
                            if first_player:
                                player_info = first_player[0]
                                name = next((item.get('name', {}).get('full') 
                                           for item in player_info 
                                           if isinstance(item, dict) and 'name' in item), 'Unknown')
                                logger.info(f"First player in batch: {name}")
                
            except Exception as e:
                logger.error(f"Error at position {start}: {str(e)}")
            
            # Wait between requests
            time.sleep(2.1)

    except Exception as e:
        logger.error(f"Test error: {str(e)}")

if __name__ == "__main__":
    test_player_range() 