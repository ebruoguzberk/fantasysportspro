import logging
import os
from yahoo_api import YahooFantasyAPI
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Allow OAuth2 over HTTP for development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

def test_player_api():
    """Test different API endpoint formats for fetching players"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.token:
            logger.error("No token available")
            return

        # Test data - a few player keys from game 238
        test_player_keys = [
            "238.p.4975",
            "238.p.5132",
            "238.p.5164"
        ]
        
        # Try different endpoint formats
        endpoints = [
            # Format 1: Direct players endpoint
            f'{yahoo_api.BASE_URL}/players;player_keys={",".join(test_player_keys)}?format=json',
            
            # Format 2: With game/nfl prefix
            f'{yahoo_api.BASE_URL}/game/nfl/players;player_keys={",".join(test_player_keys)}?format=json',
            
            # Format 3: With specific game code
            f'{yahoo_api.BASE_URL}/game/238/players;player_keys={",".join(test_player_keys)}?format=json',
            
            # Format 4: League-specific format
            f'{yahoo_api.BASE_URL}/league/238.l.627060/players;player_keys={",".join(test_player_keys)}?format=json'
        ]
        
        # Test each endpoint
        for i, endpoint in enumerate(endpoints, 1):
            logger.info(f"\nTesting endpoint format {i}:")
            logger.info(f"Endpoint: {endpoint}")
            
            try:
                response = yahoo_api.make_request(endpoint)
                if response and 'fantasy_content' in response:
                    logger.info("✅ Success! Got valid response")
                    logger.info("Response structure:")
                    if 'fantasy_content' in response:
                        if 'players' in response['fantasy_content']:
                            players = response['fantasy_content']['players']
                            logger.info(f"Found {players.get('count', 0)} players")
                        else:
                            logger.info("No players in response")
                    return endpoint  # Return the first working endpoint format
                else:
                    logger.error("❌ Invalid response format")
            except Exception as e:
                logger.error(f"❌ Error with endpoint {i}: {str(e)}")
            
            logger.info("-" * 50)
        
        return None
            
    except Exception as e:
        logger.error(f"Error during API test: {str(e)}")
        return None

if __name__ == "__main__":
    working_endpoint = test_player_api()
    if working_endpoint:
        logger.info(f"\n✨ Found working endpoint format: {working_endpoint}")
    else:
        logger.error("\n❌ No working endpoint format found") 