import os
import json
import logging
from datetime import datetime
from yahoo_api import YahooFantasyAPI
from dotenv import load_dotenv
from types import SimpleNamespace
from database import Database

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Also set yahoo_api logger to ERROR
yahoo_logger = logging.getLogger('yahoo_api')
yahoo_logger.setLevel(logging.ERROR)

def load_tokens():
    try:
        if os.path.exists('auth.json'):
            with open('auth.json', 'r') as f:
                data = json.load(f)
                print("Found existing authentication file")
                if isinstance(data, dict):
                    if 'token_expiry' in data:
                        data['token_expiry'] = datetime.fromisoformat(data['token_expiry'])
                    if 'last_update' in data:
                        data['last_update'] = datetime.fromisoformat(data['last_update'])
                return data
    except Exception as e:
        print(f"Error loading auth file: {e}")
    return None

def save_tokens(tokens):
    try:
        serializable_tokens = {
            'access_token': tokens['access_token'],
            'refresh_token': tokens['refresh_token'],
            'token_expiry': tokens['token_expiry'].isoformat(),
            'last_update': tokens['last_update'].isoformat()
        }
        with open('auth.json', 'w') as f:
            json.dump(serializable_tokens, f)
            print("Saved authentication tokens")
    except Exception as e:
        print(f"Error saving auth file: {e}")

def authenticate():
    load_dotenv()
    session_state = SimpleNamespace()
    api = YahooFantasyAPI(session_state=session_state)
    
    tokens = load_tokens()
    if tokens:
        session_state.yahoo_tokens = tokens
        api = YahooFantasyAPI(session_state=session_state)
        
        try:
            api.get_user_info()
            return api
        except Exception as e:
            print(f"Authentication error: {e}")
    
    print("\nStarting new authentication process...")
    auth_url = api.get_authorization_url()
    print(f"\nPlease visit this URL to authorize the application:\n{auth_url}")
    
    auth_code = input("\nEnter the authorization code: ")
    
    if api.handle_authorization(auth_code):
        if hasattr(session_state, 'yahoo_tokens'):
            save_tokens(session_state.yahoo_tokens)
        return api
    else:
        print("Authorization failed!")
        return None

def parse_player_data(player_data):
    """Parse individual player data from the API response"""
    if isinstance(player_data, list):
        player_data = player_data[0]
    
    player = {}
    
    # Print raw data for debugging
    print(f"Raw player data: {player_data}")
    
    try:
        for item in player_data:
            if isinstance(item, dict):
                if 'player_key' in item:
                    player['player_key'] = item['player_key']
                elif 'name' in item and isinstance(item['name'], dict):
                    player['name'] = item['name'].get('full')
                elif 'editorial_team_full_name' in item:
                    player['team'] = item['editorial_team_full_name']
                elif 'display_position' in item:
                    player['position'] = item['display_position']
                elif 'status' in item:
                    player['status'] = item['status']
                elif 'injury_note' in item:
                    player['injury_note'] = item['injury_note']
                elif 'headshot' in item and isinstance(item['headshot'], dict):
                    player['headshot_url'] = item['headshot'].get('url')
    except Exception as e:
        print(f"Error parsing player data: {e}")
        print(f"Problematic data: {player_data}")
    
    return player

def get_nfl_players():
    try:
        yahoo_api = authenticate()
        if not yahoo_api:
            logger.error("Failed to authenticate with Yahoo API")
            return
            
        print("Authentication successful, fetching players...")
        start = 0
        count = 25
        all_players = []
        
        # Initialize database connection
        db = Database()
        
        while True:
            print(f"Fetching batch starting at {start}...")
            endpoint = f"/games;game_keys=nfl/players;start={start};count={count}"
            response = yahoo_api.make_request(endpoint)
            
            if not response or 'fantasy_content' not in response:
                print("No response or missing fantasy_content")
                break
                
            games = response['fantasy_content'].get('games', {})
            if not isinstance(games, dict) or '0' not in games:
                print("Invalid games data structure")
                break
                
            game_data = games['0'].get('game', [])
            if len(game_data) < 2:
                print("Incomplete game data")
                break
                
            players_data = game_data[1].get('players', {})
            if not players_data:
                print("No players data found")
                break
                
            current_batch = []
            for i in range(count):
                if str(i) not in players_data:
                    break
                    
                player_data = players_data[str(i)].get('player', [])
                if not player_data:
                    continue
                
                player = parse_player_data(player_data)
                if player and 'name' in player:
                    current_batch.append(player)
                    # Save player to database
                    try:
                        player['sport_code'] = 'nfl'  # Add sport code
                        db.save_player(player)
                        print(f"Saved player to database: {player['name']}")
                    except Exception as e:
                        print(f"Error saving player {player['name']} to database: {str(e)}")
            
            print(f"Found {len(current_batch)} players in this batch")
            
            if not current_batch:
                print("No more players to fetch")
                break
                
            all_players.extend(current_batch)
            start += count
            
            # Break after first batch for testing
            # break  # Removing this line to fetch all batches
        
        print(f"\nTotal players found: {len(all_players)}")
        if all_players:
            print("\nPlayer data:")
            for player in all_players:
                print(player)
        else:
            print("No players were found")
            
    except Exception as e:
        print(f"Error in get_nfl_players: {str(e)}")
        logger.error(f"Error fetching players: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        get_nfl_players()
    except Exception as e:
        print(f"Script error: {str(e)}")