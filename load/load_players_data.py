import logging
import time
from yahoo_api import YahooFantasyAPI
from database import Database
from datetime import datetime
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def wait_with_backoff(retry_count, base_wait=2):
    """Wait with exponential backoff"""
    wait_time = base_wait * (2 ** retry_count) + random.uniform(0, 1)
    logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
    time.sleep(wait_time)

def get_nfl_game_keys(yahoo_api):
    """Get all NFL game keys from the database, sorted starting from 406 and going backwards"""
    try:
        # Get games from Yahoo API
        response = yahoo_api.make_request('/users;use_login=1/games?format=json')
        
        if not response or 'fantasy_content' not in response:
            logger.error("No fantasy content in response")
            return []
            
        users = response['fantasy_content'].get('users', {})
        if '0' not in users:
            logger.error("No user data found")
            return []
            
        user_data = users['0'].get('user', [])
        if len(user_data) < 2:
            logger.error("Incomplete user data")
            return []
            
        games_data = user_data[1].get('games', {})
        games_count = int(games_data.get('count', 0))
        
        # Extract NFL game keys with seasons directly from API response
        game_keys_with_seasons = []
        for i in range(games_count):
            idx = str(i)
            if idx not in games_data:
                continue
                
            game = games_data[idx].get('game', [])
            if not game:
                continue
                
            # Handle both list and dict formats
            game_info = game[0] if isinstance(game, list) else game
            
            if game_info.get('code') == 'nfl':
                game_key = game_info.get('game_key')
                # Only include game key 49
                if game_key and int(game_key) == 49:
                    game_keys_with_seasons.append({
                        'game_key': game_key,
                        'season': int(game_info.get('season', 0))
                    })
                
        # Sort by game_key in descending order
        game_keys_with_seasons.sort(key=lambda x: int(x['game_key']), reverse=True)
        
        # Log the sorted game keys
        logger.info("Game keys sorted by game key (starting from 49):")
        for game in game_keys_with_seasons:
            logger.info(f"Game key {game['game_key']} (Season {game['season']})")
            
        return [game['game_key'] for game in game_keys_with_seasons]
        
    except Exception as e:
        logger.error(f"Error getting NFL game keys: {str(e)}")
        return []

def load_players_batch(yahoo_api, game_key, start, count, existing_player_keys, max_retries=3):
    """Load a batch of players with retry logic"""
    retry_count = 0
    while retry_count <= max_retries:
        try:
            endpoint = f'/game/{game_key}/players;start={start};count={count}'
            players_data = yahoo_api.make_request(endpoint)
            
            if not players_data or 'fantasy_content' not in players_data:
                if retry_count < max_retries:
                    retry_count += 1
                    wait_with_backoff(retry_count)
                    continue
                return None, 0
                
            game_data = players_data['fantasy_content'].get('game', [{}])
            if len(game_data) < 2:
                if retry_count < max_retries:
                    retry_count += 1
                    wait_with_backoff(retry_count)
                    continue
                return None, 0
                
            players = game_data[1].get('players', {})
            if not players or not isinstance(players, dict):
                return None, 0
                
            batch_count = int(players.get('count', 0))
            return players, batch_count
            
        except Exception as e:
            logger.error(f"Error loading batch: {str(e)}")
            if retry_count < max_retries:
                retry_count += 1
                wait_with_backoff(retry_count)
            else:
                return None, 0

def get_last_processed_position(db, game_key):
    """Get the last processed position for a game key"""
    try:
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM players 
            WHERE sport_code = ?
        """, (game_key,))
        return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting last position: {str(e)}")
        return 0

def get_existing_player_keys_for_game(db, game_key):
    """Get existing player keys for a specific game key"""
    try:
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT player_key 
            FROM players 
            WHERE sport_code = ?
        """, (game_key,))
        return {row[0] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"Error getting existing player keys: {str(e)}")
        return set()

def load_players_data():
    """Load remaining players for game key 449"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.access_token:
            logger.error("No access token available")
            return
            
        # Initialize database
        db = Database(db_path='fantasy_data.db')
        
        # Track progress
        processed_players = 0
        failed_players = 0
        
        # Get current count for game 449
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM players WHERE sport_code = '449'")
        current_count = cursor.fetchone()[0]
        logger.info(f"Current player count for game 449: {current_count}")
        
        # Get existing player keys for game 449
        existing_player_keys = get_existing_player_keys_for_game(db, '449')
        logger.info(f"Found {len(existing_player_keys)} existing players for game 449")
        
        # Start from where we left off
        start = current_count
        count = 25  # Smaller batch size to be gentler on the API
        game_key = '449'
        total_loaded = 0
        consecutive_empty = 0
        
        logger.info(f"Starting to load players from position {start}")
        
        while True:
            logger.info(f"Loading players batch starting at {start}")
            
            # Get players from Yahoo API
            players, batch_count = load_players_batch(yahoo_api, game_key, start, count, existing_player_keys)
            
            if players is None or batch_count == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:  # If we get no players 3 times in a row, assume we're done
                    logger.info(f"No more players found after {consecutive_empty} attempts")
                    break
                logger.info("No players in batch, will retry...")
                wait_with_backoff(consecutive_empty)
                continue
            
            consecutive_empty = 0  # Reset counter when we get players
            
            # Process each player in the batch
            new_players_in_batch = 0
            for i in range(batch_count):
                player_data = players.get(str(i), {}).get('player', [])
                if not player_data:
                    continue
                    
                try:
                    # Extract player info
                    player_info = player_data[0]
                    
                    # Get player key
                    player_key = next((item.get('player_key') for item in player_info if isinstance(item, dict) and 'player_key' in item), None)
                    
                    # Skip if we already have this player
                    if player_key in existing_player_keys:
                        continue
                        
                    # Extract player details
                    name = next((item.get('name', {}).get('full') for item in player_info if isinstance(item, dict) and 'name' in item), None)
                    editorial_team = next((item.get('editorial_team_full_name') for item in player_info if isinstance(item, dict) and 'editorial_team_full_name' in item), None)
                    display_position = next((item.get('display_position') for item in player_info if isinstance(item, dict) and 'display_position' in item), None)
                    status = next((item.get('status') for item in player_info if isinstance(item, dict) and 'status' in item), None)
                    injury_note = next((item.get('injury_note') for item in player_info if isinstance(item, dict) and 'injury_note' in item), None)
                    
                    # Get headshot if available
                    headshot_url = None
                    for item in player_info:
                        if isinstance(item, dict) and 'image_url' in item:
                            headshot_url = item.get('image_url')
                            break
                    
                    # Prepare player data for database
                    player_data = {
                        'player_key': player_key,
                        'sport_code': game_key,
                        'name': name,
                        'team': editorial_team,
                        'position': display_position,
                        'status': status,
                        'injury_note': injury_note,
                        'headshot_url': headshot_url
                    }
                    
                    # Save to database
                    db.save_player(player_data)
                    processed_players += 1
                    new_players_in_batch += 1
                    existing_player_keys.add(player_key)
                    logger.info(f"Saved player: {name} ({player_key})")
                    
                except Exception as e:
                    logger.error(f"Error processing player: {str(e)}")
                    failed_players += 1
                    continue
            
            total_loaded += batch_count
            start += batch_count
            
            # Log progress
            logger.info(f"Progress: {start} players processed, {new_players_in_batch} new players in this batch")
            logger.info(f"Total new players saved so far: {processed_players}")
            
            # Add smart rate limiting delay
            wait_time = 2.1  # Slightly over 2 seconds to ensure we stay under 30 requests per minute
            logger.debug(f"Waiting {wait_time} seconds before next batch...")
            time.sleep(wait_time)
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM players WHERE sport_code = '449'")
        final_count = cursor.fetchone()[0]
        logger.info(f"\nFinal player count for game 449: {final_count}")
        logger.info(f"Successfully processed: {processed_players}")
        logger.info(f"Failed to process: {failed_players}")
        
    except Exception as e:
        logger.error(f"Error during players data load: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_players_data() 