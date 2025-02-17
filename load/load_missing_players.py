import logging
import time
from yahoo_api import YahooFantasyAPI
from database import Database
import random
from typing import List, Set, Dict
import sqlite3
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Allow OAuth2 over HTTP for development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

def wait_with_backoff(retry_count, base_wait=2):
    """Wait with exponential backoff"""
    wait_time = base_wait * (2 ** retry_count) + random.uniform(0, 1)
    logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
    time.sleep(wait_time)

def get_missing_player_keys(db: Database) -> Dict[str, List[str]]:
    """Get all missing player keys grouped by game code"""
    try:
        cursor = db.conn.cursor()
        
        # First, let's check if we have any draft results
        cursor.execute("SELECT COUNT(*) FROM league_draft_results")
        count = cursor.fetchone()[0]
        logger.info(f"Total draft results found: {count}")
        
        # Modified query to ensure we only get non-null player keys
        cursor.execute("""
            SELECT dr.player_key, dr.league_key
            FROM league_draft_results dr 
            LEFT OUTER JOIN players ON players.player_key = dr.player_key 
            WHERE players.player_key IS NULL 
            AND dr.player_key IS NOT NULL
            ORDER BY dr.player_key
        """)
        
        results = cursor.fetchall()
        logger.info(f"Found {len(results)} missing players")
        
        missing_players: Dict[str, List[str]] = {}
        for row in results:
            player_key = row[0]
            league_key = row[1]
            
            if not player_key:  # Skip if player_key is None or empty
                logger.warning(f"Skipping invalid player key in league {league_key}")
                continue
                
            try:
                game_code = player_key.split('.')[0]
                if game_code not in missing_players:
                    missing_players[game_code] = []
                missing_players[game_code].append(player_key)
            except Exception as e:
                logger.warning(f"Error processing player key {player_key} in league {league_key}: {str(e)}")
                continue
        
        # Log summary of missing players by game code
        for game_code, players in missing_players.items():
            logger.info(f"Game code {game_code}: {len(players)} missing players")
            
        return missing_players
    except Exception as e:
        logger.error(f"Error getting missing player keys: {str(e)}")
        return {}

def load_players_batch(yahoo_api: YahooFantasyAPI, game_code: str, start: int, count: int, needed_player_keys: Set[str], max_retries=3) -> List[dict]:
    """Load a batch of players with retry logic"""
    retry_count = 0
    while retry_count <= max_retries:
        try:
            endpoint = f'{yahoo_api.BASE_URL}/game/{game_code}/players;start={start};count={count}?format=json'
            logger.debug(f"Making request to endpoint: {endpoint}")
            
            players_data = yahoo_api.make_request(endpoint)
            
            if not players_data or 'fantasy_content' not in players_data:
                if retry_count < max_retries:
                    retry_count += 1
                    wait_with_backoff(retry_count)
                    continue
                return []
            
            game_data = players_data['fantasy_content'].get('game', [{}])
            if len(game_data) < 2:
                if retry_count < max_retries:
                    retry_count += 1
                    wait_with_backoff(retry_count)
                    continue
                return []
                
            players = game_data[1].get('players', {})
            if not players or not isinstance(players, dict):
                return []
                
            processed_players = []
            count = int(players.get('count', 0))
            
            for i in range(count):
                player_data = players.get(str(i), {}).get('player', [])
                if not player_data:
                    continue
                    
                try:
                    # Extract player info
                    player_info = player_data[0]
                    
                    # Get player key
                    player_key = next((item.get('player_key') for item in player_info if isinstance(item, dict) and 'player_key' in item), None)
                    if not player_key or player_key not in needed_player_keys:
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
                    
                    processed_players.append({
                        'player_key': player_key,
                        'sport_code': game_code,
                        'name': name,
                        'team': editorial_team,
                        'position': display_position,
                        'status': status,
                        'injury_note': injury_note,
                        'headshot_url': headshot_url
                    })
                    logger.info(f"Found needed player: {name} ({player_key})")
                    
                except Exception as e:
                    logger.error(f"Error processing player data: {str(e)}")
                    continue
                    
            return processed_players
            
        except Exception as e:
            error_str = str(e)
            if "400 Client Error" in error_str:
                logger.info(f"Reached end of available players for game {game_code} at position {start}")
                return []  # Return empty list to indicate we're done
            logger.error(f"Error loading batch: {error_str}")
            if retry_count < max_retries:
                retry_count += 1
                wait_with_backoff(retry_count)
            else:
                return []

def load_missing_players():
    """Load all missing players in batches"""
    try:
        # Initialize Yahoo API and database
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.token:
            logger.error("No token available")
            return
            
        db = Database(db_path='fantasy_data.db')
        
        # Get missing player keys grouped by game code
        missing_players = get_missing_player_keys(db)
        if not missing_players:
            logger.info("No missing players found")
            return
            
        total_processed = 0
        total_failed = 0
        
        # Process each game code separately
        for game_code, player_keys in missing_players.items():
            logger.info(f"\nProcessing {len(player_keys)} missing players for game code {game_code}")
            needed_player_keys = set(player_keys)  # Convert to set for faster lookups
            initial_needed_count = len(needed_player_keys)
            
            # Start from the beginning and process in batches
            start = 0
            batch_size = 25
            consecutive_empty = 0
            
            while True:
                logger.info(f"Loading players batch starting at position {start}")
                processed_players = load_players_batch(yahoo_api, game_code, start, batch_size, needed_player_keys)
                
                if not processed_players:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:  # If we get no players 3 times in a row, assume we're done
                        logger.info(f"No more players found after {consecutive_empty} attempts")
                        # Log how many players we still need
                        remaining_count = len(needed_player_keys)
                        if remaining_count > 0:
                            logger.warning(f"Could not find {remaining_count} players for game {game_code}:")
                            for key in needed_player_keys:
                                logger.warning(f"  - {key}")
                        break
                    logger.info("No players in batch, will retry...")
                    wait_with_backoff(consecutive_empty)
                    continue
                
                consecutive_empty = 0  # Reset counter when we get players
                
                # Save processed players to database
                for player in processed_players:
                    try:
                        db.save_player(player)
                        total_processed += 1
                        needed_player_keys.remove(player['player_key'])  # Remove from needed set
                        logger.info(f"Saved player: {player['name']} ({player['player_key']})")
                    except Exception as e:
                        logger.error(f"Error saving player to database: {str(e)}")
                        total_failed += 1
                
                # If we've found all needed players for this game code, we can stop
                if not needed_player_keys:
                    logger.info(f"Found all needed players for game code {game_code}")
                    break
                
                # Move to next batch
                start += batch_size
                
                # Add rate limiting delay
                time.sleep(2.1)  # Slightly over 2 seconds to stay under rate limit
            
            # Log summary for this game code
            found_count = initial_needed_count - len(needed_player_keys)
            logger.info(f"\nGame {game_code} summary:")
            logger.info(f"Found {found_count} out of {initial_needed_count} needed players")
            if needed_player_keys:
                logger.info(f"Missing {len(needed_player_keys)} players")
            
        logger.info(f"\nProcessing complete!")
        logger.info(f"Successfully processed: {total_processed}")
        logger.info(f"Failed to process: {total_failed}")
        
    except Exception as e:
        logger.error(f"Error during missing players load: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_missing_players()
