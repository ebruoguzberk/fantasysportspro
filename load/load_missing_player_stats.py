import logging
import time
from yahoo_api import YahooFantasyAPI
from database import Database
import random
import json
from typing import List, Dict, Any, Tuple
import requests

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

def get_active_leagues(db: Database) -> List[Tuple[str, str, str, str]]:
    """Get all active leagues from database"""
    try:
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT league_key, name, sport_code, league_key
            FROM leagues
            ORDER BY league_key DESC
        """)
        return cursor.fetchall()
    except Exception as e:
        logging.error(f"Error getting active leagues: {str(e)}")
        return []

def get_missing_roster_players(db: Database, game_id: int, league_id: int, week: int) -> List[str]:
    """Get players from database that don't have stats for the specified week"""
    try:
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT r.player_key
            FROM rosters r
            LEFT JOIN player_stats ps ON 
                r.player_key = ps.player_key 
                AND ps.week = ?
                AND ps.league_key = ?
            WHERE r.game_id = ? 
            AND r.league_id = ?
            AND ps.player_key IS NULL
        """, (week, f"{game_id}.l.{league_id}", game_id, league_id))
        player_keys = [row[0] for row in cursor.fetchall()]
        return player_keys
    except Exception as e:
        logging.error(f"Error getting missing roster players from database: {str(e)}")
        return []

def load_player_stats_batch(yahoo_api: YahooFantasyAPI, league_key: str, player_keys: List[str], week: int, max_retries=3) -> Dict:
    """Load stats for a batch of players"""
    players_str = ','.join(player_keys)
    
    # Get actual stats
    stats_endpoint = f'{yahoo_api.BASE_URL}/league/{league_key}/players;player_keys={players_str}/stats;type=week;week={week}?format=json'
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Get actual stats
            stats_response = yahoo_api.make_request(stats_endpoint)
            
            if stats_response and 'fantasy_content' in stats_response:
                return stats_response['fantasy_content']
            return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                logging.warning(f"Week {week} not available for league {league_key}")
                return None
            elif retry_count < max_retries - 1:
                retry_count += 1
                wait_with_backoff(retry_count)
            else:
                logging.error(f"Error loading stats batch: {str(e)}")
                raise
        except Exception as e:
            if retry_count < max_retries - 1:
                retry_count += 1
                wait_with_backoff(retry_count)
            else:
                logging.error(f"Error loading stats batch: {str(e)}")
                raise
    
    return None

def extract_player_stats(player_data: Dict, week: int, league_key: str = None) -> Dict:
    """Extract relevant stats from player data"""
    try:
        logger.debug(f"Extracting stats from player data: {json.dumps(player_data, indent=2)}")
        
        stats = {
            'week': week,
            'stats': {},
            'points': 0.0,
            'projected_points': 0.0,
            'season': None,
            'position': None,
            'fan_points': 0.0,
            'percent_started': 0,
            'percent_owned': 0
        }
        
        # Extract season from league key if provided
        if league_key:
            game_id = int(league_key.split('.')[0])
            # Map game_id to season year
            season_map = {
                449: '2024',  # Current season
                423: '2023',  # Previous season
                406: '2022',
                399: '2021',
                390: '2020'
            }
            stats['season'] = season_map.get(game_id)
        
        # Handle the player data structure from the API
        if isinstance(player_data, list) and len(player_data) >= 2:
            player_info = player_data[0]  # First element contains player info
            player_stats = player_data[1]  # Second element contains stats
            
            # Extract position
            if isinstance(player_info, dict) and 'selected_position' in player_info:
                selected_position = player_info['selected_position']
                if isinstance(selected_position, dict):
                    stats['position'] = selected_position.get('position')
            
            # Extract stats and points
            if isinstance(player_stats, dict):
                # Extract percent started and owned
                if 'percent_started' in player_stats:
                    stats['percent_started'] = int(player_stats.get('percent_started', '0'))
                if 'percent_owned' in player_stats:
                    stats['percent_owned'] = int(player_stats.get('percent_owned', '0'))
                
                # Extract actual stats
                if 'player_stats' in player_stats:
                    stats_data = player_stats['player_stats']
                    if isinstance(stats_data, dict) and 'stats' in stats_data:
                        raw_stats = stats_data['stats']
                        if isinstance(raw_stats, list):
                            for stat_entry in raw_stats:
                                if isinstance(stat_entry, dict) and 'stat' in stat_entry:
                                    stat_info = stat_entry['stat']
                                    stat_id = str(stat_info.get('stat_id', ''))
                                    stat_value = stat_info.get('value', '0')
                                    
                                    # Convert '-' to '0' and handle empty strings
                                    if stat_value == '-' or not stat_value:
                                        stat_value = '0'
                                    stats['stats'][stat_id] = stat_value
                
                # Extract fantasy points
                if 'player_points' in player_stats:
                    points_data = player_stats['player_points']
                    if isinstance(points_data, dict):
                        try:
                            total_points = points_data.get('total', '0')
                            stats['points'] = float(total_points) if total_points != '-' else 0.0
                            stats['fan_points'] = stats['points']  # Use the same value for fan_points
                        except (ValueError, TypeError):
                            stats['points'] = 0.0
                            stats['fan_points'] = 0.0
                
                # Always set projected points to 0.0
                stats['projected_points'] = 0.0
        
        return stats
    except Exception as e:
        logger.error(f"Error extracting stats: {str(e)}")
        return {
            'week': week,
            'stats': {},
            'points': 0.0,
            'projected_points': 0.0,
            'season': None,
            'position': None,
            'fan_points': 0.0,
            'percent_started': 0,
            'percent_owned': 0
        }

def save_player_stats(db: Database, player_key: str, stats_data: Dict, league_key: str):
    """Save player stats to database"""
    try:
        cursor = db.conn.cursor()
        
        # Map Yahoo stat IDs to our column names
        stat_mapping = {
            '4': 'passing_yards',
            '5': 'passing_touchdowns',
            '6': 'passing_interceptions',
            '8': 'rushing_attempts',
            '9': 'rushing_yards',
            '10': 'rushing_touchdowns',
            '11': 'receptions',
            '12': 'receiving_yards',
            '13': 'receiving_touchdowns',
            '14': 'passing_2pt_conversions',
            '15': 'rushing_2pt_conversions',
            '16': 'receiving_2pt_conversions',
            '18': 'fumbles_lost',
            '19': 'passing_attempts',
            '20': 'passing_completions',
            '31': 'field_goals_made',
            '32': 'field_goals_attempted',
            '33': 'extra_points_made',
            '78': 'targets'
        }
        
        # Build the column values
        column_values = {
            'player_key': player_key,
            'league_key': league_key,
            'season': stats_data.get('season'),
            'week': stats_data.get('week'),
            'position': stats_data.get('position'),
            'points': stats_data.get('points', 0.0),
            'projected_points': stats_data.get('projected_points', 0.0),
            'fan_points': stats_data.get('fan_points', 0.0)
        }
        
        # Add stat values
        raw_stats = stats_data.get('stats', {})
        for yahoo_stat_id, column_name in stat_mapping.items():
            stat_value = raw_stats.get(yahoo_stat_id, '0')
            # Convert '-' to 0
            column_values[column_name] = int(stat_value) if stat_value != '-' else 0
        
        # Build the SQL query dynamically
        columns = ', '.join(column_values.keys())
        placeholders = ', '.join(['?' for _ in column_values])
        values = tuple(column_values.values())
        
        # Insert or replace the stats
        cursor.execute(f"""
            INSERT OR REPLACE INTO player_stats 
            ({columns}, updated_at) 
            VALUES ({placeholders}, CURRENT_TIMESTAMP)
        """, values)
        
        db.conn.commit()
        
    except Exception as e:
        logger.error(f"Error saving stats for player {player_key}: {str(e)}")
        raise

def get_season_weeks(game_id: int) -> int:
    """Get the number of weeks in a season based on the game ID"""
    # NFL changed to 17 games starting from 2021 season (game_id 399)
    return 17 if game_id >= 399 else 16

def main():
    try:
        # Initialize Yahoo API and database
        yahoo_api = YahooFantasyAPI()
        db = Database()
        
        # Track failures in detail
        failures = {
            'week_not_available': [],
            'no_stats_found': [],
            'api_error': [],
            'other': []
        }
        
        # Get active leagues
        active_leagues = get_active_leagues(db)
        if not active_leagues:
            logging.error("No active leagues found")
            return
            
        # Get unique game keys and sort numerically
        game_keys = sorted({int(league_key.split('.')[0]) for league_key, _, _, _ in active_leagues}, reverse=True)
        logging.info(f"Found {len(game_keys)} games to process")
        
        total_success = 0
        total_failure = 0
        
        for game_key in game_keys:
            logging.info(f"\nProcessing game: {game_key}")
            game_id = game_key
            
            # Get number of weeks for this season
            num_weeks = get_season_weeks(game_id)
            logging.info(f"Season has {num_weeks} weeks")
            
            # Get all leagues for this game
            leagues = [(league_key, league_name) for league_key, league_name, _, _ in active_leagues 
                      if league_key.startswith(f"{game_key}.")]
            
            success_count = 0
            failure_count = 0
            
            for league_key, league_name in leagues:
                logging.info(f"\nProcessing league: {league_name} ({league_key})")
                league_id = int(league_key.split('.')[2])
                
                # Process each week
                for week in range(1, num_weeks + 1):
                    # Get missing players for this week
                    missing_players = get_missing_roster_players(db, game_id, league_id, week)
                    
                    if not missing_players:
                        logging.info(f"No missing players found for week {week}")
                        continue
                        
                    logging.info(f"Found {len(missing_players)} missing players for week {week}")
                    
                    # Process players in batches
                    batch_size = 25
                    for i in range(0, len(missing_players), batch_size):
                        batch = missing_players[i:i + batch_size]
                        
                        try:
                            stats_data = load_player_stats_batch(yahoo_api, league_key, batch, week)
                            if stats_data and 'league' in stats_data:
                                league_data = stats_data['league']
                                if len(league_data) > 1 and 'players' in league_data[1]:
                                    players_stats = league_data[1]['players']
                                    # Process each player's stats
                                    for idx, player_key in enumerate(batch):
                                        try:
                                            if str(idx) in players_stats:
                                                player_data = players_stats[str(idx)].get('player', [])
                                                stats = extract_player_stats(player_data, week, league_key)
                                                if stats:
                                                    save_player_stats(db, player_key, stats, league_key)
                                                    success_count += 1
                                                    logging.info(f"Saved week {week} stats for player {player_key}")
                                            else:
                                                logging.warning(f"No week {week} data found for player {player_key}")
                                                failures['no_stats_found'].append(f"{player_key} - Week {week} - No stats data found")
                                                failure_count += 1
                                        except Exception as e:
                                            logging.error(f"Failed to process week {week} stats for player {player_key}: {str(e)}")
                                            failures['other'].append(f"{player_key} - Week {week} - Error: {str(e)}")
                                            failure_count += 1
                            else:
                                if week > 16 and game_id < 423:
                                    failures['week_not_available'].append(f"{league_key} - Week {week} not available (pre-2021 season)")
                                else:
                                    failures['no_stats_found'].append(f"{league_key} - Week {week} - No stats data available")
                                logging.warning(f"No stats available for week {week}")
                                break
                        except Exception as e:
                            logging.error(f"Failed to process batch: {str(e)}")
                            failures['api_error'].append(f"{league_key} - Week {week} - API Error: {str(e)}")
                            failure_count += len(batch)
                            
                        time.sleep(1)  # Rate limiting
            
            logging.info(f"\nGame {game_key} complete:")
            logging.info(f"Successfully processed: {success_count}")
            logging.info(f"Failed to process: {failure_count}")
            
            total_success += success_count
            total_failure += failure_count
        
        logging.info("\nOverall missing stats loading complete:")
        logging.info(f"Successfully processed: {total_success}")
        logging.info(f"Failed to process: {total_failure}")
        
        # Log detailed failure information
        logging.info("\nDetailed Failure Analysis:")
        logging.info("Week Not Available Errors:")
        for failure in failures['week_not_available']:
            logging.info(f"  {failure}")
        logging.info("\nNo Stats Found Errors:")
        for failure in failures['no_stats_found']:
            logging.info(f"  {failure}")
        logging.info("\nAPI Errors:")
        for failure in failures['api_error']:
            logging.info(f"  {failure}")
        logging.info("\nOther Errors:")
        for failure in failures['other']:
            logging.info(f"  {failure}")
        
    except Exception as e:
        logging.error(f"Error during stats load: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    main() 