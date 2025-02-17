import logging
import time
from yahoo_api import YahooFantasyAPI
from database import Database
import random
from typing import List, Dict, Any
import json
from datetime import datetime
import os

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

def get_leagues(db: Database) -> List[Dict[str, Any]]:
    """Get leagues from the most recent season"""
    try:
        cursor = db.conn.cursor()
        cursor.execute("""
            WITH latest_season AS (
                SELECT sport_code, MAX(season) as season
                FROM leagues
                GROUP BY sport_code
            )
            SELECT l.league_key, l.name, l.season, l.sport_code
            FROM leagues l
            JOIN latest_season ls ON l.sport_code = ls.sport_code AND l.season = ls.season
            ORDER BY l.sport_code, l.league_key
        """)
        return [
            {
                'league_key': row[0],
                'name': row[1],
                'season': row[2],
                'sport_code': row[3]
            }
            for row in cursor.fetchall()
        ]
    except Exception as e:
        logger.error(f"Error getting leagues: {str(e)}")
        return []

def get_teams_for_league(yahoo_api: YahooFantasyAPI, league_key: str) -> List[Dict[str, Any]]:
    """Get all teams for a league"""
    try:
        endpoint = f'{yahoo_api.BASE_URL}/league/{league_key}/teams?format=json'
        teams_data = yahoo_api.make_request(endpoint)
        
        if not teams_data or 'fantasy_content' not in teams_data:
            return []
            
        league_data = teams_data['fantasy_content']['league']
        if len(league_data) < 2 or 'teams' not in league_data[1]:
            return []
            
        teams = league_data[1]['teams']
        if not teams or not isinstance(teams, dict):
            return []
            
        team_list = []
        count = int(teams.get('count', 0))
        
        for i in range(count):
            if str(i) not in teams:
                continue
                
            team_data = teams[str(i)]['team'][0]
            team_key = next((item['team_key'] for item in team_data if isinstance(item, dict) and 'team_key' in item), None)
            if not team_key:
                continue
                
            team_list.append({'team_key': team_key})
            
        return team_list
    except Exception as e:
        logger.error(f"Error getting teams for league {league_key}: {str(e)}")
        return []

def get_roster_for_team(yahoo_api: YahooFantasyAPI, team_key: str, week: int = 1) -> List[Dict]:
    """Get roster for a team for a specific week."""
    try:
        # Get the roster data
        endpoint = f"{yahoo_api.BASE_URL}/team/{team_key}/roster;week={week}?format=json"
        roster_data = yahoo_api.make_request(endpoint)
        
        if not roster_data or 'fantasy_content' not in roster_data:
            return []
            
        team_data = roster_data['fantasy_content']['team']
        if len(team_data) < 2 or 'roster' not in team_data[1]:
            return []
            
        roster = team_data[1]['roster']
        if not roster or '0' not in roster or 'players' not in roster['0']:
            return []
            
        players = roster['0']['players']
        if not players or not isinstance(players, dict):
            return []
            
        roster_players = []
        count = int(players.get('count', 0))
        
        for i in range(count):
            if str(i) not in players:
                continue
                
            player_data = players[str(i)]['player']
            if len(player_data) < 2:
                continue
                
            # Get player info and selected position
            player_info = {}
            selected_position = None
            
            # Extract player details from the first element
            for item in player_data[0]:
                if isinstance(item, dict):
                    player_info.update(item)
            
            # Get selected position from the second element
            if len(player_data) > 1 and 'selected_position' in player_data[1]:
                selected_position = player_data[1]['selected_position'][1]['position']
                player_info['selected_position'] = selected_position
                player_info['is_starting'] = selected_position not in ['BN', 'IR', 'TAXI']
            
            roster_players.append(player_info)
            
        return roster_players
        
    except Exception as e:
        print(f"Error getting roster for team {team_key}: {str(e)}")
        return []

def process_team_roster(response: Dict) -> List[Dict[str, Any]]:
    """Process a single team's roster response, exactly matching the test file's processing."""
    if not response or 'fantasy_content' not in response:
        logger.warning("Invalid response structure")
        return []
        
    team_data = response['fantasy_content'].get('team', [])
    if len(team_data) < 2:
        logger.warning("Invalid team data structure")
        return []
        
    roster_data = team_data[1].get('roster', {})
    if not roster_data or 'players' not in roster_data:
        logger.warning("No roster data found")
        return []
        
    players = roster_data['players']
    if not players or not isinstance(players, dict):
        logger.warning("Invalid players data structure")
        return []
        
    roster_players = []
    
    for player_id, player_data in players.items():
        if player_id == 'count':
            continue
            
        if not isinstance(player_data, dict) or 'player' not in player_data:
            continue
            
        try:
            player_info = player_data['player'][0]
            selected_position = player_data['player'][1]['selected_position'][1]['position']
            
            player_dict = {}
            
            # Extract player details exactly as in test
            for attr in player_info:
                if isinstance(attr, dict):
                    if 'player_key' in attr:
                        player_dict['player_key'] = attr['player_key']
                    elif 'name' in attr:
                        player_dict['name'] = attr['name'].get('full', 'Unknown')
                    elif 'editorial_team_abbr' in attr:
                        player_dict['team'] = attr['editorial_team_abbr']
                    elif 'display_position' in attr:
                        player_dict['position'] = attr['display_position']
                    elif 'status' in attr:
                        player_dict['status'] = attr['status']
                    elif 'injury_note' in attr:
                        player_dict['injury_note'] = attr['injury_note']
                    elif 'uniform_number' in attr:
                        player_dict['uniform_number'] = attr['uniform_number']
                    elif 'image_url' in attr:
                        player_dict['headshot_url'] = attr['image_url']
                    elif 'headshot' in attr and isinstance(attr['headshot'], dict):
                        player_dict['headshot_url'] = attr['headshot'].get('url')
            
            if player_dict.get('player_key'):
                player_dict['selected_position'] = selected_position
                roster_players.append(player_dict)
                logger.info(f"Found player: {player_dict.get('name')} ({player_dict.get('team')} - {player_dict.get('position')}) - Roster Spot: {selected_position}")
                if player_dict.get('status'):
                    logger.info(f"    Status: {player_dict['status']}")
                    
        except Exception as e:
            logger.error(f"Error processing player data: {str(e)}")
            continue
            
    return roster_players

def process_league_roster(response: Dict, target_team_key: str) -> List[Dict[str, Any]]:
    """Process a league-wide roster response, filtering for the target team."""
    if not response or 'fantasy_content' not in response:
        logger.warning("Invalid response structure")
        return []
        
    league_data = response['fantasy_content'].get('league', [])
    if len(league_data) < 2:
        logger.warning("Invalid league data structure")
        return []
        
    teams_data = league_data[1].get('teams', {})
    if not teams_data:
        logger.warning("No teams data found")
        return []
        
    team_count = int(teams_data.get('count', 0))
    logger.info(f"Found {team_count} teams in league response")
    
    for i in range(team_count):
        team = teams_data.get(str(i))
        if not team or 'team' not in team:
            continue
            
        team_data = team['team']
        if len(team_data) < 2:
            continue
            
        # Get team key
        team_key = None
        for item in team_data[0]:
            if isinstance(item, dict) and 'team_key' in item:
                team_key = item['team_key']
                break
        
        if team_key == target_team_key:
            logger.info(f"Found target team: {team_key}")
            if 'roster' in team_data[1]:
                return process_team_roster({'fantasy_content': {'team': team_data}})
            else:
                logger.warning("No roster data found for team")
                return []
    
    logger.warning(f"Target team {target_team_key} not found in league response")
    return []

def save_roster(db: Database, league_key: str, team_key: str, roster_players: List[Dict[str, Any]], week: int = 1):
    """Save roster to database with enhanced player information
    
    Args:
        db (Database): Database connection
        league_key (str): League key (format: {game_id}.l.{league_id})
        team_key (str): Team key (format: {game_id}.l.{league_id}.t.{team_id})
        roster_players (List[Dict[str, Any]]): List of player data to save
        week (int): Week number (default: 1)
    """
    try:
        cursor = db.conn.cursor()
        
        # Parse IDs from keys
        game_id = int(league_key.split('.')[0])
        league_id = int(league_key.split('.')[2])
        team_id = int(team_key.split('.')[-1])
        season_year = 2024 if game_id == 449 else 2023  # Map game_id to season year
        
        # Ensure the rosters table exists with schema structure
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS rosters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_year INTEGER NOT NULL,
            week INTEGER,
            date DATE,
            
            game_id INTEGER NOT NULL,
            league_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            
            player_id INTEGER NOT NULL,
            player_key TEXT NOT NULL,
            player_name TEXT NOT NULL,
            player_first_name TEXT,
            player_last_name TEXT,
            
            position_type TEXT NOT NULL,
            eligible_positions TEXT[],
            selected_position TEXT NOT NULL,
            
            status TEXT,
            is_starting BOOLEAN,
            uniform_number TEXT,
            
            nfl_team TEXT,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(season_year, week, date, game_id, league_id, team_id, player_id)
        )
        ''')
        
        # Create indexes if they don't exist
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS rosters_season_week_idx ON rosters(season_year, week)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS rosters_player_idx ON rosters(player_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS rosters_team_idx ON rosters(team_id)
        ''')
        
        db.conn.commit()
        
        # Delete existing roster entries for this team and week
        cursor.execute("""
            DELETE FROM rosters 
            WHERE game_id = ? AND league_id = ? AND team_id = ? AND week = ?
        """, (game_id, league_id, team_id, week))
        
        # Insert new roster entries with all schema fields
        for player in roster_players:
            # Extract player ID from player key
            player_id = int(player['player_key'].split('.')[-1])
            
            # Convert eligible positions list to string representation
            eligible_positions_str = json.dumps(player.get('eligible_positions', []))
            
            cursor.execute("""
                INSERT INTO rosters (
                    season_year, week, date,
                    game_id, league_id, team_id,
                    player_id, player_key, player_name, player_first_name, player_last_name,
                    position_type, eligible_positions, selected_position,
                    status, is_starting, uniform_number,
                    nfl_team,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                season_year,
                week,
                None,  # date (NULL for NFL)
                game_id,
                league_id,
                team_id,
                player_id,
                player['player_key'],
                player['name']['full'],
                player['name']['first'],
                player['name']['last'],
                player['position_type'],
                eligible_positions_str,
                player['selected_position'],
                player.get('status'),
                player['is_starting'],
                player.get('uniform_number'),
                player.get('nfl_team')
            ))
            
        db.conn.commit()
        logger.info(f"Saved {len(roster_players)} players for team {team_key} week {week}")
        
    except Exception as e:
        logger.error(f"Error saving roster for team {team_key}: {str(e)}")
        raise

def load_all_rosters():
    """Load rosters for all leagues"""
    try:
        # Initialize Yahoo API and database
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.token:
            logger.error("No token available")
            return
            
        db = Database(db_path='fantasy_data.db')
        
        # Get all leagues
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT league_key, name, sport_code, season
            FROM leagues
            WHERE sport_code = 'nfl'  -- Only load NFL leagues
            ORDER BY season DESC, sport_code
        """)
        leagues = cursor.fetchall()
        
        if not leagues:
            logger.error("No leagues found")
            return
            
        total_leagues = len(leagues)
        total_teams_processed = 0
        total_rosters_processed = 0
        failed_rosters = []
        
        # Cache for league settings
        league_settings_cache = {}
        
        # Process each league
        for idx, (league_key, league_name, sport_code, season) in enumerate(leagues, 1):
            logger.info(f"\nProcessing league {idx}/{total_leagues}: {league_name} ({league_key})")
            logger.info(f"Season: {season}, Sport: {sport_code}")
            
            # Get league settings to determine number of weeks
            if league_key not in league_settings_cache:
                league_settings = yahoo_api.get_league(league_key)
                league_settings_cache[league_key] = league_settings
            else:
                league_settings = league_settings_cache[league_key]
                
            num_weeks = 18  # Default to 18 weeks for NFL
            if league_settings and 'fantasy_content' in league_settings:
                league_data = league_settings['fantasy_content'].get('league', [])
                if len(league_data) > 0:
                    for item in league_data[0]:
                        if isinstance(item, dict) and 'end_week' in item:
                            num_weeks = int(item['end_week'])
                            break
            
            logger.info(f"League has {num_weeks} weeks")
            
            # Get teams for league
            teams = get_teams_for_league(yahoo_api, league_key)
            if not teams:
                logger.warning(f"No teams found for league {league_key}")
                continue
                
            logger.info(f"Found {len(teams)} teams")
            
            # Process each team
            for team in teams:
                team_key = team['team_key']
                logger.info(f"Getting roster for team {team_key}")
                
                # Process each week
                for week in range(1, num_weeks + 1):
                    try:
                        logger.info(f"Processing week {week}")
                        roster = get_roster_for_team(yahoo_api, team_key, week)
                        if roster:
                            try:
                                save_roster(db, league_key, team_key, roster, week)
                                logger.info(f"Saved roster with {len(roster)} players for team {team_key} week {week}")
                                total_rosters_processed += 1
                            except Exception as e:
                                logger.error(f"Failed to save roster for team {team_key} week {week}: {str(e)}")
                                failed_rosters.append((team_key, week))
                        else:
                            logger.warning(f"No roster found for team {team_key} week {week}")
                    except Exception as e:
                        logger.error(f"Error processing roster for team {team_key} week {week}: {str(e)}")
                        failed_rosters.append((team_key, week))
                    
                    time.sleep(1.1)  # Reduced wait time while staying under rate limit
                
                total_teams_processed += 1
                
            logger.info(f"Completed league {league_name}")
            
        # Log final summary
        logger.info(f"\nRoster loading complete!")
        logger.info(f"Processed {total_teams_processed} teams across {total_leagues} leagues")
        logger.info(f"Successfully loaded {total_rosters_processed} rosters")
        
        if failed_rosters:
            logger.warning(f"\nFailed to process {len(failed_rosters)} rosters:")
            for team_key, week in failed_rosters:
                logger.warning(f"- Team {team_key} Week {week}")
        
    except Exception as e:
        logger.error(f"Error during roster load: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_all_rosters() 