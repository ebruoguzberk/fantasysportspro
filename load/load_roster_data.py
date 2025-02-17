import logging
import sys
import json
from typing import Dict, Any, List
from yahoo_api import YahooFantasyAPI
from database import Database
from data_manager import DataManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def process_roster_data(roster: Dict[str, Any], league_key: str, team_key: str) -> List[Dict[str, Any]]:
    """Process roster data and extract player information"""
    roster_entries = []
    
    logger.debug(f"Raw roster response: {roster}")
    
    if not roster or 'fantasy_content' not in roster:
        logger.debug("No fantasy_content in roster data")
        return roster_entries
        
    team_data = roster['fantasy_content'].get('team', [])
    if not team_data or len(team_data) < 2:
        logger.debug(f"Invalid team_data: {team_data}")
        return roster_entries
        
    roster_data = team_data[1].get('roster', {}).get('0', {}).get('players', {})
    if not roster_data:
        logger.debug(f"No roster_data found in team_data[1]: {team_data[1]}")
        return roster_entries
        
    logger.debug(f"Processing roster_data: {roster_data}")
    
    # Exclude the 'count' field and process each player
    for key, value in roster_data.items():
        if key == 'count':
            continue
            
        if not isinstance(value, dict) or 'player' not in value:
            logger.debug(f"Invalid player value for key {key}: {value}")
            continue
            
        player_data = value['player']
        if not isinstance(player_data, list) or len(player_data) < 2:
            logger.debug(f"Invalid player_data structure: {player_data}")
            continue
            
        player_info = {}
        player_details = player_data[0]
        selected_position = player_data[1].get('selected_position', [{}])[0]
        
        logger.debug(f"Processing player_details: {player_details}")
        logger.debug(f"Selected position: {selected_position}")
        
        # Extract player details
        for item in player_details:
            if isinstance(item, dict):
                if 'player_key' in item:
                    player_info['player_key'] = item['player_key']
                elif 'name' in item and isinstance(item['name'], dict):
                    player_info['name'] = item['name'].get('full')
                elif 'display_position' in item:
                    player_info['position'] = item['display_position']
                elif 'status' in item:
                    player_info['status'] = item['status']
                    
        # Add selected position and starting status
        if player_info.get('player_key'):
            player_info.update({
                'league_key': league_key,
                'team_key': team_key,
                'selected_position': selected_position.get('position'),
                'is_starting': 1,  # All players in roster are considered starting
                'week': selected_position.get('week')
            })
            logger.debug(f"Adding player to roster: {player_info}")
            roster_entries.append(player_info)
            
    logger.debug(f"Processed {len(roster_entries)} roster entries")
    return roster_entries

def load_roster_data():
    """Load roster data from Yahoo Fantasy API into database"""
    logger = logging.getLogger(__name__)
    
    # Initialize Yahoo API
    yahoo_api = YahooFantasyAPI()
    if not yahoo_api.access_token:
        logger.error("No access token available")
        return
    logger.info("Using access token")
    
    # Initialize database
    db = Database()
    
    # Get all teams first
    cursor = db.conn.cursor()
    cursor.execute("SELECT team_key FROM teams")
    teams = cursor.fetchall()
    
    # Process teams in batches of 5
    batch_size = 5
    total_entries = 0
    
    for i in range(0, len(teams), batch_size):
        logger.info(f"Processing batch {i//batch_size + 1} of {(len(teams) + batch_size - 1)//batch_size}")
        batch_teams = teams[i:i+batch_size]
        
        for (team_key,) in batch_teams:
            # Extract league_key from team_key (format: <game_key>.l.<league_id>.t.<team_id>)
            league_key = '.'.join(team_key.split('.')[:3])
            
            # Get league settings to determine number of weeks
            cursor.execute("SELECT settings FROM leagues WHERE league_key = ?", (league_key,))
            result = cursor.fetchone()
            if not result:
                logger.warning(f"No settings found for league {league_key}")
                continue
                
            settings = result[0]
            if not settings:
                logger.warning(f"Empty settings for league {league_key}")
                continue
                
            try:
                settings_dict = json.loads(settings)
                # Handle case where end_week is None or not present
                end_week = settings_dict.get('end_week')
                if end_week is None:
                    end_week = 17  # Default to 17 weeks
                else:
                    end_week = int(end_week)
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Invalid settings JSON for league {league_key}: {e}")
                end_week = 17
                
            logger.info(f"Fetching roster for team {team_key} in league {league_key}")
            
            # Fetch roster for each week
            for week in range(1, end_week + 1):
                try:
                    roster = yahoo_api.get_team_roster(team_key, week=week)
                    entries = process_roster_data(roster, league_key, team_key)
                    
                    # Save each roster entry
                    for entry in entries:
                        entry['week'] = week
                        db.save_league_roster(entry)
                        total_entries += 1
                        
                    logger.info(f"Processed {len(entries)} roster entries for team {team_key} week {week}")
                except Exception as e:
                    logger.error(f"Error processing roster for team {team_key} week {week}: {str(e)}")
                    continue
                    
        # Commit after each batch
        db.conn.commit()
        logger.info(f"Committed batch {i//batch_size + 1}")
        
    logger.info(f"Successfully processed {total_entries} roster entries")
    
    # Verify data in database
    cursor.execute("SELECT COUNT(*) FROM league_rosters")
    count = cursor.fetchone()[0]
    logger.info(f"Total roster entries in database: {count}")
    
    # Close database connection
    db.conn.close()

if __name__ == "__main__":
    load_roster_data() 