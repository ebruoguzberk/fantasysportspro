import logging
import os
from yahoo_api import YahooFantasyAPI
from dotenv import load_dotenv
import json
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Allow OAuth2 over HTTP for development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

def test_roster_api():
    """Test roster retrieval for 2024 NFL league"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.token:
            logger.error("No token available")
            return

        # 2024 NFL League
        league_key = "449.l.214214"  # Format: {game_id}.l.{league_id}
        team_key = "449.l.214214.t.1"  # Format: {game_id}.l.{league_id}.t.{team_id}

        # Parse IDs from keys
        game_id = league_key.split('.')[0]
        league_id = league_key.split('.')[2]
        team_id = team_key.split('.')[-1]

        # Get roster data
        endpoint = f"{yahoo_api.BASE_URL}/team/{team_key}/roster;week=1?format=json"
        logger.info(f"Getting roster for team {team_key}")
        
        response = yahoo_api.make_request(endpoint)
        
        if response and 'fantasy_content' in response:
            fantasy_content = response['fantasy_content']
            team_data = fantasy_content.get('team', [])
            
            if len(team_data) >= 2:
                roster_data = team_data[1].get('roster', {})
                
                if roster_data and '0' in roster_data:
                    players = roster_data['0'].get('players', {})
                    
                    # Print header matching the schema
                    logger.info("\nRoster Data (Schema Format):")
                    logger.info("=" * 150)
                    logger.info(f"{'Player Key':<15} {'Name':<25} {'First':<15} {'Last':<15} {'Pos Type':<10} {'Eligible Pos':<15} "
                              f"{'Selected Pos':<15} {'Status':<10} {'Starting':<10} {'Number':<8} {'NFL Team':<10}")
                    logger.info("-" * 150)
                    
                    # Process each player
                    for key, player_data in players.items():
                        if key == 'count':
                            continue
                            
                        if not isinstance(player_data, dict) or 'player' not in player_data:
                            continue
                            
                        player = player_data['player']
                        if len(player) < 2:
                            continue
                            
                        # Get player info and selected position
                        player_info = player[0]
                        selected_position = player[1]['selected_position'][1]['position']
                        
                        # Initialize player details matching schema
                        player_details = {
                            'player_key': None,
                            'player_id': None,
                            'name': {'full': None, 'first': None, 'last': None},
                            'position_type': None,
                            'eligible_positions': [],
                            'selected_position': selected_position,
                            'status': None,
                            'is_starting': selected_position not in ['BN', 'IR', 'TAXI'],  # Bench, Injured Reserve, Taxi Squad
                            'uniform_number': None,
                            'nfl_team': None
                        }
                        
                        # Extract player details
                        for attr in player_info:
                            if isinstance(attr, dict):
                                if 'player_key' in attr:
                                    player_details['player_key'] = attr['player_key']
                                elif 'player_id' in attr:
                                    player_details['player_id'] = attr['player_id']
                                elif 'name' in attr:
                                    name_data = attr['name']
                                    player_details['name']['full'] = name_data.get('full')
                                    player_details['name']['first'] = name_data.get('first')
                                    player_details['name']['last'] = name_data.get('last')
                                elif 'position_type' in attr:
                                    player_details['position_type'] = attr['position_type']
                                elif 'eligible_positions' in attr:
                                    player_details['eligible_positions'] = [pos['position'] for pos in attr['eligible_positions']]
                                elif 'status' in attr:
                                    player_details['status'] = attr['status']
                                elif 'uniform_number' in attr:
                                    player_details['uniform_number'] = attr['uniform_number']
                                elif 'editorial_team_abbr' in attr:
                                    player_details['nfl_team'] = attr['editorial_team_abbr']
                        
                        # Format and display the data
                        logger.info(
                            f"{player_details['player_key']:<15} "
                            f"{(player_details['name']['full'] or ''):<25} "
                            f"{(player_details['name']['first'] or ''):<15} "
                            f"{(player_details['name']['last'] or ''):<15} "
                            f"{(player_details['position_type'] or ''):<10} "
                            f"{(','.join(player_details['eligible_positions'])):<15} "
                            f"{player_details['selected_position']:<15} "
                            f"{(player_details['status'] or ''):<10} "
                            f"{str(player_details['is_starting']):<10} "
                            f"{(player_details['uniform_number'] or ''):<8} "
                            f"{(player_details['nfl_team'] or ''):<10}"
                        )
                    
                    logger.info("=" * 150)
                    logger.info("\nMetadata:")
                    logger.info(f"Game ID: {game_id}")
                    logger.info(f"League ID: {league_id}")
                    logger.info(f"Team ID: {team_id}")
                    logger.info(f"Season Year: 2024")
                    logger.info(f"Week: {roster_data.get('week', 1)}")
                    
                else:
                    logger.error("No roster data found")
            else:
                logger.error("Invalid team data structure")
        else:
            logger.error("Failed to get response")

    except Exception as e:
        logger.error(f"Error during API test: {str(e)}")

if __name__ == "__main__":
    test_roster_api() 