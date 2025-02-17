import logging
import os
from yahoo_api import YahooFantasyAPI
from database import Database
import json
from dotenv import load_dotenv
import requests
from pprint import pprint
from typing import Dict, Any

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_team_info(team_data: list) -> Dict[str, Any]:
    """Extract relevant team information from the API response"""
    team_info = {}
    if team_data and len(team_data) >= 2:
        # Basic team info is in the first element
        for item in team_data[0]:
            if isinstance(item, dict):
                if 'team_key' in item:
                    team_info['team_key'] = item['team_key']
                elif 'name' in item:
                    team_info['name'] = item['name']
                elif 'managers' in item:
                    manager = item['managers'][0]['manager']
                    team_info['manager'] = manager.get('nickname', 'Unknown')
        
        # Points info is in the second element
        points_info = team_data[1]
        if 'team_points' in points_info:
            team_info['points'] = points_info['team_points'].get('total', '0')
        if 'team_projected_points' in points_info:
            team_info['projected_points'] = points_info['team_projected_points'].get('total', '0')
    
    return team_info

def test_league_schedule():
    """Test function to display league schedule data in a readable format"""
    try:
        # Initialize API and DB
        yahoo_api = YahooFantasyAPI()
        
        # Check if token is available
        if not yahoo_api.token:
            logger.error("No Yahoo API token available. Please check your environment variables.")
            return
            
        db = Database(db_path='fantasy_data.db')
        
        # Get one league key from the database
        cursor = db.conn.cursor()
        cursor.execute("SELECT league_key, name FROM leagues LIMIT 1")
        result = cursor.fetchone()
        
        if not result:
            logger.error("No leagues found in database")
            return
            
        league_key, league_name = result
        logger.info(f"\nAnalyzing schedule for: {league_name} ({league_key})")
        
        # Get the full schedule data
        endpoint = f"/league/{league_key}/scoreboard;type=full?format=json"
        schedule_data = yahoo_api.make_request(endpoint)
        
        if not schedule_data or 'fantasy_content' not in schedule_data:
            logger.error("No valid schedule data received")
            return
            
        # Extract league info
        league_data = schedule_data['fantasy_content']['league']
        league_info = league_data[0]
        
        print("\n=== League Information ===")
        print(f"Name: {league_info.get('name')}")
        print(f"Current Week: {league_info.get('current_week')}")
        print(f"Start Week: {league_info.get('start_week')}")
        print(f"End Week: {league_info.get('end_week')}")
        print(f"Season: {league_info.get('season')}")
        print(f"Game Code: {league_info.get('game_code')}")
        print(f"Num Teams: {league_info.get('num_teams')}")
        
        # Extract matchups
        if len(league_data) > 1 and 'scoreboard' in league_data[1]:
            scoreboard = league_data[1]['scoreboard']
            current_week = scoreboard.get('week')
            matchups = scoreboard.get('0', {}).get('matchups', {})
            
            print(f"\n=== Week {current_week} Matchups ===")
            
            for matchup_idx in range(int(matchups.get('count', 0))):
                matchup = matchups.get(str(matchup_idx), {}).get('matchup', {})
                if matchup:
                    print(f"\nMatchup {matchup_idx + 1}:")
                    print(f"Status: {matchup.get('status')}")
                    print(f"Week Start: {matchup.get('week_start')}")
                    print(f"Week End: {matchup.get('week_end')}")
                    print(f"Is Playoffs: {'Yes' if matchup.get('is_playoffs') == '1' else 'No'}")
                    print(f"Is Consolation: {'Yes' if matchup.get('is_consolation') == '1' else 'No'}")
                    
                    teams = matchup.get('0', {}).get('teams', {})
                    if teams and int(teams.get('count', 0)) == 2:
                        # Home Team
                        home_team = extract_team_info(teams.get('0', {}).get('team', []))
                        # Away Team
                        away_team = extract_team_info(teams.get('1', {}).get('team', []))
                        
                        print("\nTeams:")
                        print(f"  Home: {home_team.get('name')} (Manager: {home_team.get('manager')})")
                        print(f"    Points: {home_team.get('points')} (Projected: {home_team.get('projected_points')})")
                        print(f"  Away: {away_team.get('name')} (Manager: {away_team.get('manager')})")
                        print(f"    Points: {away_team.get('points')} (Projected: {away_team.get('projected_points')})")
                        
                        winner_key = matchup.get('winner_team_key')
                        if winner_key:
                            winner = "Home Team" if winner_key == home_team.get('team_key') else "Away Team"
                            print(f"  Winner: {winner}")
                    
                    print("-" * 50)
            
    except Exception as e:
        logger.error(f"Error processing schedule data: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    test_league_schedule() 