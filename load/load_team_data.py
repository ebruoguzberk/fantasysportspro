import logging
import os
import time
from dotenv import load_dotenv
from yahoo_api import YahooFantasyAPI
from database import Database
from data_manager import DataManager
from datetime import datetime

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_teams_from_leagues(yahoo_api: YahooFantasyAPI, db: Database) -> set:
    """Load all teams from leagues in the database"""
    processed_teams = set()
    
    # Get all leagues from database
    cursor = db.conn.cursor()
    cursor.execute("SELECT league_key FROM leagues")
    leagues = cursor.fetchall()
    
    for (league_key,) in leagues:
        logger.info(f"Loading teams for league {league_key}")
        try:
            # Get league teams from Yahoo API
            response = yahoo_api.make_request(f'/league/{league_key}/teams')
            
            if not response or 'fantasy_content' not in response:
                logger.error(f"No fantasy content in response for league {league_key}")
                continue
                
            league_data = response['fantasy_content'].get('league', [])
            if not league_data or len(league_data) < 2:
                logger.error(f"Invalid league data for {league_key}")
                continue
            
            teams_container = league_data[1].get('teams', {})
            teams_count = int(teams_container.get('count', 0))
            
            logger.info(f"Found {teams_count} teams in league {league_key}")
            
            # Process each team in the league
            for i in range(teams_count):
                team_idx = str(i)
                if team_idx not in teams_container:
                    continue
                    
                team_data = teams_container[team_idx].get('team', [])
                if not team_data or len(team_data) == 0:
                    continue
                    
                # Get the first element which contains team info
                team_info_list = team_data[0]
                
                # Extract team logo if available
                team_logos = next((item.get('team_logos', []) for item in team_info_list if isinstance(item, dict) and 'team_logos' in item), [])
                logo_url = team_logos[0].get('team_logo', {}).get('url') if team_logos else None
                
                # Extract manager info if available
                managers_data = next((item.get('managers', []) for item in team_info_list if isinstance(item, dict) and 'managers' in item), [])
                manager = managers_data[0].get('manager', {}) if managers_data else {}
                
                # Extract other team info
                team_key = next((item.get('team_key') for item in team_info_list if isinstance(item, dict) and 'team_key' in item), None)
                team_id = next((item.get('team_id') for item in team_info_list if isinstance(item, dict) and 'team_id' in item), None)
                name = next((item.get('name') for item in team_info_list if isinstance(item, dict) and 'name' in item), None)
                url = next((item.get('url') for item in team_info_list if isinstance(item, dict) and 'url' in item), None)
                
                # Extract stats and metadata
                waiver_priority = next((item.get('waiver_priority') for item in team_info_list if isinstance(item, dict) and 'waiver_priority' in item), None)
                faab_balance = next((item.get('faab_balance') for item in team_info_list if isinstance(item, dict) and 'faab_balance' in item), None)
                number_of_moves = next((item.get('number_of_moves') for item in team_info_list if isinstance(item, dict) and 'number_of_moves' in item), None)
                number_of_trades = next((item.get('number_of_trades') for item in team_info_list if isinstance(item, dict) and 'number_of_trades' in item), None)
                clinched_playoffs = next((item.get('clinched_playoffs') for item in team_info_list if isinstance(item, dict) and 'clinched_playoffs' in item), None)
                
                # Get draft info if available
                draft_info = next((item for item in team_info_list if isinstance(item, dict) and 'has_draft_grade' in item), {})
                
                # Get sport code from league key
                sport_code = league_key.split('.')[0]
                
                # Prepare team data for database
                team_info = {
                    'team_key': team_key,
                    'sport_code': sport_code,
                    'name': name,
                    'logo_url': logo_url,
                    'stats': {
                        'waiver_priority': waiver_priority,
                        'faab_balance': faab_balance,
                        'number_of_moves': number_of_moves,
                        'number_of_trades': number_of_trades,
                        'clinched_playoffs': clinched_playoffs,
                        'draft_grade': draft_info.get('draft_grade'),
                        'draft_recap_url': draft_info.get('draft_recap_url'),
                        'manager': {
                            'manager_id': manager.get('manager_id'),
                            'nickname': manager.get('nickname'),
                            'guid': manager.get('guid'),
                            'felo_score': manager.get('felo_score'),
                            'felo_tier': manager.get('felo_tier')
                        }
                    }
                }
                
                try:
                    db.save_team(team_info)
                    processed_teams.add(team_key)
                    logger.info(f"Saved team: {team_info['name']} (Sport: {team_info['sport_code']})")
                except Exception as e:
                    logger.error(f"Failed to save team: {str(e)}")
                
            # Sleep briefly to avoid rate limits
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing league {league_key}: {str(e)}")
            continue
    
    return processed_teams

def load_team_data():
    """Load team data from Yahoo Fantasy API into the database"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.access_token:
            logger.info("No access token available, starting authorization flow...")
            auth_url = yahoo_api.get_authorization_url()
            logger.info(f"Please visit this URL to authorize the application: {auth_url}")
            auth_code = input("Enter the authorization code: ")
            if yahoo_api.handle_authorization(auth_code):
                logger.info("Successfully authorized and obtained tokens")
            else:
                logger.error("Failed to authorize")
                return
        logger.info("Using access token")

        # Initialize database with the correct path
        db = Database(db_path='fantasy_data.db')

        # Initialize DataManager with Yahoo API and database
        data_manager = DataManager(yahoo_api=yahoo_api, use_db=True, db_path='fantasy_data.db')

        logger.info("Starting teams data fetch...")
        
        # First load teams from user's games
        response = yahoo_api.make_request('/users;use_login=1/games/teams')
        logger.debug(f"Raw API response: {response}")
        
        if not response or 'fantasy_content' not in response:
            logger.error("No fantasy content in response")
            return
            
        users = response['fantasy_content'].get('users', {})
        if '0' not in users:
            logger.error("No user data found")
            return
            
        user_data = users['0'].get('user', [])
        if len(user_data) < 2:
            logger.error("Incomplete user data")
            return
            
        games = user_data[1].get('games', {})
        games_count = int(games.get('count', 0))
        logger.info(f"Found {games_count} games with teams")
        
        # Process teams from each game
        processed_teams = set()
        total_teams = 0
        
        for i in range(games_count):
            idx = str(i)
            if idx not in games:
                logger.debug(f"Skipping game index {idx} - not found in games")
                continue
                
            game_data = games[idx].get('game', [])
            if not game_data:
                logger.debug(f"Skipping game index {idx} - no game data")
                continue
                
            logger.debug(f"Game data for index {idx}: {game_data}")
            
            # Handle both list and dict formats for game data
            if isinstance(game_data, list):
                if len(game_data) == 0:
                    logger.debug(f"Skipping game index {idx} - empty game data list")
                    continue
                game = game_data[0]
                teams_container = game_data[1].get('teams', {}) if len(game_data) > 1 else {}
            else:
                game = game_data
                teams_container = {}
            
            if not teams_container:
                logger.debug(f"No teams found for game in index {idx}")
                continue
                
            teams_count = int(teams_container.get('count', 0))
            total_teams += teams_count
            
            sport_code = game.get('code')
            season = game.get('season')
            
            logger.info(f"Processing {teams_count} teams for {sport_code} season {season}")
            
            # Process each team in the game
            for j in range(teams_count):
                team_idx = str(j)
                if team_idx not in teams_container:
                    logger.debug(f"Skipping team index {team_idx} - not found in teams container")
                    continue
                    
                team_data = teams_container[team_idx].get('team', [])
                if not team_data or len(team_data) == 0:
                    logger.debug(f"Skipping team index {team_idx} - no team data")
                    continue
                    
                # Get the first element which contains team info
                team_info_list = team_data[0]
                
                # Extract team logo if available
                team_logos = next((item.get('team_logos', []) for item in team_info_list if isinstance(item, dict) and 'team_logos' in item), [])
                logo_url = team_logos[0].get('team_logo', {}).get('url') if team_logos else None
                
                # Extract manager info if available
                managers_data = next((item.get('managers', []) for item in team_info_list if isinstance(item, dict) and 'managers' in item), [])
                manager = managers_data[0].get('manager', {}) if managers_data else {}
                
                # Extract other team info
                team_key = next((item.get('team_key') for item in team_info_list if isinstance(item, dict) and 'team_key' in item), None)
                team_id = next((item.get('team_id') for item in team_info_list if isinstance(item, dict) and 'team_id' in item), None)
                name = next((item.get('name') for item in team_info_list if isinstance(item, dict) and 'name' in item), None)
                url = next((item.get('url') for item in team_info_list if isinstance(item, dict) and 'url' in item), None)
                
                # Extract stats and metadata
                waiver_priority = next((item.get('waiver_priority') for item in team_info_list if isinstance(item, dict) and 'waiver_priority' in item), None)
                faab_balance = next((item.get('faab_balance') for item in team_info_list if isinstance(item, dict) and 'faab_balance' in item), None)
                number_of_moves = next((item.get('number_of_moves') for item in team_info_list if isinstance(item, dict) and 'number_of_moves' in item), None)
                number_of_trades = next((item.get('number_of_trades') for item in team_info_list if isinstance(item, dict) and 'number_of_trades' in item), None)
                clinched_playoffs = next((item.get('clinched_playoffs') for item in team_info_list if isinstance(item, dict) and 'clinched_playoffs' in item), None)
                
                # Get draft info if available
                draft_info = next((item for item in team_info_list if isinstance(item, dict) and 'has_draft_grade' in item), {})
                
                # Prepare team data for database
                team_info = {
                    'team_key': team_key,
                    'sport_code': sport_code,
                    'name': name,
                    'logo_url': logo_url,
                    'stats': {
                        'waiver_priority': waiver_priority,
                        'faab_balance': faab_balance,
                        'number_of_moves': number_of_moves,
                        'number_of_trades': number_of_trades,
                        'clinched_playoffs': clinched_playoffs,
                        'draft_grade': draft_info.get('draft_grade'),
                        'draft_recap_url': draft_info.get('draft_recap_url'),
                        'manager': {
                            'manager_id': manager.get('manager_id'),
                            'nickname': manager.get('nickname'),
                            'guid': manager.get('guid'),
                            'felo_score': manager.get('felo_score'),
                            'felo_tier': manager.get('felo_tier')
                        }
                    }
                }
                
                try:
                    db.save_team(team_info)
                    processed_teams.add(team_key)
                    logger.info(f"Saved team: {team_info['name']} (Sport: {team_info['sport_code']})")
                except Exception as e:
                    logger.error(f"Failed to save team: {str(e)}")
                
        logger.info(f"Successfully processed {len(processed_teams)} out of {total_teams} teams from user's games")

        # Now load teams from all leagues in the database
        logger.info("Loading teams from leagues in database...")
        league_teams = load_teams_from_leagues(yahoo_api, db)
        processed_teams.update(league_teams)
        
        logger.info(f"Total unique teams processed: {len(processed_teams)}")

        # Verify teams in database
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM teams")
        count = cursor.fetchone()[0]
        logger.info(f"Teams in database: {count}")
        
        # Print team details
        cursor.execute("SELECT team_key, name, sport_code FROM teams")
        teams = cursor.fetchall()
        for team in teams:
            logger.info(f"Team: {team[1]} (Key: {team[0]}, Sport: {team[2]})")

    except Exception as e:
        logger.error(f"Error during teams data load: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_team_data() 