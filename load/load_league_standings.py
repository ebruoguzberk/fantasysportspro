import logging
import os
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

def load_league_standings():
    """Load league standings data from Yahoo Fantasy API into the database"""
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

        logger.info("Starting league standings data fetch...")
        
        # Get league standings from Yahoo API
        response = yahoo_api.make_request('/users;use_login=1/games/leagues/standings')
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
        logger.info(f"Found {games_count} games with standings")
        
        # Process standings from each game
        processed_standings = 0
        total_standings = 0
        
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
                leagues_container = game_data[1].get('leagues', {}) if len(game_data) > 1 else {}
            else:
                game = game_data
                leagues_container = {}
            
            if not leagues_container:
                logger.debug(f"No leagues found for game in index {idx}")
                continue
                
            leagues_count = int(leagues_container.get('count', 0))
            
            sport_code = game.get('code')
            season = game.get('season')
            
            logger.info(f"Processing standings for {leagues_count} leagues in {sport_code} season {season}")
            
            # Process each league's standings
            for j in range(leagues_count):
                league_idx = str(j)
                if league_idx not in leagues_container:
                    logger.debug(f"Skipping league index {league_idx} - not found in leagues container")
                    continue
                    
                league_data = leagues_container[league_idx].get('league', [])
                if not league_data or len(league_data) == 0:
                    logger.debug(f"Skipping league index {league_idx} - no league data")
                    continue
                
                # Get league info
                league_info = league_data[0] if isinstance(league_data[0], dict) else {}
                league_key = league_info.get('league_key')
                
                # Get standings data
                standings_data = next((item.get('standings', []) for item in league_data if isinstance(item, dict) and 'standings' in item), [])
                if not standings_data:
                    logger.debug(f"No standings data found for league {league_key}")
                    continue
                
                teams = standings_data[0].get('teams', {}) if standings_data else {}
                teams_count = int(teams.get('count', 0))
                total_standings += teams_count
                
                # Process each team's standings
                for k in range(teams_count):
                    team_idx = str(k)
                    if team_idx not in teams:
                        continue
                        
                    team_data = teams[team_idx].get('team', [])
                    if not team_data:
                        continue
                        
                    # Get team info
                    team_info = team_data[0] if isinstance(team_data[0], list) else []
                    team_key = next((item.get('team_key') for item in team_info if isinstance(item, dict) and 'team_key' in item), None)
                    team_name = next((item.get('name') for item in team_info if isinstance(item, dict) and 'name' in item), None)
                    
                    # Get points data
                    points_data = next((item.get('team_points', {}) for item in team_data if isinstance(item, dict) and 'team_points' in item), {})
                    
                    # Get standings data
                    standings_info = next((item.get('team_standings', {}) for item in team_data if isinstance(item, dict) and 'team_standings' in item), {})
                    
                    # Extract outcome totals
                    outcome_totals = standings_info.get('outcome_totals', {})
                    streak_info = standings_info.get('streak', {})
                    
                    # Prepare standings data for database
                    standings_entry = {
                        'league_key': league_key,
                        'team_key': team_key,
                        'team_name': team_name,
                        'rank': standings_info.get('rank'),
                        'playoff_seed': standings_info.get('playoff_seed'),
                        'wins': outcome_totals.get('wins'),
                        'losses': outcome_totals.get('losses'),
                        'ties': outcome_totals.get('ties'),
                        'percentage': outcome_totals.get('percentage'),
                        'points_for': standings_info.get('points_for'),
                        'points_against': standings_info.get('points_against'),
                        'streak_type': streak_info.get('type'),
                        'streak_value': streak_info.get('value'),
                        'season': season,
                        'sport_code': sport_code
                    }
                    
                    try:
                        db.save_league_standings(standings_entry)
                        processed_standings += 1
                        logger.info(f"Saved standings for team: {team_name} (Rank: {standings_info.get('rank')}, League: {league_key})")
                    except Exception as e:
                        logger.error(f"Failed to save standings: {str(e)}")
                
        logger.info(f"Successfully processed {processed_standings} out of {total_standings} standings entries")

        # Verify standings in database
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM league_standings")
        count = cursor.fetchone()[0]
        logger.info(f"Standings entries in database: {count}")
        
        # Print standings details
        cursor.execute("SELECT team_name, rank, wins, losses, points_for FROM league_standings ORDER BY rank")
        standings = cursor.fetchall()
        for standing in standings:
            logger.info(f"Team: {standing[0]} (Rank: {standing[1]}, Record: {standing[2]}-{standing[3]}, Points: {standing[4]})")

    except Exception as e:
        logger.error(f"Error during standings data load: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_league_standings() 