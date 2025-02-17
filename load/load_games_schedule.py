import logging
import time
import random
from datetime import datetime
from yahoo_api import YahooFantasyAPI
from dotenv import load_dotenv
from database import Database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize database
db = Database(db_path='fantasy_data.db')

def load_league_schedules(yahoo_api: YahooFantasyAPI, db: Database, league_key: str, league_name: str):
    """Load schedule data for a specific league"""
    try:
        logger.info(f"Loading schedule for league: {league_name} ({league_key})")
        
        # Get league info to determine start and end weeks
        league_data = yahoo_api.get_league(league_key)
        if not league_data or 'fantasy_content' not in league_data:
            logger.error("No league data found")
            return
            
        league_info = league_data['fantasy_content']['league'][0]
        start_week = int(league_info.get('start_week', 1))
        end_week = int(league_info.get('end_week', 17))
        
        # Process each week
        for week in range(start_week, end_week + 1):
            logger.info(f"Loading schedule for week {week}")
            
            # Get scoreboard data for the week
            schedule_data = yahoo_api.get_league_games(league_key, week)
            
            if schedule_data and 'fantasy_content' in schedule_data:
                league_data = schedule_data['fantasy_content'].get('league', [])
                if len(league_data) > 1 and isinstance(league_data[1], dict):
                    scoreboard = league_data[1].get('scoreboard', {})
                    matchups = scoreboard.get('0', {}).get('matchups', {})
                    
                    if isinstance(matchups, dict):
                        for matchup_idx in range(int(matchups.get('count', 0))):
                            matchup = matchups.get(str(matchup_idx), {}).get('matchup', {})
                            if matchup:
                                # Debug logging
                                logger.info("\n=== MATCHUP DATA ===")
                                logger.info(f"Week: {matchup.get('week')}")
                                logger.info(f"Status: {matchup.get('status')}")
                                logger.info(f"Week Start: {matchup.get('week_start')}")
                                logger.info(f"Week End: {matchup.get('week_end')}")
                                logger.info(f"Is Playoffs: {matchup.get('is_playoffs')}")
                                logger.info(f"Is Consolation: {matchup.get('is_consolation')}")
                                logger.info(f"Is Tied: {matchup.get('is_tied')}")
                                logger.info(f"Winner Team Key: {matchup.get('winner_team_key')}")
                                logger.info(f"Matchup Recap Title: {matchup.get('matchup_recap_title')}")
                                logger.info("===================\n")
                                
                                teams = matchup.get('0', {}).get('teams', {})
                                if teams and int(teams.get('count', 0)) == 2:
                                    # Get matchup metadata
                                    status = matchup.get('status')
                                    week_start = matchup.get('week_start')
                                    week_end = matchup.get('week_end')
                                    is_playoffs = 1 if matchup.get('is_playoffs') == '1' else 0
                                    is_consolation = 1 if matchup.get('is_consolation') == '1' else 0
                                    is_tied = 1 if matchup.get('is_tied') == '1' else 0
                                    winner_team_key = matchup.get('winner_team_key')
                                    matchup_recap_title = matchup.get('matchup_recap_title')
                                    
                                    # Get team data
                                    team1_data = teams.get('0', {}).get('team', [])
                                    team2_data = teams.get('1', {}).get('team', [])
                                    
                                    # Extract team keys and metadata
                                    team1_key = None
                                    team2_key = None
                                    team1_manager = None
                                    team2_manager = None
                                    team1_points = 0
                                    team2_points = 0
                                    team1_projected = 0
                                    team2_projected = 0
                                    team1_division = None
                                    team2_division = None
                                    team1_clinched = 0
                                    team2_clinched = 0
                                    
                                    # Process team 1
                                    if team1_data:
                                        for item in team1_data[0]:
                                            if isinstance(item, dict):
                                                if 'team_key' in item:
                                                    team1_key = item['team_key']
                                                elif 'managers' in item:
                                                    team1_manager = item['managers'][0]['manager'].get('nickname')
                                                elif 'division_id' in item:
                                                    team1_division = item['division_id']
                                                elif 'clinched_playoffs' in item:
                                                    team1_clinched = 1 if item['clinched_playoffs'] == '1' else 0
                                        if len(team1_data) > 1:
                                            team1_points = float(team1_data[1].get('team_points', {}).get('total', 0))
                                            team1_projected = float(team1_data[1].get('team_projected_points', {}).get('total', 0))
                                    
                                    # Process team 2
                                    if team2_data:
                                        for item in team2_data[0]:
                                            if isinstance(item, dict):
                                                if 'team_key' in item:
                                                    team2_key = item['team_key']
                                                elif 'managers' in item:
                                                    team2_manager = item['managers'][0]['manager'].get('nickname')
                                                elif 'division_id' in item:
                                                    team2_division = item['division_id']
                                                elif 'clinched_playoffs' in item:
                                                    team2_clinched = 1 if item['clinched_playoffs'] == '1' else 0
                                        if len(team2_data) > 1:
                                            team2_points = float(team2_data[1].get('team_points', {}).get('total', 0))
                                            team2_projected = float(team2_data[1].get('team_projected_points', {}).get('total', 0))
                                    
                                    if team1_key and team2_key:
                                        # Save game data
                                        game_data = {
                                            'league_key': league_key,
                                            'week': week,
                                            'home_team_key': team1_key,
                                            'away_team_key': team2_key,
                                            'home_team_points': team1_points,
                                            'away_team_points': team2_points,
                                            'home_team_projected_points': team1_projected,
                                            'away_team_projected_points': team2_projected,
                                            'status': status,
                                            'game_start_time': datetime.strptime(week_start, '%Y-%m-%d').date() if week_start else None,
                                            'is_playoffs': is_playoffs,
                                            'is_consolation': is_consolation,
                                            'is_tied': is_tied,
                                            'winner_team_key': winner_team_key,
                                            'matchup_recap_title': matchup_recap_title,
                                            'home_team_manager': team1_manager,
                                            'away_team_manager': team2_manager
                                        }
                                        
                                        try:
                                            db.save_league_game(game_data)
                                            logger.info(f"Saved game data for week {week}")
                                        except Exception as e:
                                            logger.error(f"Error saving game data: {str(e)}")
                                        
                                        # Save home team schedule entry
                                        home_entry = {
                                            'league_key': league_key,
                                            'week': week,
                                            'team_key': team1_key,
                                            'opponent_team_key': team2_key,
                                            'is_home': 1,
                                            'week_start': week_start,
                                            'week_end': week_end,
                                            'is_playoffs': is_playoffs,
                                            'is_consolation': is_consolation,
                                            'is_tied': is_tied,
                                            'status': status,
                                            'points': team1_points,
                                            'projected_points': team1_projected,
                                            'is_winner': 1 if winner_team_key == team1_key else 0,
                                            'manager_name': team1_manager,
                                            'opponent_manager_name': team2_manager,
                                            'matchup_recap_title': matchup_recap_title,
                                            'division_id': team1_division,
                                            'opponent_division_id': team2_division,
                                            'clinched_playoffs': team1_clinched,
                                            'opponent_clinched_playoffs': team2_clinched
                                        }
                                        
                                        # Save away team schedule entry
                                        away_entry = {
                                            'league_key': league_key,
                                            'week': week,
                                            'team_key': team2_key,
                                            'opponent_team_key': team1_key,
                                            'is_home': 0,
                                            'week_start': week_start,
                                            'week_end': week_end,
                                            'is_playoffs': is_playoffs,
                                            'is_consolation': is_consolation,
                                            'is_tied': is_tied,
                                            'status': status,
                                            'points': team2_points,
                                            'projected_points': team2_projected,
                                            'is_winner': 1 if winner_team_key == team2_key else 0,
                                            'manager_name': team2_manager,
                                            'opponent_manager_name': team1_manager,
                                            'matchup_recap_title': matchup_recap_title,
                                            'division_id': team2_division,
                                            'opponent_division_id': team1_division,
                                            'clinched_playoffs': team2_clinched,
                                            'opponent_clinched_playoffs': team1_clinched
                                        }
                                        
                                        try:
                                            db.save_league_schedule(home_entry)
                                            db.save_league_schedule(away_entry)
                                            logger.info(f"Saved schedule entries for week {week}")
                                        except Exception as e:
                                            logger.error(f"Error saving schedule entries: {str(e)}")
            
            # Add a small delay between requests to avoid rate limiting
            time.sleep(1)
    
    except Exception as e:
        logger.error(f"Error loading schedule for league {league_key}: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)

def load_games_schedule():
    """Load league games and schedule data from Yahoo Fantasy API into database"""
    try:
        # Initialize Yahoo API
        yahoo_api = YahooFantasyAPI()
        if not yahoo_api.token:
            logger.error("No token available")
            return
            
        logger.info("Starting league games and schedule load...")
        
        # Get all leagues from database
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT league_key, name 
            FROM leagues
        """)
        leagues = cursor.fetchall()
        
        if not leagues:
            logger.error("No leagues found in database")
            return
            
        logger.info(f"Found {len(leagues)} leagues to process")
        
        processed_games = 0
        
        for league_key, league_name in leagues:
            logger.info(f"Processing games for league: {league_name} ({league_key})")
            
            # Load schedule data first
            load_league_schedules(yahoo_api, db, league_key, league_name)
            
            # Process each week from 1 to 17
            for week in range(1, 18):
                try:
                    logger.info(f"Processing week {week}")
                    # Get league games for specific week
                    games_data = yahoo_api.get_league_games(league_key, week)
                    if games_data and 'fantasy_content' in games_data:
                        league_info = games_data['fantasy_content'].get('league', [])
                        if len(league_info) > 1 and isinstance(league_info[1], dict):
                            scoreboard = league_info[1].get('scoreboard', {})
                            matchups = scoreboard.get('0', {}).get('matchups', {})
                            if isinstance(matchups, dict):
                                for matchup_idx in range(int(matchups.get('count', 0))):
                                    matchup_data = matchups.get(str(matchup_idx), {}).get('matchup', {})
                                    if matchup_data:
                                        week = matchup_data.get('week')
                                        status = matchup_data.get('status')
                                        teams_data = matchup_data.get('0', {}).get('teams', {})
                                        
                                        if teams_data and int(teams_data.get('count', 0)) == 2:
                                            # Get team data
                                            team1_data = teams_data.get('0', {}).get('team', [[]])[0]
                                            team2_data = teams_data.get('1', {}).get('team', [[]])[0]
                                            
                                            # Get team points
                                            team1_points = teams_data.get('0', {}).get('team', [None, {}])[1].get('team_points', {}).get('total')
                                            team2_points = teams_data.get('1', {}).get('team', [None, {}])[1].get('team_points', {}).get('total')
                                            
                                            # Get team keys
                                            team1_key = next((item.get('team_key') for item in team1_data if isinstance(item, dict) and 'team_key' in item), None)
                                            team2_key = next((item.get('team_key') for item in team2_data if isinstance(item, dict) and 'team_key' in item), None)
                                            
                                            if team1_key and team2_key:
                                                game_data = {
                                                    'league_key': league_key,
                                                    'week': week,
                                                    'home_team_key': team1_key,
                                                    'away_team_key': team2_key,
                                                    'home_team_points': float(team1_points) if team1_points else None,
                                                    'away_team_points': float(team2_points) if team2_points else None,
                                                    'status': status,
                                                    'game_start_time': datetime.strptime(matchup_data.get('week_start', ''), '%Y-%m-%d').date() if matchup_data.get('week_start') else None
                                                }
                                                
                                                try:
                                                    db.save_league_game(game_data)
                                                    processed_games += 1
                                                    logger.info(f"Saved game for week {week}")
                                                except Exception as e:
                                                    logger.error(f"Error saving game: {str(e)}")
                    
                    # Add a small delay between requests to avoid rate limiting
                    time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error processing league {league_key} week {week}: {str(e)}")
                    continue
        
        logger.info(f"League games load complete:")
        logger.info(f"Processed {processed_games} games")
        
        # Verify data in database
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM league_games")
        games_count = cursor.fetchone()[0]
        
        logger.info(f"Final database counts:")
        logger.info(f"Games in database: {games_count}")
        
        if games_count > 0:
            # Show some sample games
            cursor.execute("""
                SELECT lg.week, lg.home_team_points, lg.away_team_points, 
                       ht.name as home_team, at.name as away_team
                FROM league_games lg
                JOIN teams ht ON lg.home_team_key = ht.team_key
                JOIN teams at ON lg.away_team_key = at.team_key
                ORDER BY lg.week DESC
                LIMIT 5
            """)
            sample_games = cursor.fetchall()
            logger.info("\nSample games:")
            for game in sample_games:
                logger.info(f"Week {game[0]}: {game[3]} ({game[1]}) vs {game[4]} ({game[2]})")
        
    except Exception as e:
        logger.error(f"Error in load_games_schedule: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    try:
        load_games_schedule()
    except Exception as e:
        logger.error(f"Script error: {str(e)}") 