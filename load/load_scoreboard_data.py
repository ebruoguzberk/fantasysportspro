import logging
import sys
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

def process_matchup_data(matchup: Dict[str, Any], league_key: str, week: int) -> List[Dict[str, Any]]:
    """Process matchup data and extract team points"""
    points_data = []
    
    # Get teams data from the matchup
    teams = matchup.get('0', {}).get('teams', {})
    if not teams:
        logger.debug(f"No teams found in matchup for league {league_key}, week {week}")
        return points_data

    for team_idx in ['0', '1']:  # Each matchup has 2 teams
        if team_idx not in teams:
            continue
            
        team_data = teams[team_idx].get('team', [])
        if not team_data or len(team_data) < 2:
            continue

        # Extract team info and points
        team_info = team_data[0]
        team_points = team_data[1]

        # Get team key from the first element
        team_key = next((item.get('team_key') for item in team_info if isinstance(item, dict) and 'team_key' in item), None)
        if not team_key:
            continue

        # Extract points and projected points
        points = team_points.get('team_points', {}).get('total')
        projected_points = team_points.get('team_projected_points', {}).get('total')

        logger.debug(f"Extracted points for team {team_key}: actual={points}, projected={projected_points}")

        points_data.append({
            'league_key': league_key,
            'team_key': team_key,
            'week': week,
            'points': float(points) if points else None,
            'projected_points': float(projected_points) if projected_points else None
        })

    return points_data

def process_league_scoreboard(league_data: Dict[str, Any], db: Database) -> None:
    """Process league scoreboard data and save to database"""
    league_key = league_data[0].get('league_key')
    if not league_key:
        logger.warning("No league key found in league data")
        return

    scoreboard_data = league_data[1].get('scoreboard', {})
    if not scoreboard_data:
        logger.warning(f"No scoreboard data found for league {league_key}")
        return

    week = int(scoreboard_data.get('week', 0))
    if not week:
        logger.warning(f"No week information found for league {league_key}")
        return

    # Save raw scoreboard data
    logger.info(f"Saving raw scoreboard data for league {league_key}, week {week}")
    db.cursor.execute('''
        INSERT OR REPLACE INTO league_scoreboard 
        (league_key, week, scoreboard_json, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    ''', (league_key, week, str(scoreboard_data)))
    
    # Process matchups
    matchups = scoreboard_data.get('0', {}).get('matchups', {})
    if not matchups:
        logger.warning(f"No matchups found in scoreboard for league {league_key}, week {week}")
        return

    points_saved = 0
    for matchup_idx in range(int(matchups.get('count', 0))):
        matchup = matchups.get(str(matchup_idx), {}).get('matchup')
        if not matchup:
            continue

        # Process and save points data for each team in the matchup
        points_data_list = process_matchup_data(matchup, league_key, week)
        for points_data in points_data_list:
            try:
                logger.info(f"Saving points data for team {points_data['team_key']}: {points_data['points']} points")
                db.save_league_points(points_data)
                points_saved += 1
            except Exception as e:
                logger.error(f"Error saving points data for team {points_data['team_key']}: {str(e)}")

    logger.info(f"Saved {points_saved} team points records for league {league_key}, week {week}")

def load_scoreboard_data(start_week: int = 1, end_week: int = None):
    """Load scoreboard data from Yahoo Fantasy API into the database with pagination"""
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

        # Initialize database
        db = Database(db_path='fantasy_data.db')

        # Get all leagues first
        cursor = db.conn.cursor()
        cursor.execute("SELECT league_key, settings FROM leagues")
        leagues = cursor.fetchall()

        total_processed = 0
        batch_size = 5  # Process 5 leagues at a time

        for i in range(0, len(leagues), batch_size):
            batch_leagues = leagues[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} of {(len(leagues) + batch_size - 1)//batch_size}")

            for league_key, settings in batch_leagues:
                try:
                    # If end_week is not specified, try to get it from league settings
                    if end_week is None:
                        try:
                            league_settings = eval(settings)  # Convert string to dict
                            current_end_week = int(league_settings.get('end_week', 17))
                        except:
                            current_end_week = 17  # Default to 17 if can't get from settings
                    else:
                        current_end_week = end_week

                    # Process each week for the league
                    for week in range(start_week, current_end_week + 1):
                        logger.info(f"Fetching scoreboard for league {league_key}, week {week}")
                        
                        # Make API request for the specific week
                        response = yahoo_api.make_request(f'/league/{league_key}/scoreboard;week={week}')
                        
                        if not response or 'fantasy_content' not in response:
                            logger.error(f"No fantasy content in response for league {league_key}, week {week}")
                            continue

                        league_data = response['fantasy_content'].get('league', [])
                        if not league_data:
                            logger.error(f"No league data found for {league_key}, week {week}")
                            continue

                        process_league_scoreboard(league_data, db)
                        total_processed += 1
                        
                        logger.info(f"Processed scoreboard for league {league_key}, week {week}")

                except Exception as e:
                    logger.error(f"Error processing league {league_key}: {str(e)}")
                    continue

            # Commit after each batch
            db.conn.commit()
            logger.info(f"Committed batch {i//batch_size + 1}")

        logger.info(f"Successfully processed {total_processed} scoreboards")

        # Verify data in database
        cursor.execute("SELECT COUNT(*) FROM league_scoreboard")
        scoreboard_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM league_points")
        points_count = cursor.fetchone()[0]
        
        logger.info(f"Total records: {scoreboard_count} scoreboards, {points_count} point entries")

    except Exception as e:
        logger.error(f"Error during scoreboard data load: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    load_scoreboard_data() 