import logging
import os
from yahoo_api import YahooFantasyAPI
from dotenv import load_dotenv
import json
import time
from database import Database
from load_players_stats_data import extract_player_stats, save_player_stats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Allow OAuth2 over HTTP for development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

def test_api_connection():
    """Test Yahoo API connection using existing token"""
    try:
        yahoo_api = YahooFantasyAPI()
        
        # Test with a known league and player
        league_key = "423.l.137347"  # Use an existing league
        player_key = "423.p.30977"   # Use an existing player
        week = 1
        
        # Make API request
        endpoint = f'{yahoo_api.BASE_URL}/league/{league_key}/players;player_keys={player_key}/stats;type=week;week={week};stats.details=1?format=json'
        response = yahoo_api.make_request(endpoint)
        
        if response and 'fantasy_content' in response:
            logger.info("✅ API connection successful")
            # Log sample of response structure
            logger.info("Sample response structure:")
            logger.info(json.dumps(response['fantasy_content']['league'][1]['players']['0'], indent=2))
            return True
        else:
            logger.error("❌ Invalid API response format")
            return False
            
    except Exception as e:
        logger.error(f"API connection test failed: {str(e)}")
        return False

def test_data_extraction():
    """Test data extraction with real API response"""
    try:
        yahoo_api = YahooFantasyAPI()
        
        # Test with multiple players from the image
        test_players = [
            {
                'name': 'Jared Goff',
                'key': '449.p.29235',
                'team': 'Det',
                'position': 'QB',
                'game_info': {
                    'final_score': 'Final W 42-29',
                    'opponent': 'vs Sea',
                    'bye_week': 5
                },
                'expected': {
                    'position': 'QB',
                    'fan_pts': 27.18,
                    'proj_pts': 16.61,
                    'start_pct': 31,
                    'ros_pct': 83,
                    'passing_yards': 292,
                    'passing_td': 2,
                    'interceptions': 0,
                    'rushing_yards': -2,
                    'rushing_td': 0,
                    'receptions': 1,
                    'receiving_yards': 7,
                    'receiving_td': 1,
                    'targets': 1,
                    'two_pt': 0
                }
            },
            {
                'name': 'Garrett Wilson',
                'key': '449.p.33965',
                'team': 'NYJ',
                'position': 'WR',
                'game_info': {
                    'final_score': 'Final L 9-10',
                    'opponent': 'vs Den',
                    'bye_week': 12
                },
                'expected': {
                    'position': 'WR',
                    'fan_pts': 7.10,
                    'proj_pts': 14.29,
                    'start_pct': 90,
                    'ros_pct': 100,
                    'passing_yards': 0,
                    'passing_td': 0,
                    'interceptions': 0,
                    'rushing_yards': 0,
                    'rushing_td': 0,
                    'receptions': 5,
                    'receiving_yards': 41,
                    'receiving_td': 0,
                    'targets': 8,
                    'two_pt': 0
                }
            },
            {
                'name': 'DK Metcalf',
                'key': '449.p.31896',
                'team': 'Sea',
                'position': 'WR',
                'game_info': {
                    'final_score': 'Final L 29-42',
                    'opponent': '@ Det',
                    'bye_week': 10
                },
                'expected': {
                    'position': 'WR',
                    'fan_pts': 15.40,
                    'proj_pts': 13.35,
                    'start_pct': 91,
                    'ros_pct': 98,
                    'passing_yards': 0,
                    'passing_td': 0,
                    'interceptions': 0,
                    'rushing_yards': 0,
                    'rushing_td': 0,
                    'receptions': 7,
                    'receiving_yards': 104,
                    'receiving_td': 0,
                    'targets': 12,
                    'two_pt': 0
                }
            }
        ]
        
        league_key = "449.l.343734"  # "Stay Classy Roethlisberger" league from image
        week = 4  # Week shown in the image
        
        for player in test_players:
            logger.info(f"\nTesting stats for {player['name']} ({player['team']} {player['position']})...")
            logger.info(f"Game: {player['game_info']['final_score']} {player['game_info']['opponent']}")
            logger.info(f"Bye Week: {player['game_info']['bye_week']}")
            
            # Make API requests
            stats_endpoint = f'{yahoo_api.BASE_URL}/league/{league_key}/players;player_keys={player["key"]}/stats;type=week;week={week}?format=json'
            projected_endpoint = f'{yahoo_api.BASE_URL}/league/{league_key}/players;player_keys={player["key"]}/stats;type=week;week={week};is_projected=1?format=json'
            
            stats_response = yahoo_api.make_request(stats_endpoint)
            projected_response = yahoo_api.make_request(projected_endpoint)
            
            if not stats_response or 'fantasy_content' not in stats_response:
                logger.error(f"Failed to get stats API response for {player['name']}")
                continue
                
            # Log full API response structures
            logger.info("\nFull stats API response structure:")
            logger.info(json.dumps(stats_response['fantasy_content'], indent=2))
            
            if projected_response and 'fantasy_content' in projected_response:
                logger.info("\nFull projected stats API response structure:")
                logger.info(json.dumps(projected_response['fantasy_content'], indent=2))
            else:
                logger.info("\nNo projected stats response available")
            
            # Extract player data
            league_data = stats_response['fantasy_content']['league']
            if len(league_data) > 1 and 'players' in league_data[1]:
                players_stats = league_data[1]['players']
                if '0' in players_stats:
                    player_data = players_stats['0'].get('player', [])
                    
                    # Extract stats
                    stats = extract_player_stats(player_data, week, league_key)
                    raw_stats = stats['stats']
                    
                    # Log raw API response for debugging
                    logger.info("\nRaw API response:")
                    logger.info(json.dumps(player_data[1], indent=2))
                    
                    # Log extracted data with detailed comparison
                    logger.info(f"\nStats for {player['name']}:")
                    logger.info(f"Position: {stats['position']} (Expected: {player['expected']['position']})")
                    
                    # Format stats based on position
                    if player['position'] == 'QB':
                        logger.info(f"Att*: {raw_stats.get('19', 0)}")  # passing attempts
                        logger.info(f"Passing: {raw_stats.get('4', 0)} yds, {raw_stats.get('5', 0)} TD, {raw_stats.get('6', 0)} INT")
                        if int(raw_stats.get('11', 0)) > 0:  # Only show receiving if there are receptions
                            logger.info(f"Receiving: {raw_stats.get('11', 0)} rec, {raw_stats.get('12', 0)} yds, {raw_stats.get('13', 0)} TD")
                    else:  # WR
                        logger.info(f"Tgt: {raw_stats.get('78', 0)}")
                        logger.info(f"Receiving: {raw_stats.get('11', 0)} rec, {raw_stats.get('12', 0)} yds, {raw_stats.get('13', 0)} TD")
                    
                    logger.info(f"Fan Pts: {stats['points']} (Expected: {player['expected']['fan_pts']})")
                    logger.info(f"Proj Pts: {stats['projected_points']} (Expected: {player['expected']['proj_pts']})")
                    logger.info(f"Start%: {player['expected']['start_pct']}%")
                    logger.info(f"Ros%: {player['expected']['ros_pct']}%")
                    
                    # Save to database
                    db = Database()
                    save_player_stats(db, player['key'], stats, league_key)
                    
                    # Verify database save
                    cursor = db.conn.cursor()
                    cursor.execute("""
                        SELECT 
                            season, position, points, projected_points,
                            passing_yards, passing_touchdowns, passing_interceptions,
                            rushing_yards, rushing_touchdowns,
                            receptions, receiving_yards, receiving_touchdowns,
                            targets, passing_2pt_conversions, rushing_2pt_conversions, receiving_2pt_conversions
                        FROM player_stats
                        WHERE player_key = ? AND league_key = ? AND week = ?
                    """, (player['key'], league_key, week))
                    
                    row = cursor.fetchone()
                    if row:
                        logger.info("\nVerified Database Save:")
                        logger.info(f"Season: {row[0]}")
                        logger.info(f"Position: {row[1]}")
                        logger.info(f"Fantasy Points: {row[2]}")
                        logger.info(f"Projected Points: {row[3]}")
                        logger.info(f"Passing: {row[4]} yds, {row[5]} TD, {row[6]} INT")
                        logger.info(f"Rushing: {row[7]} yds, {row[8]} TD")
                        logger.info(f"Receiving: {row[9]} rec, {row[10]} yds, {row[11]} TD")
                        logger.info(f"Targets: {row[12]}")
                        logger.info(f"2PT: {row[13]} pass, {row[14]} rush, {row[15]} rec")
                    else:
                        logger.error(f"Failed to verify database save for {player['name']}")
                    
                    db.close()
    except Exception as e:
        logger.error(f"Data extraction test failed: {str(e)}")

if __name__ == "__main__":
    logger.info("Testing Yahoo Fantasy API integration...")
    
    if test_api_connection():
        logger.info("\nTesting data extraction and storage...")
        test_data_extraction()
    else:
        logger.error("Skipping data extraction test due to API connection failure") 