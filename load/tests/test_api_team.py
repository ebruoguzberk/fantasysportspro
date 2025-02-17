import logging
from yahoo_api import YahooFantasyAPI
from database import Database
import json

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_team_draft_data():
    """Test function to get and display draft data for a specific team"""
    try:
        # Initialize API
        yahoo_api = YahooFantasyAPI()
        
        # Team and league keys
        team_key = "101.l.166056.t.1"
        league_key = "101.l.166056"
        
        logger.info(f"Testing draft data for team_key: {team_key}")
        
        # Get draft results for the league
        draft_results = yahoo_api.get_draft_results(league_key)
        
        if draft_results and 'fantasy_content' in draft_results:
            league_data = draft_results['fantasy_content'].get('league', [])
            if len(league_data) > 1 and isinstance(league_data[1], dict):
                draft_data = league_data[1].get('draft_results', {})
                
                # Filter draft results for this team
                team_picks = {}
                for pick_num, pick in draft_data.items():
                    if isinstance(pick, dict) and 'draft_result' in pick:
                        result = pick['draft_result']
                        if result.get('team_key') == team_key:
                            team_picks[pick_num] = result
                
                logger.info("\nTeam's draft picks:")
                print(json.dumps(team_picks, indent=2))
                
                # Print summary
                logger.info(f"\nTotal picks for team: {len(team_picks)}")
                for pick_num, pick in team_picks.items():
                    logger.info(f"Round {pick['round']}, Pick {pick['pick']}: Player key {pick['player_key']}")
            
        else:
            logger.warning("No draft results found")
            
    except Exception as e:
        logger.error(f"Error testing team draft data: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    test_team_draft_data() 