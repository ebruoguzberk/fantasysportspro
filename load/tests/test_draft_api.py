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

def test_draft_results():
    """Test function to get and display raw draft results from Yahoo API"""
    try:
        # Initialize API and DB
        yahoo_api = YahooFantasyAPI()
        db = Database(db_path='fantasy_data.db')
        
        # Get one league key from the database
        cursor = db.conn.cursor()
        cursor.execute("SELECT league_key, name FROM leagues LIMIT 1")
        result = cursor.fetchone()
        
        if not result:
            logger.error("No leagues found in database")
            return
            
        league_key, league_name = result
        logger.info(f"Testing draft results for league: {league_name} ({league_key})")
        
        # Get draft results from API
        draft_data = yahoo_api.get_draft_results(league_key)
        
        # Pretty print the entire response
        logger.info("Raw draft results response:")
        print(json.dumps(draft_data, indent=2))
        
        # Extract and display specific parts
        if draft_data and 'fantasy_content' in draft_data:
            league_data = draft_data['fantasy_content'].get('league', [])
            if len(league_data) > 1 and isinstance(league_data[1], dict):
                draft_results = league_data[1].get('draft_results', {})
                logger.info("\nExtracted draft results:")
                print(json.dumps(draft_results, indent=2))
            else:
                logger.warning("No draft results found in league data")
        else:
            logger.warning("No fantasy content found in response")
            
    except Exception as e:
        logger.error(f"Error testing draft results: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    test_draft_results() 