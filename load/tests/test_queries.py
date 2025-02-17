from database import Database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_player_queries():
    """Test various player queries"""
    try:
        # Initialize database connection
        db = Database()
        
        # 1. Get all NFL players
        logger.info("Getting all NFL players...")
        all_players = db.get_all_players(sport_code='nfl')
        logger.info(f"Found {len(all_players)} total players")

        # 2. Get injured players
        logger.info("\nGetting injured players...")
        injured_players = db.get_players_by_status(status='Injured', sport_code='nfl')
        logger.info(f"Found {len(injured_players)} injured players")
        for player in injured_players[:5]:  # Show first 5 injured players
            logger.info(f"- {player['name']} ({player['position']}): {player['injury_note']}")

        # 3. Get quarterbacks
        logger.info("\nGetting quarterbacks...")
        qbs = db.get_players_by_position(position='QB', sport_code='nfl')
        logger.info(f"Found {len(qbs)} quarterbacks")
        for qb in qbs[:5]:  # Show first 5 QBs
            logger.info(f"- {qb['name']} ({qb['team']})")

        # 4. Search for a player
        search_term = "Mahomes"
        logger.info(f"\nSearching for players with name containing '{search_term}'...")
        search_results = db.search_players_by_name(search_term, sport_code='nfl')
        logger.info(f"Found {len(search_results)} matching players")
        for player in search_results:
            logger.info(f"- {player['name']} ({player['position']}, {player['team']})")

        # 5. Get top players
        logger.info("\nGetting top 5 players by fantasy points...")
        top_players = db.get_top_players(sport_code='nfl', limit=5)
        for idx, player in enumerate(top_players, 1):
            logger.info(f"{idx}. {player['name']} ({player['position']}, {player['team']})")

    except Exception as e:
        logger.error(f"Error during query testing: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    test_player_queries() 