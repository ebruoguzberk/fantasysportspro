import unittest
import os
import json
from dotenv import load_dotenv
import logging
from yahoo_api import YahooFantasyAPI
from data_manager import DataManager
from database import Database
from datetime import datetime, timedelta
 

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Also set yahoo_api logger to DEBUG
yahoo_logger = logging.getLogger('yahoo_api')
yahoo_logger.setLevel(logging.DEBUG)


class TestFantasyDashPro(unittest.TestCase):
    TOKEN_FILE = ".yahoo_tokens.json"
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        load_dotenv()
        cls.test_db_path = "test_fantasy_football.db"
        
        # Check for required environment variables
        required_vars = ["YAHOO_CONSUMER_KEY", "YAHOO_CONSUMER_SECRET"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Try to load existing tokens
        token_path = os.path.join(os.path.dirname(__file__), cls.TOKEN_FILE)
        try:
            with open(token_path) as f:
                cls.saved_tokens = json.load(f)
                logger.info("Loaded existing tokens from file")
        except (FileNotFoundError, json.JSONDecodeError):
            cls.saved_tokens = None
            logger.info("No existing tokens found - will need to authenticate")
            
        logger.info("Test environment setup complete")

    def setUp(self):
        class SessionState:
            def __init__(self, tokens=None):
                self.yahoo_tokens = tokens
                self.authenticated = bool(tokens)

        self.session_state = SessionState(self.__class__.saved_tokens)
        self.yahoo_api = YahooFantasyAPI(session_state=self.session_state)
        self.data_manager = DataManager(yahoo_api=self.yahoo_api, use_db=True, db_path=self.test_db_path)
        logger.info(f"Test setup complete. Authenticated: {self.session_state.authenticated}")

    def test_1_authentication_url(self):
        if self.session_state.authenticated:
            logger.info("Already authenticated - skipping authentication test")
            self.skipTest("Already authenticated")

        try:
            auth_url = self.yahoo_api.get_authorization_url()
            self.assertIsNotNone(auth_url)
            self.assertTrue(auth_url.startswith("https://api.login.yahoo.com/oauth2/request_auth"))
            self.assertTrue("scope=openid+fspt-r" in auth_url)
            logger.info("✅ Authentication URL test passed")

            print("\n🔑 Authentication Required:")
            print("1. Visit this URL:", auth_url)
            print("2. Log in to Yahoo and authorize the application")
            print("3. Copy the authorization code")
            auth_code = input("\nEnter the authorization code: ").strip()

            if not auth_code:
                self.fail("No authorization code provided")

            result = self.yahoo_api.handle_authorization(auth_code)
            self.assertTrue(result, "Authorization failed")
            self.assertIsNotNone(self.session_state.yahoo_tokens)
            logger.info("✅ Authentication test passed")

            self.__class__.saved_tokens = self.session_state.yahoo_tokens
            token_path = os.path.join(os.path.dirname(__file__), self.__class__.TOKEN_FILE)

            tokens_to_save = self.__class__.saved_tokens.copy()
            if isinstance(tokens_to_save.get('token_expiry'), datetime):
                tokens_to_save['token_expiry'] = tokens_to_save['token_expiry'].isoformat()

            with open(token_path, 'w') as f:
                json.dump(tokens_to_save, f)
            logger.info(f"Saved tokens to: {token_path}")

        except Exception as e:
            logger.error(f"Authentication test failed: {str(e)}", exc_info=True)
            raise

    def test_2_database_initialization(self):
        db = Database(db_path=self.test_db_path)
        status = db.test_connection()
        self.assertEqual(status["status"], "connected")
        self.assertGreater(len(status["tables"]), 0)
        logger.info("✅ Database initialization test passed")

    def test_3_load_all_endpoints(self):
        """Test loading data from all endpoints."""
        # Test loading games
        games_data = self.data_manager.update_data('games')
        self.assertIsNotNone(games_data)
        logging.info(f"Found {len(games_data)} games")
        
        # Test loading players with detailed logging
        logging.info("Making request to Yahoo API for players...")
        raw_response = self.yahoo_api.make_request('/games;game_keys=nfl/players;start=0;count=25')
        logging.info("Raw API Response:")
        logging.info(json.dumps(raw_response, indent=2))
        
        players_data = self.data_manager.update_data('players')
        logging.info("Processed players data:")
        logging.info(json.dumps(players_data, indent=2))
        
        if isinstance(players_data, dict) and 'game' in players_data:
            game_data = players_data['game']
            if isinstance(game_data, list) and len(game_data) > 1:
                players_section = game_data[1]
                logging.info("Players section:")
                logging.info(json.dumps(players_section, indent=2))

    def tearDown(self):
        if hasattr(self, 'data_manager') and hasattr(self.data_manager, 'db'):
            self.data_manager.db.close()

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        # Remove test database if it exists
        try:
            if os.path.exists(cls.test_db_path):
                os.remove(cls.test_db_path)
                logger.info(f"Removed test database: {cls.test_db_path}")
        except Exception as e:
            logger.error(f"Failed to clean up test database: {str(e)}")

if __name__ == '__main__':
    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create test class and load tokens
    TestFantasyDashPro.setUpClass()
    
    # Create and run the test
    test = TestFantasyDashPro('test_3_load_all_endpoints')
    test.setUp()
    try:
        test.test_3_load_all_endpoints()
    finally:
        test.tearDown()
        TestFantasyDashPro.tearDownClass()