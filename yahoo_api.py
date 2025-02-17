import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, List, Any
import json
import base64
from urllib.parse import urlencode
import time
from functools import wraps
from requests_oauthlib import OAuth2Session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rate_limit(calls: int = 30, period: int = 60):
    """Rate limiting decorator"""
    def decorator(func):
        last_reset = datetime.now()
        calls_made = 0

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_reset, calls_made

            now = datetime.now()
            time_passed = (now - last_reset).total_seconds()

            if time_passed > period:
                calls_made = 0
                last_reset = now

            if calls_made >= calls:
                sleep_time = period - time_passed
                if sleep_time > 0:
                    logger.warning(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                    last_reset = datetime.now()
                    calls_made = 0

            calls_made += 1
            return func(*args, **kwargs)
        return wrapper
    return decorator

class YahooFantasyAPI:
    """Interface for the Yahoo Fantasy Sports API."""
    
    def __init__(self):
        """Initialize the API client."""
        # Load environment variables
        load_dotenv()
        
        # Check for both CLIENT and CONSUMER environment variables
        self.client_id = os.getenv('YAHOO_CLIENT_ID') or os.getenv('YAHOO_CONSUMER_KEY')
        self.client_secret = os.getenv('YAHOO_CLIENT_SECRET') or os.getenv('YAHOO_CONSUMER_SECRET')
        
        if not self.client_id or not self.client_secret:
            raise ValueError("Missing Yahoo API credentials. Please set either YAHOO_CLIENT_ID/YAHOO_CLIENT_SECRET or YAHOO_CONSUMER_KEY/YAHOO_CONSUMER_SECRET environment variables")
            
        self.token = self._load_token()
        self.session = OAuth2Session(
            self.client_id,
            token=self.token,
            auto_refresh_url='https://api.login.yahoo.com/oauth2/get_token',
            auto_refresh_kwargs={
                'client_id': self.client_id,
                'client_secret': self.client_secret
            },
            token_updater=self._save_token
        )
    
    def _load_token(self) -> Dict:
        """Load OAuth token from file."""
        try:
            with open('.yahoo_tokens.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("No token file found")
            return {}
        except json.JSONDecodeError:
            logger.error("Invalid token file")
            return {}
    
    def _save_token(self, token: Dict) -> None:
        """Save OAuth token to file."""
        try:
            with open('.yahoo_tokens.json', 'w') as f:
                json.dump(token, f)
        except Exception as e:
            logger.error(f"Error saving token: {str(e)}")
    
    def make_request(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make an authenticated request to the Yahoo Fantasy API."""
        try:
            # Ensure the URL starts with BASE_URL
            if not url.startswith('http'):
                url = f"{self.BASE_URL}{url}"
            
            # Try to make the request
            response = self.session.get(url, params=params)
            
            # If unauthorized, try refreshing the token
            if response.status_code == 401:
                logger.info("Received 401 response, attempting to refresh token...")
                self._refresh_access_token()
                # Retry the request with the new token
                response = self.session.get(url, params=params)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error occurred: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding response: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise

    BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"
    AUTH_URL = "https://api.login.yahoo.com/oauth2/request_auth"
    TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"
    
    # Add supported sports constants
    SUPPORTED_SPORTS = {
        'nfl': {'name': 'Football', 'code': 'nfl'},
        'mlb': {'name': 'Baseball', 'code': 'mlb'},
        'nba': {'name': 'Basketball', 'code': 'nba'},
        'nhl': {'name': 'Hockey', 'code': 'nhl'}
    }

    def get_authorization_url(self) -> str:
        """Generate the authorization URL for Yahoo OAuth2"""
        params = {
            'client_id': self.client_id,
            'redirect_uri': 'oob',  # Out-of-band callback
            'response_type': 'code',
            'scope': 'openid fspt-r'  # Both OpenID and Fantasy Sports Read scopes are required
        }
        auth_url = f"{self.AUTH_URL}?{urlencode(params)}"
        logger.info(f"Authorization URL: {auth_url}")
        return auth_url

    def handle_authorization(self, auth_code: str) -> bool:
        """
        Handle the authorization code and get access token.
        
        Args:
            auth_code: The authorization code from Yahoo
            
        Returns:
            bool: True if authorization was successful, False otherwise
        """
        try:
            logger.info("Starting authorization process...")
            
            # Prepare token request
            logger.info("Preparing token request...")
            token_data = {
                'grant_type': 'authorization_code',
                'redirect_uri': 'oob',
                'code': auth_code
            }
            
            # Make token request
            logger.info(f"Making token request to {self.TOKEN_URL}")
            response = requests.post(
                self.TOKEN_URL,
                data=token_data,
                auth=(self.client_id, self.client_secret)
            )
            
            logger.info(f"Token request completed with status code: {response.status_code}")
            
            if response.status_code == 200:
                token_data = response.json()
                logger.info("Successfully parsed token response")
                
                # Update instance variables directly
                self.token = token_data
                
                # Save tokens to file
                self._save_token(self.token)
                
                return True
                
            else:
                logger.error(f"Token request failed with status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error during authorization: {str(e)}")
            return False

    def _get_initial_tokens(self, authorization_code: str) -> Dict[str, Any]:
        """Exchange authorization code for initial access and refresh tokens"""
        try:
            logger.info("Preparing token request...")
            # Remove any duplicates in the authorization code
            auth_code = authorization_code[:len(authorization_code)//2] if len(authorization_code) % 2 == 0 else authorization_code
            
            data = {
                'grant_type': 'authorization_code',
                'redirect_uri': 'oob',
                'code': auth_code
            }

            auth_string = base64.b64encode(
                f"{self.client_id}:{self.client_secret}".encode()
            ).decode()

            headers = {
                'Authorization': f'Basic {auth_string}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            logger.info(f"Making token request to {self.TOKEN_URL}")
            logger.debug(f"Request data: {data}")
            logger.debug(f"Request headers: Authorization: Basic *****, Content-Type: {headers['Content-Type']}")
            
            try:
                response = requests.post(
                    self.TOKEN_URL,
                    data=data,
                    headers=headers,
                    timeout=10  # Add timeout
                )
                logger.info(f"Token request completed with status code: {response.status_code}")
                logger.debug(f"Response headers: {dict(response.headers)}")
                
                if response.status_code != 200:
                    error_msg = f"Token request failed with status {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    logger.error(f"Full response: {response.text}")
                    raise ValueError(error_msg)

                try:
                    response_data = response.json()
                    logger.info("Successfully parsed token response")
                    logger.debug(f"Response data keys: {list(response_data.keys())}")
                    return response_data
                except json.JSONDecodeError as e:
                    logger.error("Failed to parse JSON response")
                    logger.error(f"Raw response text: {response.text}")
                    raise

            except requests.exceptions.Timeout:
                logger.error("Token request timed out after 10 seconds")
                raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"Error getting initial tokens: {str(e)}", exc_info=True)
            raise

    def _refresh_access_token(self) -> None:
        """Refresh the access token using the refresh token"""
        try:
            logger.info("Refreshing access token...")
            
            # Prepare token request
            token_data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.token.get('refresh_token')
            }
            
            # Make token request
            response = requests.post(
                self.TOKEN_URL,
                data=token_data,
                auth=(self.client_id, self.client_secret)
            )
            
            if response.status_code == 200:
                new_token = response.json()
                self.token.update(new_token)
                self._save_token(self.token)
                logger.info("Successfully refreshed access token")
                
                # Update session
                self.session = OAuth2Session(
                    self.client_id,
                    token=self.token,
                    auto_refresh_url=self.TOKEN_URL,
                    auto_refresh_kwargs={
                        'client_id': self.client_id,
                        'client_secret': self.client_secret
                    },
                    token_updater=self._save_token
                )
            else:
                logger.error(f"Token refresh failed with status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                
        except Exception as e:
            logger.error(f"Error refreshing access token: {str(e)}")
            raise

    def _update_tokens(self, token_data: Dict[str, Any]) -> None:
        """Update stored tokens and expiration time"""
        try:
            self.token = token_data

            expires_in = int(token_data.get('expires_in', 3600))
            self.token['token_expiry'] = datetime.now() + timedelta(seconds=expires_in)
            self.token['last_update'] = datetime.now()

            self._save_token(self.token)

        except KeyError as e:
            logger.error(f"Missing required token data: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error updating tokens: {str(e)}")
            raise

    def get_games(self, sport_codes=None, seasons=None):
        """
        Get all available games (current and historical) from Yahoo Fantasy API.
        
        Args:
            sport_codes (List[str], optional): List of sport codes (e.g., ['nfl', 'mlb']). If None, get all supported sports.
            seasons (List[int], optional): List of seasons to get. If None, get all available seasons.
            
        Returns:
            List of games
        """
        if isinstance(sport_codes, str):
            sport_codes = [sport_codes]
        elif not sport_codes:
            sport_codes = list(self.SUPPORTED_SPORTS.keys())
        
        games_list = []
        
        # Get all available games for each sport
        for sport in sport_codes:
            # Use the games endpoint with out=metadata to get all historical games
            endpoint = f'/games;out=metadata;game_codes={sport}'
            response = self.make_request(endpoint)
            
            if not response or 'fantasy_content' not in response:
                logger.warning(f"No fantasy content in response for {sport}")
                continue
            
            games = response['fantasy_content'].get('games', {})
            if not isinstance(games, dict):
                logger.warning(f"Unexpected games data format for {sport}: {type(games)}")
                continue
            
            # Process each game
            for game_id, game_data in games.items():
                if isinstance(game_id, str) and game_id.isdigit():
                    if isinstance(game_data, dict) and 'game' in game_data:
                        game = game_data['game']
                        if isinstance(game, list) and len(game) > 0:
                            game_info = game[0]
                            if isinstance(game_info, dict):
                                # Filter by season if specified
                                if not seasons or game_info.get('season') in map(str, seasons):
                                    games_list.append(game_info)
        
        # Sort games by sport and season (most recent first)
        sorted_games = sorted(
            games_list,
            key=lambda x: (x.get('code'), -int(x.get('season', 0)))
        )
        
        logger.info(f"Found {len(sorted_games)} games")
        for game in sorted_games:
            logger.info(f"Game: {game.get('code')} Season {game.get('season')} (Key: {game.get('game_key')})")
        
        return sorted_games

    def get_nfl_teams(self) -> Dict:
        """Get NFL teams for the authenticated user"""
        try:
            endpoint = "/users;use_login=1/games;game_keys=nfl/teams"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting NFL teams: {str(e)}")
            raise

    def update_required(self, last_update: Optional[datetime] = None) -> bool:
        """Check if data update is required"""
        if not last_update and not self.token.get('last_update'):
            return True
        
        check_time = last_update or self.token.get('last_update')
        time_since_update = datetime.now() - check_time
        
        # Update if more than 1 hour has passed
        return time_since_update.total_seconds() > 3600  # 1 hour in seconds

    def get_leagues(self, sport_code='nfl', season=None) -> Dict:
        """Get user's fantasy leagues for a specific sport and season
        
        Args:
            sport_code (str): Sport code (e.g., 'nfl', 'mlb')
            season (int): Optional season year to get historical data
            
        Returns:
            List of league dictionaries containing league data
        """
        try:
            # First get all user's games
            endpoint = "/users;use_login=1/games"
            response = self.make_request(endpoint)
            
            if not response or 'fantasy_content' not in response:
                logger.error("No fantasy content in response")
                return []
                
            users = response['fantasy_content'].get('users', {})
            if '0' not in users:
                logger.error("No user data found")
                return []
                
            user_data = users['0'].get('user', [])
            if len(user_data) < 2:
                logger.error("Incomplete user data")
                return []
                
            games = user_data[1].get('games', {})
            if not isinstance(games, dict):
                logger.error("Invalid games data format")
                return []
                
            # Process each game to find leagues
            leagues = []
            for game_idx in range(int(games.get('count', 0))):
                game_data = games.get(str(game_idx), {}).get('game', [])
                if not game_data:
                    continue
                    
                # Get game info
                game = game_data[0] if isinstance(game_data, list) else game_data
                if game.get('code') != sport_code:
                    continue
                    
                if season and str(game.get('season')) != str(season):
                    continue
                    
                # Get leagues for this game
                if len(game_data) > 1 and isinstance(game_data[1], dict):
                    leagues_data = game_data[1].get('leagues', {})
                    if isinstance(leagues_data, dict):
                        for league_idx in range(int(leagues_data.get('count', 0))):
                            league = leagues_data.get(str(league_idx), {}).get('league', [])
                            if league and isinstance(league, list):
                                leagues.append(league[0])
            
            logger.info(f"Found {len(leagues)} leagues")
            return leagues
            
        except Exception as e:
            logger.error(f"Error getting leagues: {str(e)}")
            return []

    def get_league_standings(self, league_key: str) -> Dict:
        """Get standings for a specific league
        
        Args:
            league_key (str): The league key to get standings for
            
        Returns:
            Dictionary containing league standings data
        """
        try:
            endpoint = f"/league/{league_key}/standings"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting league standings: {str(e)}")
            raise

    def get_league_scoreboard(self, league_key: str, week: int = None) -> Dict:
        """Get scoreboard for a specific league and week
        
        Args:
            league_key (str): The league key to get scoreboard for
            week (int): Optional week number, defaults to current week
            
        Returns:
            Dictionary containing league scoreboard data
        """
        try:
            endpoint = f"/league/{league_key}/scoreboard"
            if week:
                endpoint += f";week={week}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting league scoreboard: {str(e)}")
            raise

    def get_league_info(self, league_key: str) -> Dict:
        """Get info for a specific league
        
        Args:
            league_key (str): The league key to get info for
            
        Returns:
            Dictionary containing league info data
        """
        try:
            endpoint = f"/league/{league_key}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting league info: {str(e)}")
            raise

    def get_teams(self, sport_code='nfl'):
        """Get teams for the authenticated user for a specific sport."""
        try:
            endpoint = '/users;use_login=1/teams'
            response = self.make_request(endpoint)
            
            logger.debug(f"Teams response: {response}")
            
            if not response or 'fantasy_content' not in response:
                logger.error("No fantasy content in response")
                return []
            
            fantasy_content = response['fantasy_content']
            users = fantasy_content.get('users', [])
            logger.debug(f"Users data: {users}")
            
            teams_list = []
            
            # Users should be in index 0
            if '0' in users:
                user_data = users['0']
                if isinstance(user_data, dict) and 'user' in user_data:
                    user = user_data['user']
                    if len(user) > 1 and isinstance(user[1], dict):
                        teams = user[1].get('teams', {})
                        if isinstance(teams, dict):
                            for team_key, team_data in teams.items():
                                if isinstance(team_data, dict) and 'team' in team_data:
                                    team = team_data['team'][0]
                                    if isinstance(team, dict):
                                        teams_list.append(team)
            
            logger.info(f"Found {len(teams_list)} teams")
            return teams_list
            
        except Exception as e:
            logger.error(f"Error getting teams: {str(e)}")
            return []

    def get_players(self, sport_code='nfl', start: int = 0, count: int = 25, existing_player_keys: List[str] = None) -> Dict[str, Any]:
        """
        Get players with pagination for a specific sport.
        
        Args:
            sport_code (str): Sport code (e.g., 'nfl', 'mlb')
            start (int): Starting index for pagination
            count (int): Number of players to retrieve
            existing_player_keys (List[str]): List of player keys that already exist
            
        Returns:
            Dict containing:
            - players: List of player dictionaries
            - total_available: Total number of players available
            - new_players_found: Number of new players found
            - has_more: Boolean indicating if there are more players to fetch
        """
        endpoint = f'/games;game_keys={sport_code}/players;start={start};count={count}'
        response = self.make_request(endpoint)
        
        players_list = []
        total_available = 0
        new_players_found = 0
        
        if not response or 'fantasy_content' not in response:
            logger.error("No fantasy content in response")
            return {
                'players': players_list,
                'total_available': total_available,
                'new_players_found': new_players_found,
                'has_more': False
            }
            
        fantasy_content = response['fantasy_content']
        games_data = fantasy_content.get('games', {})
        
        if not games_data or not isinstance(games_data, dict):
            logger.error("No games data found or invalid format")
            return {
                'players': players_list,
                'total_available': total_available,
                'new_players_found': new_players_found,
                'has_more': False
            }
            
        # Debug log the games data structure
        logger.debug(f"Games data structure: {games_data}")
        
        for game_key, game_data in games_data.items():
            if not isinstance(game_data, dict) or 'game' not in game_data:
                continue
                
            game = game_data['game']
            if not isinstance(game, list) or len(game) < 2:
                continue
                
            players_data = game[1].get('players', {})
            if not players_data:
                continue
                
            # Debug log the players data structure
            logger.debug(f"Players data structure: {players_data}")
            
            # Get the count of total available players
            if isinstance(players_data, dict):
                total_available = int(players_data.get('count', 0))
                
                # Process each player in the response
                for idx in range(count):
                    idx_str = str(idx)
                    if idx_str not in players_data:
                        break
                        
                    player_data = players_data[idx_str].get('player', [])
                    if not player_data or not isinstance(player_data, list):
                        continue
                        
                    # Process the player data
                    player_info = {}
                    for item in player_data:
                        if isinstance(item, dict):
                            # Extract player key
                            if 'player_key' in item:
                                player_info['player_key'] = item['player_key']
                            # Extract name
                            elif 'name' in item and isinstance(item['name'], dict):
                                player_info['name'] = item['name'].get('full')
                            # Extract team info
                            elif 'editorial_team_full_name' in item:
                                player_info['team'] = item['editorial_team_full_name']
                            # Extract position
                            elif 'display_position' in item:
                                player_info['position'] = item['display_position']
                            # Extract status
                            elif 'status' in item:
                                player_info['status'] = item['status']
                            # Extract injury note
                            elif 'injury_note' in item:
                                player_info['injury_note'] = item['injury_note']
                            # Extract headshot URL
                            elif 'headshot' in item and isinstance(item['headshot'], dict):
                                player_info['headshot_url'] = item['headshot'].get('url')
                    
                    # Only add player if we have the key and it's not in existing_player_keys
                    if player_info.get('player_key') and (not existing_player_keys or player_info['player_key'] not in existing_player_keys):
                        players_list.append(player_info)
                        new_players_found += 1
        
        has_more = (start + count) < total_available
        logger.info(f"Found {new_players_found} new players out of {len(players_list)} retrieved")
        
        return {
            'players': players_list,
            'total_available': total_available,
            'new_players_found': new_players_found,
            'has_more': has_more
        }

    def get_game(self, game_key: str) -> Dict:
        """Get metadata for a specific game
        
        Args:
            game_key (str): The game key to get metadata for
            
        Returns:
            Dictionary containing game metadata
        """
        try:
            endpoint = f"/game/{game_key}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting game metadata: {str(e)}")
            raise

    def get_league(self, league_key: str) -> Dict:
        """Get metadata for a specific league
        
        Args:
            league_key (str): The league key to get metadata for
            
        Returns:
            Dictionary containing league metadata
        """
        try:
            endpoint = f"{self.BASE_URL}/league/{league_key}?format=json"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting league metadata: {str(e)}")
            raise

    def get_league_settings(self, league_key: str) -> Dict:
        """Get settings for a specific league including roster positions
        
        Args:
            league_key (str): The league key to get settings for
            
        Returns:
            Dictionary containing league settings
        """
        try:
            endpoint = f"{self.BASE_URL}/league/{league_key}/settings?format=json"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting league settings: {str(e)}")
            raise

    def get_league_players(self, league_key: str, start: int = 0, count: int = 25) -> Dict:
        """Get players in a specific league
        
        Args:
            league_key (str): The league key to get players for
            start (int): Starting position for pagination
            count (int): Number of players to retrieve
            
        Returns:
            Dictionary containing league players
        """
        try:
            endpoint = f"/league/{league_key}/players;start={start};count={count}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting league players: {str(e)}")
            raise

    def get_team(self, team_key: str) -> Dict:
        """Get metadata for a specific team
        
        Args:
            team_key (str): The team key to get metadata for
            
        Returns:
            Dictionary containing team metadata
        """
        try:
            endpoint = f"/team/{team_key}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting team metadata: {str(e)}")
            raise

    def get_team_roster(self, team_key: str, week: int = None) -> Dict[str, Any]:
        """Get roster for a team.
        
        Args:
            team_key: The team key to get roster for
            week: Optional week number to get roster for a specific week
            
        Returns:
            Dict containing roster data
        """
        params = {'format': 'json'}
        if week is not None:
            params['week'] = week
        
        return self.make_request(f"/team/{team_key}/roster", params)

    def get_team_matchups(self, team_key: str) -> Dict:
        """Get matchups for a specific team
        
        Args:
            team_key (str): The team key to get matchups for
            
        Returns:
            Dictionary containing team matchups
        """
        try:
            endpoint = f"/team/{team_key}/matchups"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting team matchups: {str(e)}")
            raise

    def get_player(self, player_key: str) -> Dict:
        """Get metadata for a specific player
        
        Args:
            player_key (str): The player key to get metadata for
            
        Returns:
            Dictionary containing player metadata
        """
        try:
            endpoint = f"/player/{player_key}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting player metadata: {str(e)}")
            raise

    def get_player_stats(self, player_key: str) -> Dict:
        """Get statistics for a specific player
        
        Args:
            player_key (str): The player key to get statistics for
            
        Returns:
            Dictionary containing player statistics
        """
        try:
            endpoint = f"/player/{player_key}/stats"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting player stats: {str(e)}")
            raise

    def get_player_ownership(self, player_key: str) -> Dict:
        """Get ownership data for a specific player
        
        Args:
            player_key (str): The player key to get ownership data for
            
        Returns:
            Dictionary containing player ownership data
        """
        try:
            endpoint = f"/player/{player_key}/ownership"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting player ownership: {str(e)}")
            raise

    def get_league_transactions(self, league_key: str) -> Dict:
        """Get transactions for a specific league
        
        Args:
            league_key (str): The league key to get transactions for
            
        Returns:
            Dictionary containing league transactions
        """
        try:
            endpoint = f"/league/{league_key}/transactions"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting league transactions: {str(e)}")
            raise

    def create_transaction(self, league_key: str, transaction_data: Dict) -> Dict:
        """Create a transaction in a specific league
        
        Args:
            league_key (str): The league key to create transaction in
            transaction_data (Dict): Transaction details
            
        Returns:
            Dictionary containing transaction result
        """
        try:
            endpoint = f"/league/{league_key}/transactions"
            return self.make_request(endpoint, method='POST', data=transaction_data)
        except Exception as e:
            logger.error(f"Error creating transaction: {str(e)}")
            raise

    def get_draft_results(self, league_key: str) -> Dict:
        """Get draft results for a specific league
        
        Args:
            league_key (str): The league key to get draft results for
            
        Returns:
            Dictionary containing draft results
        """
        try:
            endpoint = f"/league/{league_key}/draftresults"
            response = self.make_request(endpoint)
            
            # Log the raw response
            logger.debug("Raw draft results response from Yahoo API:")
            logger.debug(json.dumps(response, indent=2))
            
            return response
        except Exception as e:
            logger.error(f"Error getting draft results: {str(e)}")
            raise

    def search_players(self, league_key: str, search_term: str, filters: Dict = None) -> Dict:
        """Search for players in a league based on name or other criteria
        
        Args:
            league_key (str): The league key to search in
            search_term (str): Term to search for
            filters (Dict): Optional filters like status, position, etc.
            
        Returns:
            Dictionary containing matching players
        """
        try:
            endpoint = f"/league/{league_key}/players;search={search_term}"
            
            # Add any additional filters
            if filters:
                if 'status' in filters:
                    endpoint += f";status={filters['status']}"
                if 'position' in filters:
                    endpoint += f";position={filters['position']}"
                if 'start' in filters:
                    endpoint += f";start={filters['start']}"
                if 'count' in filters:
                    endpoint += f";count={filters['count']}"
                    
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error searching players: {str(e)}")
            raise

    def get_player_stats_by_league(self, league_key: str, player_keys: List[str]) -> Dict:
        """Get detailed stats for specific players in a league
        
        Args:
            league_key (str): The league key
            player_keys (List[str]): List of player keys to get stats for
            
        Returns:
            Dictionary containing player stats
        """
        try:
            player_keys_str = ','.join(player_keys)
            endpoint = f"/league/{league_key}/players;player_keys={player_keys_str}/stats"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting player stats: {str(e)}")
            raise

    def get_player_projections(self, league_key: str, player_keys: List[str]) -> Dict:
        """Get projected stats for specific players
        
        Args:
            league_key (str): The league key
            player_keys (List[str]): List of player keys to get projections for
            
        Returns:
            Dictionary containing player projections
        """
        try:
            player_keys_str = ','.join(player_keys)
            endpoint = f"/league/{league_key}/players;player_keys={player_keys_str}/projections"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting player projections: {str(e)}")
            raise

    def get_user_metadata(self) -> Dict:
        """Get metadata for user-specific fantasy content
        
        Returns:
            Dictionary containing user metadata including custom rules and settings
        """
        try:
            endpoint = "/users;use_login=1/out=metadata"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting user metadata: {str(e)}")
            raise

    def get_filtered_players(self, league_key: str, filters: Dict) -> Dict:
        """Get players with advanced filtering options
        
        Args:
            league_key (str): The league key
            filters (Dict): Dictionary of filters including:
                - status: Player status (e.g., 'FA', 'ALL')
                - position: Player position
                - start: Starting point for pagination
                - count: Number of results to return
                
        Returns:
            Dictionary containing filtered players
        """
        try:
            endpoint = f"/league/{league_key}/players"
            
            # Add filters to endpoint
            filter_parts = []
            if 'status' in filters:
                filter_parts.append(f"status={filters['status']}")
            if 'position' in filters:
                filter_parts.append(f"position={filters['position']}")
            if 'start' in filters:
                filter_parts.append(f"start={filters['start']}")
            if 'count' in filters:
                filter_parts.append(f"count={filters['count']}")
                
            if filter_parts:
                endpoint += ";" + ";".join(filter_parts)
                
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting filtered players: {str(e)}")
            raise

    def get_user_info(self) -> Dict:
        """Get metadata about the logged-in user
        
        Returns:
            Dictionary containing user metadata
        """
        try:
            endpoint = "/users;use_login=1"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting user info: {str(e)}")
            raise

    def get_league_roster(self, league_key: str, team_key: str, week: Optional[int] = None) -> Dict:
        """Get roster for a specific team in a league
        
        Args:
            league_key (str): The league key
            team_key (str): The team key
            week (Optional[int]): Week number for historical rosters
            
        Returns:
            Dictionary containing team roster data
        """
        try:
            endpoint = f"/team/{team_key}/roster"
            if week:
                endpoint += f";week={week}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting team roster: {str(e)}")
            raise

    def get_league_points(self, league_key: str, team_key: str, week: Optional[int] = None) -> Dict:
        """Get points for a specific team in a league
        
        Args:
            league_key (str): The league key
            team_key (str): The team key
            week (Optional[int]): Week number for historical points
            
        Returns:
            Dictionary containing team points data
        """
        try:
            endpoint = f"/team/{team_key}/stats"
            if week:
                endpoint += f";week={week}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting team points: {str(e)}")
            raise

    def get_league_games(self, league_key: str, week: Optional[int] = None) -> Dict:
        """Get games for a specific league
        
        Args:
            league_key (str): The league key
            week (Optional[int]): Week number to get games for
            
        Returns:
            Dictionary containing games data
        """
        try:
            endpoint = f"/league/{league_key}/scoreboard?format=json"
            if week:
                endpoint += f"&week={week}"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting league games: {str(e)}")
            raise

    def get_league_schedule(self, league_key: str, team_key: Optional[str] = None) -> Dict:
        """Get schedule for a specific league or team
        
        Args:
            league_key (str): The league key
            team_key (Optional[str]): Team key to get schedule for specific team
            
        Returns:
            Dictionary containing schedule data
        """
        try:
            if team_key:
                endpoint = f"/team/{team_key}/matchups"
            else:
                endpoint = f"/league/{league_key}/scoreboard;type=full"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting schedule: {str(e)}")
            raise

    def get_all_league_data(self, league_key: str, team_key: str = None) -> Dict[str, Any]:
        """Get all league data including rosters, points, games, schedules, and draft info
        
        Args:
            league_key (str): The league key
            team_key (Optional[str]): Team key if getting data for specific team
            
        Returns:
            Dictionary containing all league data
        """
        try:
            data = {
                'league_info': self.get_league(league_key),
                'standings': self.get_league_standings(league_key),
                'scoreboard': self.get_league_scoreboard(league_key),
                'settings': self.get_league_settings(league_key),
                'draft_results': self.get_draft_results(league_key),
                'games': self.get_league_games(league_key),
                'schedule': self.get_league_schedule(league_key)
            }

            if team_key:
                data.update({
                    'team_roster': self.get_league_roster(league_key, team_key),
                    'team_points': self.get_league_points(league_key, team_key),
                    'team_schedule': self.get_league_schedule(league_key, team_key)
                })

            return data
        except Exception as e:
            logger.error(f"Error getting all league data: {str(e)}")
            raise

    def get_historical_team(self, team_key: str) -> Dict:
        """Get metadata for a historical team
        
        Args:
            team_key (str): The team key to get metadata for
            
        Returns:
            Dictionary containing team metadata
        """
        try:
            endpoint = f"/team/{team_key}/metadata"
            response = self.make_request(endpoint)
            
            if not response or 'fantasy_content' not in response:
                logger.error("No fantasy content in response")
                return {}
                
            team_data = response['fantasy_content'].get('team', [])
            if not team_data or len(team_data) < 1:
                logger.error(f"No team data found for key {team_key}")
                return {}
                
            # Get the first element which contains team info
            team_info_list = team_data[0]
            
            # Extract data using helper function
            def get_value(key):
                return next((item.get(key) for item in team_info_list if isinstance(item, dict) and key in item), None)
            
            # Extract basic team info
            team_key = get_value('team_key')
            name = get_value('name')
            
            # Extract team logo if available
            team_logos = next((item.get('team_logos', []) for item in team_info_list if isinstance(item, dict) and 'team_logos' in item), [])
            logo_url = team_logos[0].get('team_logo', {}).get('url') if team_logos else None
            
            # Extract manager info if available
            managers_data = next((item.get('managers', []) for item in team_info_list if isinstance(item, dict) and 'managers' in item), [])
            manager = managers_data[0].get('manager', {}) if managers_data else {}
            
            # Extract stats and metadata
            stats = {
                'waiver_priority': get_value('waiver_priority'),
                'number_of_moves': get_value('number_of_moves'),
                'number_of_trades': get_value('number_of_trades'),
                'clinched_playoffs': get_value('clinched_playoffs'),
                'league_scoring_type': get_value('league_scoring_type'),
                'draft_position': get_value('draft_position'),
                'roster_adds': next((item.get('roster_adds') for item in team_info_list if isinstance(item, dict) and 'roster_adds' in item), {}),
                'manager': {
                    'manager_id': manager.get('manager_id'),
                    'nickname': manager.get('nickname'),
                    'guid': manager.get('guid'),
                    'is_commissioner': manager.get('is_commissioner'),
                    'image_url': manager.get('image_url')
                }
            }
            
            # Map game code to sport code
            game_code_to_sport = {
                '49': 'nfl',  # 2002
                '79': 'nfl',  # 2003
                '101': 'nfl', # 2004
                '124': 'nfl', # 2005
                '153': 'nfl', # 2006
                '175': 'nfl', # 2007
                '199': 'nfl', # 2008
                '222': 'nfl', # 2009
                '242': 'nfl', # 2010
                '257': 'nfl', # 2011
                '273': 'nfl', # 2012
                '314': 'nfl', # 2013
                '331': 'nfl', # 2014
                '348': 'nfl', # 2015
                '359': 'nfl', # 2016
                '371': 'nfl', # 2017
                '380': 'nfl', # 2018
                '390': 'nfl', # 2019
                '399': 'nfl', # 2020
                '406': 'nfl', # 2021
                '414': 'nfl', # 2022
                '423': 'nfl', # 2023
                '449': 'nfl', # 2024
            }
            
            # Extract game code from team key (e.g., 101 from 101.l.166056.t.1)
            game_code = team_key.split('.')[0]
            sport_code = game_code_to_sport.get(game_code, 'nfl')  # Default to 'nfl' if not found
            
            return {
                'team_key': team_key,
                'sport_code': sport_code,
                'name': name,
                'logo_url': logo_url,
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Error getting historical team data: {str(e)}")
            return {}

    def get_team_metadata(self, team_key: str) -> Dict:
        """Get metadata for a specific team, including historical teams
        
        Args:
            team_key (str): The team key (e.g., '101.l.166056.t.1')
            
        Returns:
            Dictionary containing team metadata
        """
        try:
            endpoint = f"/team/{team_key}/metadata"
            return self.make_request(endpoint)
        except Exception as e:
            logger.error(f"Error getting team metadata: {str(e)}")
            raise

