from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, List, Any, Tuple
import pandas as pd
from yahoo_api import YahooFantasyAPI

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self, yahoo_api=None, use_db=False, db_path=None):
        self.yahoo_api = yahoo_api
        self.use_db = use_db
        if use_db:
            try:
                from database import Database
                self.db = Database(db_path=db_path)
            except Exception as e:
                logger.warning(f"Database initialization failed: {str(e)}. Running without database.")
                self.use_db = False

    def update_data(self, force: bool = False, data_type: str = None, sport_code: str = 'nfl',
                   start: int = 0, count: int = 25, seasons: List[int] = None) -> Dict[str, Any]:
        """Update data in the database.
        
        Args:
            force: Whether to force update regardless of last update time
            data_type: Specific type of data to update ('games', 'teams', 'players')
            sport_code: Sport code (e.g., 'nfl', 'mlb', 'nba', 'nhl')
            start: Starting index for player pagination
            count: Number of items to retrieve per request
            seasons: List of seasons to fetch for games
            
        Returns:
            Dict containing update status and details
        """
        try:
            if not self.yahoo_api:
                return {"status": "error", "message": "Yahoo API not initialized"}

            if data_type == "players":
                return self._update_players(sport_code, start, count)
            elif data_type == "games":
                return self._update_games(sport_code, seasons)
            elif data_type == "teams":
                return self._update_teams(sport_code)
            elif data_type == "leagues":
                return self._update_leagues(sport_code)
            elif data_type == "league_standings":
                return self._update_league_standings(sport_code)
            elif data_type == "league_settings":
                return self._update_league_settings(sport_code)
            elif data_type == "league_scoreboard":
                return self._update_league_scoreboard(sport_code)
            else:
                # Update all data types
                results = {}
                for dt in ["games", "leagues", "teams", "players", "league_standings", "league_settings", "league_scoreboard"]:
                    results[dt] = self.update_data(force=force, data_type=dt, sport_code=sport_code, seasons=seasons)
                return {"status": "success", **results}
                
        except Exception as e:
            logger.error(f"Error updating data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def _update_players(self, sport_code: str, start: int, count: int) -> Dict[str, Any]:
        # Get existing player keys from database
        existing_players = self.db.get_all_players(sport_code=sport_code) if self.use_db else []
        existing_player_keys = [p['player_key'] for p in existing_players]
        
        # Get players with checkpoint info
        result = self.yahoo_api.get_players(
            sport_code=sport_code,
            start=start,
            count=count,
            existing_player_keys=existing_player_keys
        )
        
        if not result['players']:
            return {
                "status": "success",
                "message": "No new players to add",
                "count": 0,
                "has_more": False
            }
        
        # Save only new players
        if self.use_db:
            for player in result['players']:
                # Add sport code to player data
                player['sport_code'] = sport_code
                self.db.save_player(player)
        
        return {
            "status": "success",
            "message": f"Added {result['new_players_found']} new players",
            "count": result['new_players_found'],
            "has_more": result['has_more'],
            "total_available": result['total_available']
        }

    def _update_games(self, sport_code: str, seasons: List[int]) -> Dict[str, Any]:
        games = self.yahoo_api.get_games(sport_codes=[sport_code], seasons=seasons)
        if self.use_db:
            for game in games:
                game['sport_code'] = sport_code
                self.db.save_game(game)
        return {"status": "success", "count": len(games)}

    def _update_teams(self, sport_code: str) -> Dict[str, Any]:
        teams = self.yahoo_api.get_teams(sport_code=sport_code)
        if self.use_db:
            for team in teams:
                team['sport_code'] = sport_code
                self.db.save_team(team)
        return {"status": "success", "count": len(teams)}

    def _update_leagues(self, sport_code: str) -> Dict[str, Any]:
        leagues = self.yahoo_api.get_leagues(sport_code=sport_code)
        if self.use_db:
            for league in leagues:
                league['sport_code'] = sport_code
                self.db.save_league(league)
        return {"status": "success", "count": len(leagues)}

    def _update_league_standings(self, sport_code: str) -> Dict[str, Any]:
        leagues = self.yahoo_api.get_leagues(sport_code=sport_code)
        standings = []
        for league in leagues:
            league_standings = self.yahoo_api.get_league_standings(league['league_key'])
            if self.use_db:
                self.db.save_league_standings(league_standings)
            standings.append(league_standings)
        return {"status": "success", "count": len(standings)}

    def _update_league_settings(self, sport_code: str) -> Dict[str, Any]:
        leagues = self.yahoo_api.get_leagues(sport_code=sport_code)
        settings = []
        for league in leagues:
            league_settings = self.yahoo_api.get_league_settings(league['league_key'])
            if self.use_db:
                self.db.save_league_settings(league_settings)
            settings.append(league_settings)
        return {"status": "success", "count": len(settings)}

    def _update_league_scoreboard(self, sport_code: str) -> Dict[str, Any]:
        leagues = self.yahoo_api.get_leagues(sport_code=sport_code)
        scoreboards = []
        for league in leagues:
            league_scoreboard = self.yahoo_api.get_league_scoreboard(league['league_key'])
            if self.use_db:
                self.db.save_league_scoreboard(league_scoreboard)
            scoreboards.append(league_scoreboard)
        return {"status": "success", "count": len(scoreboards)}

    def get_league_stats(self, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get league statistics for visualization"""
        try:
            if not self.use_db:
                return []
            
            # Get league stats from database
            return self.db.get_league_stats(sport_code=sport_code)
        except Exception as e:
            logger.error(f"Error getting league stats: {str(e)}")
            return []

    def get_player_stats(self, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get player statistics for visualization"""
        try:
            if not self.use_db:
                return []
            
            # Get player stats from database
            return self.db.get_player_stats(sport_code=sport_code)
        except Exception as e:
            logger.error(f"Error getting player stats: {str(e)}")
            return []

    def get_leaderboard(self, sport_code: str = 'nfl', season: int = None, week: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get the leaderboard for a specific sport, season and week"""
        try:
            if not self.use_db:
                return []
            
            # Get leaderboard from database
            return self.db.get_leaderboard(sport_code=sport_code, season=season, week=week)
        except Exception as e:
            logger.error(f"Error getting leaderboard: {str(e)}")
            return []

    def get_player_performance(self, player_name: str, sport_code: str = 'nfl') -> List[Dict[str, Any]]:
        """Get performance data for a specific player"""
        try:
            if not self.use_db:
                return []
            
            # Get player performance from database
            return self.db.get_player_performance(player_name=player_name, sport_code=sport_code)
        except Exception as e:
            logger.error(f"Error getting player performance: {str(e)}")
            return []

    def update_games(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Update games data"""
        try:
            if not self.yahoo_api:
                return {"processed": 0, "error": "Yahoo API not initialized"}
            
            games = self.yahoo_api.get_nfl_teams()  # Using get_nfl_teams as placeholder
            if not games:
                return {"processed": 0}

            if self.use_db:
                # Save to database if available
                for game in games:
                    self.db.save_game(game)

            return {"processed": len(games) if isinstance(games, list) else 1}
        except Exception as e:
            logger.error(f"Error updating games: {str(e)}")
            return {"processed": 0, "error": str(e)}

    def update_leagues(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Update leagues data"""
        try:
            # Placeholder implementation
            return {"processed": 0}
        except Exception as e:
            logger.error(f"Error updating leagues: {str(e)}")
            return {"processed": 0, "error": str(e)}

    def update_teams(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Update teams data"""
        try:
            if not self.yahoo_api:
                return {"processed": 0, "error": "Yahoo API not initialized"}
            
            teams = self.yahoo_api.get_nfl_teams()
            if not teams:
                return {"processed": 0}

            if self.use_db:
                # Save to database if available
                for team in teams:
                    self.db.save_team(team)

            return {"processed": len(teams) if isinstance(teams, list) else 1}
        except Exception as e:
            logger.error(f"Error updating teams: {str(e)}")
            return {"processed": 0, "error": str(e)}

    def update_players(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Update players data"""
        try:
            # Placeholder implementation
            return {"processed": 0}
        except Exception as e:
            logger.error(f"Error updating players: {str(e)}")
            return {"processed": 0, "error": str(e)}

    def save_games(self, games_data: List[Dict]) -> None:
        """Save games data to the database."""
        if not self.use_db:
            return
            
        for game in games_data:
            game_data = {
                'game_key': game.get('game_key'),
                'sport_code': game.get('code'),
                'name': game.get('name'),
                'season': game.get('season'),
                'game_code': game.get('code'),
                'game_type': game.get('type')
            }
            try:
                self.db.save_game(game_data)
            except Exception as e:
                logger.error(f"Error saving game {game_data['game_key']}: {str(e)}")

    def save_leagues(self, leagues_data: List[Dict]) -> None:
        """Save leagues data to the database."""
        if not self.use_db:
            return
            
        for league in leagues_data:
            league_data = {
                'league_key': league.get('league_key'),
                'name': league.get('name'),
                'game_code': league.get('game_code'),
                'season': league.get('season'),
                'num_teams': league.get('num_teams'),
                'league_type': league.get('league_type')
            }
            self.db.save_league(league_data)

    def save_league_settings(self, settings_data: Dict) -> None:
        """Save league settings to the database."""
        if not self.use_db:
            return
            
        if settings_data and 'league_key' in settings_data:
            self.db.save_league_settings({
                'league_key': settings_data['league_key'],
                'settings': settings_data
            })

    def save_league_standings(self, standings_data: Dict) -> None:
        """Save league standings to the database."""
        if not self.use_db:
            return
            
        if standings_data and 'league_key' in standings_data:
            self.db.save_league_standings({
                'league_key': standings_data['league_key'],
                'standings': standings_data
            })

    def save_teams(self, teams_data: Dict) -> None:
        """Save teams data to the database."""
        if not self.use_db:
            return
            
        if teams_data and 'teams' in teams_data:
            teams = teams_data['teams'].get('0', {}).get('team', [])
            for team in teams:
                team_data = {
                    'team_key': team.get('team_key'),
                    'name': team.get('name'),
                    'logo_url': team.get('team_logos', [{}])[0].get('url') if team.get('team_logos') else None,
                    'manager_name': team.get('managers', [{}])[0].get('nickname') if team.get('managers') else None
                }
                self.db.save_team(team_data)

    def save_players(self, players_data):
        """Save player data to the database.
        
        Args:
            players_data (dict): Player data from Yahoo API
            
        Returns:
            int: Number of players successfully saved
        """
        if not self.use_db or not players_data:
            return 0
        
        players = []
        players_saved = 0
        
        # Handle the nested structure from games endpoint
        if 'game' in players_data:
            players_section = players_data['game'][1]['players']
            # Process up to 25 players
            for i in range(25):
                if str(i) not in players_section:
                    break
                player_data = players_section[str(i)]['player'][0]
                
                # Extract player info
                player = {
                    'player_key': next((item['player_key'] for item in player_data if 'player_key' in item), None),
                    'name': next((item['name']['full'] for item in player_data if 'name' in item), None),
                    'team': next((item['editorial_team_full_name'] for item in player_data if 'editorial_team_full_name' in item), None),
                    'position': next((item['display_position'] for item in player_data if 'display_position' in item), None),
                    'status': next((item['status'] for item in player_data if 'status' in item), None),
                    'injury_note': next((item['injury_note'] for item in player_data if 'injury_note' in item), None),
                    'headshot_url': next((item['headshot']['url'] for item in player_data if 'headshot' in item), None),
                    'sport_code': 'nfl'  # Hardcoded for now since we're only handling NFL
                }
                
                if all(player[key] is not None for key in ['player_key', 'name']):
                    players.append(player)
        
        # Save each player to the database
        for player in players:
            try:
                self.db.save_player(**player)
                players_saved += 1
                logging.info(f"Saved player: {player['name']}")
            except Exception as e:
                logging.error(f"Error saving player {player['name']}: {str(e)}")
        
        return players_saved

    def _update_league_rosters(self, sport_code: str) -> Dict[str, Any]:
        """Update league rosters for all teams"""
        leagues = self.yahoo_api.get_leagues(sport_code=sport_code)
        rosters_count = 0
        
        for league in leagues:
            league_key = league['league_key']
            teams = self.yahoo_api.get_teams(sport_code=sport_code)
            
            for team in teams:
                team_key = team['team_key']
                roster = self.yahoo_api.get_league_roster(league_key, team_key)
                
                if roster and 'roster' in roster:
                    for player in roster['roster']:
                        roster_data = {
                            'league_key': league_key,
                            'team_key': team_key,
                            'player_key': player['player_key'],
                            'selected_position': player.get('selected_position'),
                            'is_starting': 1 if player.get('is_starting') else 0,
                            'week': None  # Current week
                        }
                        if self.use_db:
                            self.db.save_league_roster(roster_data)
                        rosters_count += 1
        
        return {"status": "success", "count": rosters_count}

    def _update_league_points(self, sport_code: str) -> Dict[str, Any]:
        """Update league points for all teams"""
        leagues = self.yahoo_api.get_leagues(sport_code=sport_code)
        points_count = 0
        
        for league in leagues:
            league_key = league['league_key']
            teams = self.yahoo_api.get_teams(sport_code=sport_code)
            
            for team in teams:
                team_key = team['team_key']
                points = self.yahoo_api.get_league_points(league_key, team_key)
                
                if points and 'points' in points:
                    points_data = {
                        'league_key': league_key,
                        'team_key': team_key,
                        'points': points['points'].get('total'),
                        'projected_points': points['points'].get('projected'),
                        'week': None  # Current week
                    }
                    if self.use_db:
                        self.db.save_league_points(points_data)
                    points_count += 1
        
        return {"status": "success", "count": points_count}

    def _update_league_games(self, sport_code: str) -> Dict[str, Any]:
        """Update league games"""
        leagues = self.yahoo_api.get_leagues(sport_code=sport_code)
        games_count = 0
        
        for league in leagues:
            league_key = league['league_key']
            games = self.yahoo_api.get_league_games(league_key)
            
            if games and 'games' in games:
                for game in games['games']:
                    game_data = {
                        'league_key': league_key,
                        'week': game.get('week'),
                        'home_team_key': game['home_team']['team_key'],
                        'away_team_key': game['away_team']['team_key'],
                        'home_team_points': game['home_team'].get('points'),
                        'away_team_points': game['away_team'].get('points'),
                        'status': game.get('status'),
                        'game_start_time': game.get('start_time')
                    }
                    if self.use_db:
                        self.db.save_league_game(game_data)
                    games_count += 1
        
        return {"status": "success", "count": games_count}

    def _update_league_schedules(self, sport_code: str) -> Dict[str, Any]:
        """Update league schedules"""
        leagues = self.yahoo_api.get_leagues(sport_code=sport_code)
        schedules_count = 0
        
        for league in leagues:
            league_key = league['league_key']
            schedule = self.yahoo_api.get_league_schedule(league_key)
            
            if schedule and 'schedule' in schedule:
                for matchup in schedule['schedule']:
                    schedule_data = {
                        'league_key': league_key,
                        'week': matchup.get('week'),
                        'team_key': matchup['team']['team_key'],
                        'opponent_team_key': matchup['opponent']['team_key'],
                        'is_home': matchup.get('is_home', 0)
                    }
                    if self.use_db:
                        self.db.save_league_schedule(schedule_data)
                    schedules_count += 1
        
        return {"status": "success", "count": schedules_count}

    def _update_league_draft_results(self, league_key: str) -> Tuple[bool, int]:
        """Update league draft results for a specific league."""
        try:
            draft_results = self.yahoo_api.get_league_draft_results(league_key)
            if draft_results:
                self.db.save_league_draft_results(draft_results)
                return True, len(draft_results)
            return True, 0
        except Exception as e:
            logger.error(f"Error updating league draft results: {str(e)}")
            return False, 0

    def update_all_league_data(self, sport_code: str = 'nfl') -> Dict[str, Any]:
        """Update all league-related data"""
        try:
            results = {
                'rosters': self._update_league_rosters(sport_code),
                'points': self._update_league_points(sport_code),
                'games': self._update_league_games(sport_code),
                'schedules': self._update_league_schedules(sport_code),
                'draft': self._update_league_draft_results(sport_code)
            }
            
            total_count = sum(r['count'] for r in results.values())
            return {
                "status": "success",
                "message": f"Updated {total_count} league records",
                "details": results
            }
        except Exception as e:
            logger.error(f"Error updating league data: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }