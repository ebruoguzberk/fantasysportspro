import logging
import json
from load_players_stats_data import extract_player_stats

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_stats_extraction():
    """Test stats extraction with sample data from Yahoo Fantasy UI"""
    
    # C.J. Stroud - QB - Hou
    stroud_data = {
        'player': [
            {
                'player_key': '449.p.123456',
                'selected_position': {'position': 'QB'},
                'name': {'full': 'C.J. Stroud'},
                'editorial_team_abbr': 'Hou',
                'bye_week': '14',
                'status': 'Final L 23-31 vs Bal'
            },
            {
                'player_stats': {
                    'stats': [
                        {'stat': {'stat_id': '4', 'value': '185'}},  # passing_yards
                        {'stat': {'stat_id': '5', 'value': '0'}},    # passing_touchdowns
                        {'stat': {'stat_id': '6', 'value': '1'}},    # passing_interceptions
                        {'stat': {'stat_id': '9', 'value': '7'}},    # rushing_yards
                        {'stat': {'stat_id': '19', 'value': '1'}},   # passing_attempts
                        {'stat': {'stat_id': '20', 'value': '0'}}    # passing_completions
                    ]
                },
                'player_points': {'total': '6.10'},
                'player_projected_points': {'total': '15.06'},
                'percent_started': '35',
                'percent_owned': '85'
            }
        ]
    }

    # Cooper Kupp - WR - LAR
    kupp_data = {
        'player': [
            {
                'player_key': '449.p.30125',
                'selected_position': {'position': 'WR'},
                'name': {'full': 'Cooper Kupp'},
                'editorial_team_abbr': 'LAR',
                'bye_week': '6',
                'status': 'Final W 13-9 vs Ari'
            },
            {
                'player_stats': {
                    'stats': [
                        {'stat': {'stat_id': '11', 'value': '1'}},   # receptions
                        {'stat': {'stat_id': '12', 'value': '29'}},  # receiving_yards
                        {'stat': {'stat_id': '13', 'value': '0'}},   # receiving_touchdowns
                        {'stat': {'stat_id': '78', 'value': '3'}}    # targets
                    ]
                },
                'player_points': {'total': '3.90'},
                'player_projected_points': {'total': '13.50'},
                'percent_started': '78',
                'percent_owned': '98'
            }
        ]
    }

    # Jameson Williams - WR - Det
    williams_data = {
        'player': [
            {
                'player_key': '449.p.789012',
                'selected_position': {'position': 'WR'},
                'name': {'full': 'Jameson Williams'},
                'editorial_team_abbr': 'Det',
                'bye_week': '5',
                'status': 'Final W 40-34 @ SF'
            },
            {
                'player_stats': {
                    'stats': [
                        {'stat': {'stat_id': '11', 'value': '5'}},   # receptions
                        {'stat': {'stat_id': '12', 'value': '77'}},  # receiving_yards
                        {'stat': {'stat_id': '13', 'value': '1'}},   # receiving_touchdowns
                        {'stat': {'stat_id': '78', 'value': '8'}},   # targets
                        {'stat': {'stat_id': '9', 'value': '-2'}},   # rushing_yards
                        {'stat': {'stat_id': '10', 'value': '0'}}    # rushing_touchdowns
                    ]
                },
                'player_points': {'total': '24.50'},
                'player_projected_points': {'total': '12.16'},
                'percent_started': '45',
                'percent_owned': '82'
            }
        ]
    }

    # Josh Jacobs - RB - GB
    jacobs_data = {
        'player': [
            {
                'player_key': '449.p.31023',
                'selected_position': {'position': 'RB'},
                'name': {'full': 'Josh Jacobs'},
                'editorial_team_abbr': 'GB',
                'bye_week': '10',
                'status': 'Final L 25-27 @ Min'
            },
            {
                'player_stats': {
                    'stats': [
                        {'stat': {'stat_id': '9', 'value': '69'}},   # rushing_yards
                        {'stat': {'stat_id': '10', 'value': '1'}},   # rushing_touchdowns
                        {'stat': {'stat_id': '11', 'value': '0'}},   # receptions
                        {'stat': {'stat_id': '12', 'value': '0'}},   # receiving_yards
                        {'stat': {'stat_id': '13', 'value': '0'}},   # receiving_touchdowns
                        {'stat': {'stat_id': '78', 'value': '0'}},   # targets
                        {'stat': {'stat_id': '18', 'value': '1'}}    # fumbles_lost
                    ]
                },
                'player_points': {'total': '10.90'},
                'player_projected_points': {'total': '17.69'},
                'percent_started': '96',
                'percent_owned': '100'
            }
        ]
    }

    # Pat Freiermuth - TE - Pit
    freiermuth_data = {
        'player': [
            {
                'player_key': '449.p.33102',
                'selected_position': {'position': 'TE'},
                'name': {'full': 'Pat Freiermuth'},
                'editorial_team_abbr': 'Pit',
                'bye_week': '9',
                'status': 'Final L 10-29 vs KC'
            },
            {
                'player_stats': {
                    'stats': [
                        {'stat': {'stat_id': '11', 'value': '7'}},   # receptions
                        {'stat': {'stat_id': '12', 'value': '60'}},  # receiving_yards
                        {'stat': {'stat_id': '13', 'value': '0'}},   # receiving_touchdowns
                        {'stat': {'stat_id': '78', 'value': '8'}}    # targets
                    ]
                },
                'player_points': {'total': '11.00'},
                'player_projected_points': {'total': '8.16'},
                'percent_started': '30',
                'percent_owned': '66'
            }
        ]
    }

    # Tyreek Hill - WR - Mia
    hill_data = {
        'player': [
            {
                'player_key': '449.p.29399',
                'selected_position': {'position': 'W/R'},
                'name': {'full': 'Tyreek Hill'},
                'editorial_team_abbr': 'Mia',
                'bye_week': '6',
                'status': 'Final W 20-3 @ Cle'
            },
            {
                'player_stats': {
                    'stats': [
                        {'stat': {'stat_id': '11', 'value': '9'}},   # receptions
                        {'stat': {'stat_id': '12', 'value': '105'}}, # receiving_yards
                        {'stat': {'stat_id': '13', 'value': '0'}},   # receiving_touchdowns
                        {'stat': {'stat_id': '78', 'value': '9'}}    # targets
                    ]
                },
                'player_points': {'total': '19.50'},
                'player_projected_points': {'total': '13.41'},
                'percent_started': '80',
                'percent_owned': '100'
            }
        ]
    }

    # Khalil Shakir - WR - Buf
    shakir_data = {
        'player': [
            {
                'player_key': '449.p.33512',
                'selected_position': {'position': 'W/R'},
                'name': {'full': 'Khalil Shakir'},
                'editorial_team_abbr': 'Buf',
                'bye_week': '12',
                'status': 'Final W 40-14 vs NYJ'
            },
            {
                'player_stats': {
                    'stats': [
                        {'stat': {'stat_id': '11', 'value': '3'}},   # receptions
                        {'stat': {'stat_id': '12', 'value': '25'}},  # receiving_yards
                        {'stat': {'stat_id': '13', 'value': '0'}},   # receiving_touchdowns
                        {'stat': {'stat_id': '78', 'value': '6'}}    # targets
                    ]
                },
                'player_points': {'total': '5.50'},
                'player_projected_points': {'total': '13.14'},
                'percent_started': '55',
                'percent_owned': '85'
            }
        ]
    }

    # Test all players
    league_key = "449.l.123456"  # League key from screenshot
    week = 17  # Week from screenshot

    # Test each player
    for player_data, name in [
        (stroud_data, "C.J. Stroud"),
        (kupp_data, "Cooper Kupp"),
        (williams_data, "Jameson Williams"),
        (jacobs_data, "Josh Jacobs"),
        (freiermuth_data, "Pat Freiermuth"),
        (hill_data, "Tyreek Hill"),
        (shakir_data, "Khalil Shakir")
    ]:
        stats = extract_player_stats(player_data['player'], week=week, league_key=league_key)
        
        # Verify week and league info
        assert stats['week'] == week, f"Expected week {week}, got {stats['week']} for {name}"
        assert stats['season'] == '2024', f"Expected season 2024 (game_id 449), got {stats['season']} for {name}"
        
        # Verify basic stats match
        player_info = player_data['player'][1]
        assert stats['points'] == float(player_info['player_points']['total']), \
            f"Expected points {player_info['player_points']['total']}, got {stats['points']} for {name}"
        assert stats['percent_started'] == int(player_info['percent_started']), \
            f"Expected start% {player_info['percent_started']}, got {stats['percent_started']} for {name}"
        assert stats['percent_owned'] == int(player_info['percent_owned']), \
            f"Expected ros% {player_info['percent_owned']}, got {stats['percent_owned']} for {name}"
        
        # Verify position
        expected_position = player_data['player'][0]['selected_position']['position']
        assert stats['position'] == expected_position, \
            f"Expected position {expected_position}, got {stats['position']} for {name}"
        
        logger.info(f"{name} stats test passed")

if __name__ == "__main__":
    logger.info("Testing stats extraction...")
    test_stats_extraction() 