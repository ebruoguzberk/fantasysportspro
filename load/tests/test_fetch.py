def parse_player_data(player_data):
    """Parse individual player data from the API response"""
    if isinstance(player_data, list):
        player_data = player_data[0]
    
    player = {}
    
    # Print raw data for debugging
    print(f"Raw player data: {player_data}")
    
    try:
        for item in player_data:
            if isinstance(item, dict):
                if 'player_key' in item:
                    player['player_key'] = item['player_key']
                elif 'player_id' in item:
                    player['player_id'] = item['player_id']
                elif 'name' in item and isinstance(item['name'], dict):
                    player['name'] = item['name'].get('full')
                elif 'editorial_team_full_name' in item:
                    player['team'] = item['editorial_team_full_name']
                elif 'display_position' in item:
                    player['position'] = item['display_position']
                elif 'status' in item:
                    player['status'] = item['status']
                elif 'injury_note' in item:
                    player['injury_note'] = item['injury_note']
                elif 'headshot' in item and isinstance(item['headshot'], dict):
                    player['headshot_url'] = item['headshot'].get('url')
                elif 'uniform_number' in item:
                    player['uniform_number'] = item['uniform_number']
                elif 'eligible_positions' in item:
                    player['eligible_positions'] = json.dumps(item['eligible_positions'])
                elif 'selected_position' in item:
                    player['selected_position'] = json.dumps(item['selected_position'])
                elif 'editorial_player_key' in item:
                    player['editorial_player_key'] = item['editorial_player_key']
                elif 'image_url' in item:
                    player['image_url'] = item['image_url']
                elif 'is_undroppable' in item:
                    player['is_undroppable'] = item['is_undroppable']
                elif 'has_player_notes' in item:
                    player['has_player_notes'] = item['has_player_notes']
                elif 'player_stats' in item:
                    player['player_stats'] = json.dumps(item['player_stats'])
                elif 'player_points' in item:
                    player['player_points'] = json.dumps(item['player_points'])
                elif 'draft_analysis' in item:
                    player['draft_analysis'] = json.dumps(item['draft_analysis'])
                elif 'season_stats' in item:
                    player['season_stats'] = json.dumps(item['season_stats'])
    except Exception as e:
        print(f"Error parsing player data: {e}")
        print(f"Problematic data: {player_data}")
    
    return player 