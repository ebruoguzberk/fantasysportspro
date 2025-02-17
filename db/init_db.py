def create_tables():
    """Create all required tables"""
    cursor = db.cursor()
    
    # Create league_draft_results table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS league_draft_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            league_key TEXT NOT NULL,
            season TEXT,
            round INTEGER,
            pick INTEGER,
            team_key TEXT,
            player_key TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (league_key) REFERENCES leagues (league_key),
            FOREIGN KEY (team_key) REFERENCES teams (team_key),
            FOREIGN KEY (player_key) REFERENCES players (player_key)
        )
    """) 