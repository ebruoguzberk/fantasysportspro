import os
import json
import logging
from datetime import datetime
from yahoo_api import YahooFantasyAPI
from dotenv import load_dotenv
from types import SimpleNamespace

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def datetime_to_str(obj):
    """Convert datetime objects to ISO format strings"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def str_to_datetime(token_data):
    """Convert ISO format strings back to datetime objects"""
    if isinstance(token_data, dict):
        if 'token_expiry' in token_data:
            token_data['token_expiry'] = datetime.fromisoformat(token_data['token_expiry'])
        if 'last_update' in token_data:
            token_data['last_update'] = datetime.fromisoformat(token_data['last_update'])
    return token_data

def load_tokens():
    """Load tokens from auth.json if it exists"""
    try:
        if os.path.exists('auth.json'):
            with open('auth.json', 'r') as f:
                data = json.load(f)
                print("Found existing authentication file")
                return str_to_datetime(data)
    except Exception as e:
        print(f"Error loading auth file: {e}")
    return None

def save_tokens(tokens):
    """Save tokens to auth.json"""
    try:
        # Convert datetime objects to strings before saving
        serializable_tokens = {
            'access_token': tokens['access_token'],
            'refresh_token': tokens['refresh_token'],
            'token_expiry': datetime_to_str(tokens['token_expiry']),
            'last_update': datetime_to_str(tokens['last_update'])
        }
        with open('auth.json', 'w') as f:
            json.dump(serializable_tokens, f)
            print("Saved authentication tokens")
    except Exception as e:
        print(f"Error saving auth file: {e}")

def authenticate():
    load_dotenv()
    session_state = SimpleNamespace()
    api = YahooFantasyAPI(session_state=session_state)
    
    # Try to load existing tokens
    tokens = load_tokens()
    if tokens:
        print("Loading existing tokens...")
        session_state.yahoo_tokens = tokens
        api = YahooFantasyAPI(session_state=session_state)
        
        try:
            # Test the connection with a simple API call
            api.get_user_info()
            print("Successfully connected with existing tokens!")
            return api
        except Exception as e:
            print(f"Existing tokens failed: {e}")
            print("Need to re-authenticate...")
    
    # If we get here, we need to do a fresh authentication
    print("\nStarting new authentication process...")
    auth_url = api.get_authorization_url()
    print(f"\nPlease visit this URL to authorize the application:\n{auth_url}")
    
    auth_code = input("\nEnter the authorization code: ")
    
    if api.handle_authorization(auth_code):
        print("Authorization successful!")
        # Save the new tokens
        if hasattr(session_state, 'yahoo_tokens'):
            save_tokens(session_state.yahoo_tokens)
        return api
    else:
        print("Authorization failed!")
        return None

def main():
    api = authenticate()
    if api:
        print("\nAuthentication successful! You can now make API calls.")
        api.get_league_info("nfl.l.686965")
    else:
        print("\nFailed to authenticate.")

if __name__ == "__main__":
    main()