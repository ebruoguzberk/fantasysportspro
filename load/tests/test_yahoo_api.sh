#!/bin/bash

# Load environment variables from .env
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo ".env file not found"
    exit 1
fi

# Check if required variables are set
if [ -z "$YAHOO_CONSUMER_KEY" ] || [ -z "$YAHOO_CONSUMER_SECRET" ]; then
    echo "Missing required environment variables"
    exit 1
fi

# Base64 encode client credentials
AUTH_HEADER=$(echo -n "${YAHOO_CONSUMER_KEY}:${YAHOO_CONSUMER_SECRET}" | base64)

# Function to test if access token is valid
test_token() {
    local token=$1
    echo "Testing access token..."
    response=$(curl -s -X GET \
        'https://fantasysports.yahooapis.com/fantasy/v2/game/nfl?format=json' \
        -H "Authorization: Bearer $token" \
        -H 'Content-Type: application/json')
    
    if [[ $response == *"error"* ]]; then
        return 1
    else
        return 0
    fi
}

# Check if we have a saved token
ACCESS_TOKEN=""
if [ -f "auth.json" ]; then
    echo "Found existing auth.json file"
    ACCESS_TOKEN=$(jq -r '.access_token' auth.json)
    
    if test_token "$ACCESS_TOKEN"; then
        echo "Existing token is valid!"
    else
        echo "Existing token is invalid, need to get new one"
        ACCESS_TOKEN=""
    fi
fi

# If we don't have a valid token, get a new one
if [ -z "$ACCESS_TOKEN" ]; then
    echo "Getting authorization URL..."
    AUTH_URL="https://api.login.yahoo.com/oauth2/request_auth?client_id=${YAHOO_CONSUMER_KEY}&redirect_uri=oob&response_type=code&scope=openid%20fspt-r"
    echo "Please visit: $AUTH_URL"

    echo -n "Enter the authorization code: "
    read AUTH_CODE

    echo "Getting access token..."
    TOKEN_RESPONSE=$(curl -s -X POST \
        'https://api.login.yahoo.com/oauth2/get_token' \
        -H "Authorization: Basic $AUTH_HEADER" \
        -H 'Content-Type: application/x-www-form-urlencoded' \
        --data-urlencode "grant_type=authorization_code" \
        --data-urlencode "redirect_uri=oob" \
        --data-urlencode "code=$AUTH_CODE")

    # Extract access token
    ACCESS_TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.access_token')
    REFRESH_TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.refresh_token')

    if [ -z "$ACCESS_TOKEN" ]; then
        echo "Failed to get access token"
        echo "Response: $TOKEN_RESPONSE"
        exit 1
    fi

    # Save tokens to auth.json
    echo "Saving tokens to auth.json..."
    echo "{
        \"access_token\": \"$ACCESS_TOKEN\",
        \"refresh_token\": \"$REFRESH_TOKEN\",
        \"created_at\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"
    }" > auth.json

    echo "Access token obtained and saved successfully!"
fi

echo "YAHOO_CONSUMER_KEY: $YAHOO_CONSUMER_KEY"
echo "YAHOO_CONSUMER_SECRET: $YAHOO_CONSUMER_SECRET"


# # 5. Get All Leagues for User
# echo -e "\nFetching All Leagues..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/leagues?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'



# echo "Making API calls..."

# # 1. Get User Metadata
# echo -e "\nFetching User Metadata..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/out=metadata?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# 2. Get All Games for User
# echo -e "\nFetching All Games..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 3. Get Games with League Data
# echo -e "\nFetching Games with Leagues..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games/leagues?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# 4. Get Games with Teams
# echo -e "\nFetching Games with Teams..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games/teams?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'


# # 6. Get All Teams Owned by User
# echo -e "\nFetching All Teams..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/teams?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 7. Get Multiple Resources at Once
# echo -e "\nFetching Multiple Resources..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1;out=games,leagues,teams?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 8. Get Games by Type
# echo -e "\nFetching Games by Type..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_types=full?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 9. Get Games by Codes
# echo -e "\nFetching Games by Codes..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_codes=nfl,nba,mlb,nhl?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 10. Get Games by Seasons
# echo -e "\nFetching Games by Seasons..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;seasons=2024,2023?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 11. Get Available Games
# echo -e "\nFetching Available Games..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;is_available=1?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 12. Get User's Game Leagues with Settings
# echo -e "\nFetching Game Leagues with Settings..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games/leagues/settings?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# 13. Get User's Game Leagues with Standings
# echo -e "\nFetching Game Leagues with Standings..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games/leagues/standings?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 14. Get User's Game Leagues with Scoreboard
# echo -e "\nFetching Game Leagues with Scoreboard..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games/leagues/scoreboard?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# 15. Get User's Teams with Roster
echo -e "\nFetching Teams with Roster..."
curl -s -X GET \
    'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/teams/roster?format=json' \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H 'Content-Type: application/json' | jq '.'

# # 16. Get User's Teams with Stats
# echo -e "\nFetching Teams with Stats..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/teams/stats?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'

# # 17. Get User's Teams with Standings
# echo -e "\nFetching Teams with Standings..."
# curl -s -X GET \
#     'https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/teams/standings?format=json' \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H 'Content-Type: application/json' | jq '.'