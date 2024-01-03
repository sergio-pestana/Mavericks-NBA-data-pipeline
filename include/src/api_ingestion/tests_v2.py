import requests
import pandas as pd

url = "https://api-nba-v1.p.rapidapi.com/players/statistics"

querystring = {"game":"10403"}

headers = {
	"X-RapidAPI-Key": "1eec4bd524mshff33a987ec5d838p15ca2ejsn788465509cc0",
	"X-RapidAPI-Host": "api-nba-v1.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()['response']

df = pd.DataFrame(data)

df['player_id'] = df['player'].apply(pd.Series)[['id']]
df['game_id'] = df['game'].apply(pd.Series)[['id']]
df['team_id'] = df['team'].apply(pd.Series)[['id']]

print(df)