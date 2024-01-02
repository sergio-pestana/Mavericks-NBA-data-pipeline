import requests
import os
from api_credentials import RAPID_API__KEY, RAPID_API__HOST

url = "https://api-nba-v1.p.rapidapi.com/games"

querystring = {"season":"2021", "team":"8"}

headers = {
	"X-RapidAPI-Key": RAPID_API__KEY,
	"X-RapidAPI-Host": RAPID_API__HOST
}

response = requests.get(url, headers=headers, params=querystring)

print(response)
