import requests
import pandas as pd

from api_credentials import RAPID_API__KEY, RAPID_API__HOST

def get_game_total_stats(game_id):
	url = "https://api-nba-v1.p.rapidapi.com/games/statistics"

	params = {"id": f"{game_id}"}

	headers = {
		"X-RapidAPI-Key": RAPID_API__KEY,
		"X-RapidAPI-Host": RAPID_API__HOST
	}

	response = requests.get(url, headers=headers, params=params)

	return response.json()['response']


games_df = pd.read_csv('include/raw_datasets/games.csv')
games_stats_df = pd.read_csv('include/raw_datasets/games_stats.csv')

finshed_games_list = games_df[games_df['status_long']=='Finished']['id'].to_list()
game_stats_filled = games_stats_df['game_id'].unique()

diff_game_ids = list(set(finshed_games_list) - set(game_stats_filled))

if len(diff_game_ids) == 1:
	print("All games were fetched.")
else: 
	df_list = []

	for id in finshed_games_list:
		data = get_game_total_stats(game_id=id)

		df = pd.DataFrame(data)
		df['game_id'] = id

		df['team_id'] = df['team'].apply(pd.Series).loc[:,'id']

		stats_df = df['statistics'].apply(pd.Series)[0].apply(pd.Series)

		ref_df = pd.concat(
							[df, stats_df],
							axis=1,
							join='inner'
						)

		df_list.append(ref_df)

	concatenated_df = pd.concat(df_list, axis=0)
	final_df = concatenated_df.sort_values(by='game_id')
	final_df.drop(['team', 'statistics'], axis=1, inplace=True)

	final_df.to_csv("include/raw_datasets/games_stats.csv", index=False)