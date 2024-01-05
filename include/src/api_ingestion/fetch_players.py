import requests
import pandas as pd
import os

from include.src.api_ingestion.api_credentials import RAPID_API__KEY, RAPID_API__HOST  # Import API credentials

def fetch_and_save_players_to_csv(teams_csv_file_path, players_csv_file_path):

    # Get all the teams' data and create a Pandas DataFrame
    teams_df = pd.read_csv(teams_csv_file_path)
    teams_id_list = teams_df['id'].to_list()

    # Function to get all the players from each team
    def get_team_players(team_id, season):
        url = "https://api-nba-v1.p.rapidapi.com/players"

        params = {
            "team": str(team_id),
            "season": f"{season}"
        }

        headers = {
            "X-RapidAPI-Key": RAPID_API__KEY,
            "X-RapidAPI-Host": RAPID_API__HOST
        }

        response = requests.get(url, headers=headers, params=params)
        return response.json()['response']

    # Create a list to store DataFrames for each team
    df_list = []

    for team_id in teams_id_list:
        data = get_team_players(team_id, 2023)

        if data:  # Check if the data is not empty
            df = pd.DataFrame(data)
            df['team_id'] = team_id  # Add 'team_id' column to the DataFrame
            df['season'] = 2023  # Add the season column to the DataFrame
            df_list.append(df)

    # Concatenate the DataFrames
    concatenated_df = pd.concat(df_list, ignore_index=True)

    # Run some simple data transformation to 'open' the dictionary rows to new columns

    birth_df = concatenated_df['birth'].apply(pd.Series)
    birth_df.columns = ['birth_' + sub for sub in birth_df.columns]

    nba_df = concatenated_df['nba'].apply(pd.Series)
    nba_df.columns = ['nba_' + sub for sub in nba_df.columns]

    height_df = concatenated_df['height'].apply(pd.Series)
    height_df.columns = ['height_' + sub for sub in height_df.columns]

    weight_df = concatenated_df['weight'].apply(pd.Series)
    weight_df.columns = ['weight_' + sub for sub in weight_df.columns]

    leagues_df = concatenated_df['leagues'].apply(pd.Series)['standard'].apply(pd.Series)

    concatenated_df.drop(['birth', 'nba', 'height', 'weight', 'leagues'], axis=1, inplace=True)

    ref_df = pd.concat([concatenated_df, birth_df, nba_df, height_df, weight_df, leagues_df],
                       axis=1,
                       join='inner'
                       )
 

    # Check if 'players.parquet' file exists, and if so, delete it
    if os.path.exists(players_csv_file_path):
        os.remove(players_csv_file_path)

    # Write the Parquet file to the specified folder
    ref_df.to_csv(players_csv_file_path, index=False)



teams_csv_file_path = 'include/raw_datasets/teams.csv'
players_csv_file_path = 'include/raw_datasets/players.csv'
fetch_and_save_players_to_csv(teams_csv_file_path, players_csv_file_path)