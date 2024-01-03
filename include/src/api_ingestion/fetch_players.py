import requests
import pandas as pd
import os

from include.src.api_ingestion.api_credentials import RAPID_API__KEY, RAPID_API__HOST  # Import API credentials

def fetch_and_save_players_data(players_csv_file_path):

    url = "https://api-nba-v1.p.rapidapi.com/players"

    params = {
        "team":"8",
        "season": "2023"
    }

    headers = {
        "X-RapidAPI-Key": RAPID_API__KEY,
        "X-RapidAPI-Host": RAPID_API__HOST
    }

    response = requests.get(url, headers=headers, params=params)

    df = pd.DataFrame(response.json()['response'])

    # Run some simple data transformation to 'open' the dictionary rows to new columns

    birth_df = df['birth'].apply(pd.Series)
    birth_df.columns = ['birth_' + sub for sub in birth_df.columns]

    nba_df = df['nba'].apply(pd.Series)
    nba_df.columns = ['nba_' + sub for sub in nba_df.columns]

    height_df = df['height'].apply(pd.Series)
    height_df.columns = ['height_' + sub for sub in height_df.columns]

    weight_df = df['weight'].apply(pd.Series)
    weight_df.columns = ['weight_' + sub for sub in weight_df.columns]

    leagues_df = df['leagues'].apply(pd.Series)['standard'].apply(pd.Series)

    df.drop(['birth', 'nba', 'height', 'weight', 'leagues'], axis=1, inplace=True)

    ref_df = pd.concat([df, birth_df, nba_df, height_df, weight_df, leagues_df],
                       axis=1,
                       join='inner'
                       )

    # Check if 'players.parquet' file exists, and if so, delete it
    if os.path.exists(players_csv_file_path):
        os.remove(players_csv_file_path)

    # Write the Parquet file to the specified folder
    ref_df.to_csv(players_csv_file_path, index=False)

# # Example usage
# players_csv_file_path = 'include/raw_datasets/players.csv'
# fetch_and_save_players_data(players_csv_file_path)