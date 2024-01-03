import requests
import pandas as pd
import os

from api_credentials import RAPID_API__KEY, RAPID_API__HOST

def fetch_and_save_teams_data(csv_file_path):
    # Set the API endpoint and headers for the API-NBA
    url = "https://api-nba-v1.p.rapidapi.com/teams"

    headers = {
        "X-RapidAPI-Key": RAPID_API__KEY,
        "X-RapidAPI-Host": RAPID_API__HOST
    }

    # Fetch data from the API-NBA using a GET request
    response = requests.get(url, headers=headers)

    # Check if the API request was successful
        # Parse the JSON response to retrieve data
    data = response.json()['response']

    # Create a Pandas DataFrame from the fetched data
    df = pd.DataFrame(data)

    # Run some simple data transformations
    leagues_df = df['leagues'].apply(pd.Series)['standard'].apply(pd.Series)[['conference', 'division']]

    df.drop(['leagues'], axis=1, inplace=True)

    ref_df = pd.concat([df, leagues_df],
                axis=1, 
                join='inner'
                )

    # Check if the Parquet file already exists, and if it does, remove it
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)

    # Convert the Pandas DataFrame to a Parquet file
    ref_df.to_csv(csv_file_path, index=False)


# # Example usage
# fetch_and_save_teams_data('include/raw_datasets/teams.csv')