import requests
import pandas as pd
import os

from include.src.api_ingestion.api_credentials import RAPID_API__KEY, RAPID_API__HOST  # Import API credentials

def fetch_and_save_games_data(csv_file_path):
    """
    This function will fetch only Dallas games. The data will be used to get new games later. 
    """

    url = "https://api-nba-v1.p.rapidapi.com/games"
    
    params = {"season":"2023", "team":"8"}
    
    headers = {
        "X-RapidAPI-Key": RAPID_API__KEY,
        "X-RapidAPI-Host": RAPID_API__HOST
    }   
    
    response = requests.get(url, headers=headers, params=params)
    
    data = response.json()['response']
    
    # Create a Pandas DataFrame
    df = pd.DataFrame(data) 

    # Simple data transformation

    date_df = df['date'].apply(pd.Series)
    date_df['Year'] = date_df['start'].str[:4]
    date_df.columns = ['game_' + sub for sub in date_df.columns] 

    status_df = df['status'].apply(pd.Series)
    status_df.columns = ['status_' + sub for sub in status_df.columns] 

    periods_df = df['periods'].apply(pd.Series)
    periods_df.columns = ['period_' + sub for sub in periods_df.columns] 

    arena_df = df['arena'].apply(pd.Series)
    arena_df.columns = ['arena_' + sub for sub in arena_df.columns]  

    visitors_df = df['teams'].apply(pd.Series)['visitors'].apply(pd.Series)[['id']]
    visitors_df.columns = ['visitor_' + sub for sub in visitors_df.columns] 

    home_df = df['teams'].apply(pd.Series)['home'].apply(pd.Series)[['id']]
    home_df.columns = ['home_' + sub for sub in home_df.columns] 

    visitors_scores_df = df['scores'].apply(pd.Series)['visitors'].apply(pd.Series)
    visitors_scores_df.columns = ['visitor_score_' + sub for sub in visitors_scores_df.columns] 

    home_scores_df = df['scores'].apply(pd.Series)['home'].apply(pd.Series)
    home_scores_df.columns = ['home_score_' + sub for sub in home_scores_df.columns] 


    new_df = df.iloc[:, :3]

    ref_df = pd.concat([new_df, date_df, status_df, periods_df, arena_df, visitors_df, home_df, visitors_scores_df, home_scores_df],
                    axis=1, 
                    join='inner')

    
    # Check if the CSV file already exists and overwrite it
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)
    
    # Save the data as a CSV file
    ref_df.to_csv(csv_file_path, index=False)

# # Example usage:
csv_file_path = "include/raw_datasets/games.csv"
fetch_and_save_games_data(csv_file_path)
