import requests
import pandas as pd

from include.src.api_ingestion.api_credentials import RAPID_API__KEY, RAPID_API__HOST  # Import API credentials

def get_game_total_stats(game_id):
    """
    Fetches game statistics based on a game ID using a RapidAPI endpoint.
    Args:
    - game_id: ID of the game for which statistics are fetched.
    Returns:
    - JSON response containing game statistics.
    """
    url = "https://api-nba-v1.p.rapidapi.com/games/statistics"

    params = {"id": f"{game_id}"}

    headers = {
        "X-RapidAPI-Key": RAPID_API__KEY,
        "X-RapidAPI-Host": RAPID_API__HOST
    }

    response = requests.get(url, headers=headers, params=params)

    return response.json()['response']

def fetch_and_update_game_stats(games_csv, games_stats_csv):
    """
    Fetches game statistics for games that are finished but not already in the existing game statistics DataFrame.
    Updates the games_stats_df DataFrame with new game statistics.
    Args:
    - games_df: DataFrame containing game information.
    - games_stats_df: DataFrame containing existing game statistics.
    """

    # Read the initial DataFrames
    games_df = pd.read_csv(games_csv)
    games_stats_df = pd.read_csv(games_stats_csv)

    # Get the list of finished games and games with existing stats
    finished_games_list = games_df[games_df['status_long'] == 'Finished']['id'].to_list()
    game_stats_filled = games_stats_df['game_id'].unique()

    # Find games that need stats to be fetched
    diff_game_ids = list(set(finished_games_list) - set(game_stats_filled))

    print(f"{len(diff_game_ids)} games added.")

    if len(diff_game_ids) == 0:
        print("All games were fetched.")
    else:
        df_list = []

        # Fetch stats for games and update the DataFrame
        for game_id in diff_game_ids:
            data = get_game_total_stats(game_id=game_id)

            df = pd.DataFrame(data)
            df['game_id'] = game_id

            df['team_id'] = df['team'].apply(pd.Series).loc[:, 'id']

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

        # Update the existing game statistics DataFrame
        return final_df.to_csv("include/raw_datasets/total_game_stats.csv", index=False)


games_df = 'include/raw_datasets/games.csv'
games_stats_df = 'include/raw_datasets/total_game_stats.csv'

# Call the function to fetch and update game stats
# fetch_and_update_game_stats(games_df, games_stats_df)
