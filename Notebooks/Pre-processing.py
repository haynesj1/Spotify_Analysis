import pandas as pd

df = pd.read_csv('/Users/student/Documents/GitHub/Spotify_Analysis/Data/playlist.csv')

# Preprocessing
df['track_add_date'] = pd.to_datetime(df['track_add_date'])
df['album_release_date'] = pd.to_datetime(df['album_release_date'])
df['track_age_days'] = (pd.Timestamp.now() - df['track_add_date']).dt.days
df_no_duplicates = df.drop_duplicates(subset='track_name')

# Create a new dataframe to store single artist and their popularity
single_artist_popularity = pd.DataFrame(columns=['artist', 'popularity'])

for index, row in df_no_duplicates.iterrows():
    artists = eval(row['name_of_artists'])
    num_artists = len(artists)
    popularity_per_artist = row['track_popularity'] / num_artists

    rows_to_concat = []
    for artist in artists:
        row_data = pd.DataFrame({'artist': [artist], 'popularity': [popularity_per_artist]})
        rows_to_concat.append(row_data)

    single_artist_popularity = pd.concat([single_artist_popularity] + rows_to_concat, ignore_index=True)

summed_artist_popularity = single_artist_popularity.groupby('artist')['popularity'].sum().reset_index().sort_values('popularity', ascending=False)
summed_artist_popularity_20 = summed_artist_popularity.head(20)

spotify_processed_data = summed_artist_popularity_20
spotify_processed_data.to_parquet('/Users/student/Documents/GitHub/Flask-Portfolio/Data.parquet')
