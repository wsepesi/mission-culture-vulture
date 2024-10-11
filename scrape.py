import pandas as pd
from openai import OpenAI
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from typing import List, Dict
from collections import Counter
import json
import requests
import io
from tqdm import tqdm
import time
from dotenv import load_dotenv
import os

load_dotenv()
# KEYS 
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID') 
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SPOTIFY_REDIRECT_URI = "http://localhost:3000"

# CONSTANTS
PLAYLIST_NAME = "heard in the mission"
OAI_RES = "OAI_RES"
SPOTIFY_ID = "SPOTIFY_ID"
CACHE_PATH = 'data/song_cache.json'

def load_cache(path: str) -> Dict[str, Dict]:
    """
    Cache is a dict that looks like:
    {
        "title1artist1": 
            {
                "OAI_RES": "Yes",
                "SPOTIFY_ID": "1234"
            },
    }
    """
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    
def save_cache(path: str, cache: Dict[str, str]) -> None:
    with open(path, 'w') as f:
        json.dump(cache, f)

def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def classify_spanish_music(df: pd.DataFrame, song_cache: Dict[str, Dict]) -> List[Dict[str, str]]:
    spanish_songs = []
    client = OpenAI() # NOTE: YOU NEED YOUR OAI KEY IN YOUR SYSTEM ENV

    for _, row in tqdm(df.iterrows()):
        title, artist = row['title'], row['artist']

        if title + artist in song_cache:
            # if in the cache, implies the song is already in the playlist. 
            #   however to remain modular we don't dedup here, but in the spotify function
            answer = song_cache[title + artist][OAI_RES] 
        else:
            completion = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You job is to identify whether the song is a Spanish language song or not, given the titel and artist. Reponse ONLY with 'Yes' or 'No'."},
                    {'role': 'user', 'content': "Title: 'Despacito', Artist: 'Luis Fonsi'"},
                    {'role': 'assistant', 'content': "Yes"},
                    {'role': 'user', 'content': f'Title: {title}, Artist: {artist}'}
                ]
            )

            answer = completion.choices[0].message.content

            song_cache[title + artist] = { OAI_RES: answer, SPOTIFY_ID: None }

        if answer == 'Yes':
            spanish_songs.append({'title': title, 'artist': artist})

    return spanish_songs, song_cache

def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri=SPOTIFY_REDIRECT_URI,
        scope="playlist-modify-private playlist-read-private"
    ))

def get_or_create_playlist(sp: spotipy.Spotify) -> str:
    user_id = sp.me()['id']

    playlists = []
    offset = 0
    while True:
        results = sp.current_user_playlists(limit=50, offset=offset)
        playlists.extend(results['items'])
        offset += len(results['items'])
        if not results['next']:
            break

    print(f"Found {len(playlists)} playlists.")
    
    for playlist in playlists:
        if playlist['name'] == PLAYLIST_NAME:
            print(f"Playlist found: {playlist['external_urls']['spotify']}")
            return playlist['id']
    
    new_playlist = sp.user_playlist_create(user_id, PLAYLIST_NAME, public=False)
    print(f"Playlist created: {new_playlist['external_urls']['spotify']}")
    return new_playlist['id']

def get_playlist_tracks(sp: spotipy.Spotify, playlist_id: str) -> List[Dict[str, str]]:
    tracks = []
    results = sp.playlist_tracks(playlist_id)
    tracks.extend([{'id': item['track']['id'], 'uri': item['track']['uri']} for item in results['items']])
    
    while results['next']:
        results = sp.next(results)
        tracks.extend([{'id': item['track']['id'], 'uri': item['track']['uri']} for item in results['items']])
    
    return tracks

def update_spotify_playlist(
        sp: spotipy.Spotify, 
        playlist_id: str, 
        songs: List[Dict[str, str]], 
        song_cache: Dict[str, Dict]
        ) -> None:
    user_id = sp.me()['id']
    existing_tracks = get_playlist_tracks(sp, playlist_id)
    existing_track_ids = [track['id'] for track in existing_tracks]
    tracks_to_add = []

    song_num = 1
    for song in tqdm(songs):
        concat_name = song['title'] + song['artist']
        track_id = None
        if concat_name in song_cache and song_cache[concat_name][SPOTIFY_ID]:
            # Useful in two ways: Songs can be in the list multiple times (this dedups work) and fetches old runs
            #    Again, this would imply we don't need to add it at all, but we're keeping it modular
            track_id = song_cache[concat_name][SPOTIFY_ID]
            # print(track_id)
        else:
            if song_num % 500 == 0:
                time.sleep(15) # to avoid API rate limits
            song_num += 1
            query = f"track:{song['title']} artist:{song['artist']}"
            results = sp.search(q=query, type='track', limit=1)
            
            if results['tracks']['items']:
                track_uri = results['tracks']['items'][0]['uri']
                track_id = results['tracks']['items'][0]['id']
                song_cache[concat_name][SPOTIFY_ID] = track_id

        if track_id not in existing_track_ids:
            tracks_to_add.append(track_uri)
            # print(f"Adding song: {song['title']} by {song['artist']}")

    if tracks_to_add:
        print(f"Adding {len(tracks_to_add)} new tracks to the playlist.")
        for i in range(0, len(tracks_to_add), 100):
            sp.user_playlist_add_tracks(user_id, playlist_id, tracks_to_add[i:i+100])
            print(f"Added {len(tracks_to_add[i:i+100])} new tracks to the playlist.")
    else:
        print("No new tracks to add.")

    remove_duplicates(sp, playlist_id)

    return song_cache

def remove_duplicates(sp: spotipy.Spotify, playlist_id: str) -> None:
    tracks = get_playlist_tracks(sp, playlist_id)
    track_ids = [track['id'] for track in tracks]
    duplicate_ids = [id for id, count in Counter(track_ids).items() if count > 1]

    if duplicate_ids:
        print(f"Removing {len(duplicate_ids)} duplicate ids (there may be multiple duplicates per id).")
        for dup_id in tqdm(duplicate_ids):
            dup_uris = [track['uri'] for track in tracks if track['id'] == dup_id]
            sp.playlist_remove_all_occurrences_of_items(playlist_id, dup_uris[1:])
    else:
        print("No duplicates found in the playlist.")

def get_df_by_curl():
    URL = 'https://walzr.com/bop-spotter/export'  # this will return a CSV on curl, we want to read this in and format into a pd df
    r = requests.get(URL)
    text = r.text
    csv_data = io.StringIO(text)
    df = pd.read_csv(csv_data)
    print(f"Dataframe loaded with {len(df)} rows.")
    return df

def main():
    # CSV_PATH = 'data/songs.csv'
    # df = load_data(CSV_PATH)

    df = get_df_by_curl()
    song_cache = load_cache(CACHE_PATH)
    spanish_songs, song_cache_updated = classify_spanish_music(df, song_cache)
    save_cache(CACHE_PATH, song_cache_updated)
    
    sp = get_spotify_client()
    playlist_id = get_or_create_playlist(sp)
    song_cache_updated = update_spotify_playlist(sp, playlist_id, spanish_songs, song_cache_updated)
    save_cache(CACHE_PATH, song_cache_updated)
    
    playlist_url = sp.playlist(playlist_id)['external_urls']['spotify']
    print(f"Playlist updated: {playlist_url}")

if __name__ == "__main__":
    main()