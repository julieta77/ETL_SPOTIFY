import pandas as pd 
import spotipy 
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import create_engine

client_id = "b2bef239d49d4dbbaa11521289957b58"
client_secret = "ba2361833ec149b69a7a1ad8b941a03b"

scope = "user-read-recently-played"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri='https://mail.google.com/', scope=scope))



def extract(limit=None,after_date=None,before_date=None):
    results = sp.current_user_recently_played(limit=limit,after=after_date,before=before_date)
    Data = []
    for track in  results['items']:
        artist_uri = track["track"]["artists"][0]["uri"]
        artist_info = sp.artist(artist_uri)
        track_uri = track["track"]["uri"]
        Data.append({'track_uri':track_uri,'song_name':track['track']['name'],'listen_date':track['played_at'],'artist_name':track['track']['artists'][0]['name'],'album_name':track['track']['album']['name'],
                       'lanzamiento_album':track['track']['album']['release_date'], 'genero':artist_info['genres'],'track_pop':track["track"]["popularity"] , 'other_info':sp.audio_features(track_uri)[0]})
    df = pd.DataFrame(Data)
    return df


def trasform(data):
    data['listen_date'] = pd.to_datetime(data['listen_date'])
    data['listen_date'] = data['listen_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    data2 = pd.json_normalize(data['other_info'])
    data.drop('other_info', axis=1,inplace=True)
    return data, data2


def load(df1,df2):
    conexion = create_engine(f"mysql+pymysql://root:{'juli4409'}@localhost:3306/Spotify") 
    df1.to_sql(name='Songs',con=conexion,if_exists= 'append',index=False)
    df2.to_sql(name='OtherInfoSongs',con=conexion,if_exists= 'append',index=False)
