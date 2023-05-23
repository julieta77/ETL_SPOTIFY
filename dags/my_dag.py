from airflow import DAG 
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd 
import spotipy 
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import create_engine
from password import client_id,client_secret,mysql, redirect_uri


sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri, scope="user-read-recently-played"))



def extract(limit=50,after_date=None,before_date=None,**context):
    results = sp.current_user_recently_played(limit=limit,after=after_date,before=before_date)
    Data = []
    for track in  results['items']:
        artist_uri = track["track"]["artists"][0]["uri"]
        artist_info = sp.artist(artist_uri)
        track_uri = track["track"]["uri"]
        Data.append({'track_uri':track_uri,'song_name':track['track']['name'],'listen_date':track['played_at'],'artist_name':track['track']['artists'][0]['name'],'album_name':track['track']['album']['name'],
                       'lanzamiento_album':track['track']['album']['release_date'], 'genero':artist_info['genres'],'track_pop':track["track"]["popularity"] , 'other_info':sp.audio_features(track_uri)[0]})
    df = pd.DataFrame(Data)
    context['ti'].xcom_push(key='extract_data', value=df)


def trasform(**context):
    data = context['ti'].xcom_pull(key='extract_data')
    data['listen_date'] = pd.to_datetime(data['listen_date'])
    data['listen_date'] = data['listen_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    data2 = pd.json_normalize(data['other_info'])
    data.drop('other_info', axis=1,inplace=True)
    context['ti'].xcom_push(key='transformed_data1', value=data)
    context['ti'].xcom_push(key='transformed_data2', value=data2)
    


def load(**context):
    df1 = context['ti'].xcom_pull(key='transformed_data1')
    df2 = context['ti'].xcom_pull(key='transformed_data2')
    conexion = create_engine(f"mysql+pymysql://root:{mysql}@localhost:3306/Spotify") 
    df1.to_sql(name='Songs',con=conexion,if_exists= 'append',index=False)
    df2.to_sql(name='OtherInfoSongs',con=conexion,if_exists= 'append',index=False)



with DAG('My_dag', start_date=datetime(2023,5,23),
        schedule_interval='@weekly', catchup=False) as dag:

        extrac = PythonOperator(task_id='extraer_data_spotify',
                               python_callable=extract,
                               op_kwargs={'limit': 50, 'after_date': None, 'before_date': None},
                               provide_context=True)

        trasforms = PythonOperator(task_id='transform_data_spotify',
                                python_callable=trasform,
                                provide_context=True)

        loads = PythonOperator(task_id='load_data_spotify',
                               python_callable=load,
                               provide_context=True)

        extrac >> trasforms >> loads


