######################################################### Importing the necessary libraries ######################################################
import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy import create_engine
from password import client_id,client_secret,mysql, redirect_uri
from airflow import DAG 
from datetime import datetime 
from airflow.operators.python import PythonOperator
import pandas as pd 


######################################################### Spotify API authentication #############################################################
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)


def extract(playlist_id,**context):
    """ This function receives as a parameter the id of a Spotify playlist in order to extract data """
    playlist = sp.playlist(playlist_id)
    Data = []
    for track in playlist['tracks']['items']:
        artist_uri = track["track"]["artists"][0]["uri"]
        artist_info = sp.artist(artist_uri)
        track_uri = track["track"]["uri"]
    
        Data.append({'track_uri':track_uri,'song_name':track['track']['name'],'artist_name':track['track']['artists'][0]['name'],'album_name':track['track']['album']['name'],
                       'launch_album':track['track']['album']['release_date'], 'gender':artist_info['genres'],'track_pop':track["track"]["popularity"] , 'other_info':sp.audio_features(track_uri)[0]})
    df = pd.DataFrame(Data)
    context['ti'].xcom_push(key='extract_data', value=df)


def trasform(**context):
    """ From the dataframe returned from the extract function we transform the data with the necessary requirements """
    data = context['ti'].xcom_pull(key='extract_data')
    data['gender'] = data['gender'].apply(lambda lista : lista[0] if lista != [] else None) 
    data['launch_album'] =  pd.to_datetime(data['launch_album'])
    data2 = pd.json_normalize(data['other_info'])
    data.drop('other_info', axis=1,inplace=True)
    context['ti'].xcom_push(key='transformed_data1', value=data)
    context['ti'].xcom_push(key='transformed_data2', value=data2)
    


def load(**context):
    """ We load the dataframes to the mysql database """
    df1 = context['ti'].xcom_pull(key='transformed_data1')
    df2 = context['ti'].xcom_pull(key='transformed_data2') 
    conexion = create_engine(f"mysql+pymysql://root:{mysql}@host.docker.internal:3306/spotify") 
    df1.to_sql(name='Songs',con=conexion,if_exists= 'append',index=False)
    df2.to_sql(name='OtherInfoSongs',con=conexion,if_exists= 'append',index=False)

############################################# We created the dag to automate the ETL #############################################################
#start_date = datetime.now().replace(hour=10, minute=49, second=0) to take a test
with DAG('ETL_SPOTIFY', start_date=datetime(2023,5,26), 
        schedule_interval='@weekly', catchup=False) as dag:


        extrac = PythonOperator(task_id='extraer_data_spotify',
                               python_callable=extract,
                               op_kwargs={'playlist_id':'https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f'},
                               provide_context=True)

        trasforms = PythonOperator(task_id='transform_data_spotify',
                                python_callable=trasform,
                                provide_context=True)

        loads = PythonOperator(task_id='load_data_spotify',
                               python_callable=load,
                               provide_context=True)

        extrac >> trasforms >> loads


