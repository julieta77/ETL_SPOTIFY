
# Welcome to my ETL project with Spotify.

![image](https://github.com/julieta77/ETL_SPOTIFY/blob/main/Graph.png)

## Objective:

The objective of this project was to make an automated ETL with the Spotify playlist and then be analyzed

## Development:

In this project for ETL automation we use Airflow together with Docker. Make sure you have Docker installed here I leave the link [download](https://www.docker.com/get-started/) I advise you to see a tutorial on how to download it regardless of your operating system. They also have to be registered in Spotify for developers to interact with the api I leave a [tutorial](https://ichi.pro/es/introduccion-a-la-api-y-spotipy-de-spotify-28021403039035)

Steps to follow:

* Clone the GitHub repository to your local machine using the following command:

```bash
!git clone https://github.com/julieta77/ETL_SPOTIFY/tree/main
!cd https://github.com/julieta77/ETL_SPOTIFY/tree/main
```
* Run the following command to launch the Airflow Docker container:
```docker
docker-compose build
docker ps 
```
*  Once the container has started successfully, open your web browser and go to the following address will take you to the Airflow web interface, where you can manage your workflows:

```arduino
http://localhost:8080
```

Make sure to configure and schedule your ETL workflow according to your needs using Airflow's DAGs and tasks. In my_dag.py do not forget to put your keys for Spotipy authentication.
