import datetime
import pendulum
import os
import sys
import requests as r
import unidecode
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import time

@dag(
    dag_id="extract_flight_schedule_bronze",
    start_date=pendulum.datetime(2026, 1, 9, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["xp-project", "current_day"]
)
def extract_flight_schedule_bronze():    
    @task
    def flights_bronze():
        BRONZE_FOLDER = os.getenv('BRONZE_LAYER_FOLDER')
        iata_codes = ['VCP', 'GRU', 'CGH', 'BSB']
        
        current_date = datetime.datetime.now()
        _day = current_date.strftime('%d')
        _month = current_date.strftime('%m')
        _year = current_date.strftime('%Y')
        _hour = current_date.strftime('%H')
        _minute = current_date.strftime('%M')
        
        import subprocess as sub
        for airport in iata_codes:
            res = r.get(url=f'https://api.aviationstack.com/v1/timetable?iataCode={airport}&type=departure', params={'access_key': 'ed8f574d48663933f8e46afc4d497b07'})
            try:
                # ret = sub.run(['ls', '-ltr'], capture_output=True, text=True)
                # print(ret.stdout, ret.returncode)
                # save = open(f"{BRONZE_FOLDER}/{airport}_{_year}{_month}{_day}_{_hour}{_minute}.json", "w")
                save = open(f"{BRONZE_FOLDER}/{airport}_{_year}{_month}{_day}.json", "w")
                # save.write("teste")
                save.write(json.dumps(res.json()))
                save.close()
            except Exception as error:
                print(error)
                sys.exit(1)
            time.sleep(62)
    flights_bronze()
                
    
extract_flight_schedule_bronze()