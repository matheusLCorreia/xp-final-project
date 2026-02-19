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
from zoneinfo import ZoneInfo

tz = ZoneInfo('America/Sao_Paulo')

@dag(
    dag_id="extract_flight_schedule_bronze",
    start_date=pendulum.datetime(2026, 1, 9, tz="America/Sao_Paulo"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["xp-project", "current_day"],
    schedule='5 8,16 * * *'
)
def extract_flight_schedule_bronze():    
    @task
    def flights_bronze():
        BRONZE_FOLDER = os.getenv('BRONZE_LAYER_FOLDER')
        iata_codes = ['VCP', 'GRU', 'CGH', 'BSB']
        
        current_date = datetime.datetime.now(tz=tz)
        _day = current_date.strftime('%d')
        _month = current_date.strftime('%m')
        _year = current_date.strftime('%Y')
        _hour = current_date.hour
        # _minute = current_date.strftime('%M')
        
        for airport in iata_codes:
            res = r.get(url=f'https://api.aviationstack.com/v1/timetable?iataCode={airport}&type=departure', params={'access_key': 'ed8f574d48663933f8e46afc4d497b07'})
            try:
                tag = 'evening' if _hour >= 12 else 'morning'
                save = open(f"{BRONZE_FOLDER}/{airport}_{_year}{_month}{_day}_{tag}.json", "w")
                
                save.write(json.dumps(res.json()))
                save.close()
            except Exception as error:
                print(error)
                sys.exit(1)
            time.sleep(62)
    flights_bronze()
                
    
extract_flight_schedule_bronze()