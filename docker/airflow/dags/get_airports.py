import datetime
import pendulum
import os

import requests as r
# import unidecode
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import sys

BRONZE_FOLDER = os.getenv('BRONZE_LAYER_FOLDER')
fields = ['id', 'gmt', 'airport_id', 'iata_code', 'city_iata_code', 'icao_code', 'country_iso2', 'geoname_id', 'latitude', 'longitude', 'airport_name', 'country_name', 'phone_number', 'timezone']

@dag(
    dag_id="extract_airports",
    start_date=pendulum.datetime(2026, 1, 9, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["xp-project", "extract"]
)
def get_airports():    
    @task
    def extract_airports():
        offset = 6800
        count = 68
        while (count < 69): # 67, 6685 total airports
            print(f"Gettin offset {count}/{offset}")
            # ed8f574d48663933f8e46afc4d497b07
            res = r.get(url="https://api.aviationstack.com/v1/airports", params={'access_key': 'bdc876930322613053834c5f0cea553d', 'limit': 100, 'offset': offset})
            
            save = open(f"{BRONZE_FOLDER}/airports_{offset}.json", "w")
            data = res.json()
            
            save.write(json.dumps(data))
            save.close()
            offset = offset + 100
            count = count + 1
    
    @task
    def read_from_file():
        from pathlib import Path
        data = []
        for i in Path(f'{BRONZE_FOLDER}').iterdir():
            if i.is_file() and i.name.find('airport') != -1:
                with open(f'{BRONZE_FOLDER}/{i.name}', 'r') as ap:
                    it = json.loads(ap.read())
                    if 'data' in it.keys():
                        data.extend(it['data'])
        
        print("SIZE: ", len(data))
        print(data[:5])
        
        return data
    
    def format_data(p_data):
        all_data = []
        for row in p_data[:]:
            line = []
            try:
                for f in fields:
                    if f in list(row.keys()):
                        if row[f] in [None, 'None', 'null', ""]:
                            line.append(None)
                        else:
                            line.append(row[f])
                    else:
                        line.append(None)
                all_data.append(line)
                del line
            except Exception as error:
                print(error, row)
        return all_data
    
    @task
    def insert_airports(airports_data):
        try:
            query = """INSERT INTO public.airports (id, gmt, airport_id, iata_code, city_iata_code, icao_code, country_iso2, geoname_id, latitude, longitude, airport_name, country_name, phone_number, timezone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                
            postgres_hook = PostgresHook(postgres_conn_id="aviation_postres")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            
            data = format_data(airports_data)
            cur.execute("truncate public.airports;")
            cur.executemany(query, data)
            
            conn.commit()
            cur.close()
            conn.close()
        except Exception as error:
            conn.rollback()
            print(error)
            sys.exit(1)
    
    # extract_airports()
    insert_airports(read_from_file())
    
get_airports()