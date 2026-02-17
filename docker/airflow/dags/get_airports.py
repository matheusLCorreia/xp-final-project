import datetime
import pendulum
import os

import requests as r
import unidecode
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="get_airports",
    start_date=pendulum.datetime(2026, 1, 9, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["xp-project", "current_day"]
)
def get_airports():    
    @task
    def extract_airports():
        r.get(url="https://api.aviationstack.com/v1/airports", params={'access_key': 'ed8f574d48663933f8e46afc4d497b07', 'limit': 100, 'offset': 0})
    
                    
    @task
    def insert_flights(flights_data):
        query = """INSERT INTO public.raw_aviation_hist (aircraft_icao24, aircraft_icaocode, aircraft_regnumber, airline_iatacode, airline_icaocode, airline_name, arrival_actualrunway, arrival_actualtime, arrival_baggage, arrival_delay, arrival_estimatedrunway, arrival_estimatedtime, arrival_gate, arrival_iatacode, arrival_icaocode, arrival_scheduledtime, arrival_terminal, codeshared, departure_actualrunway, departure_actualtime, departure_baggage, departure_delay, departure_estimatedrunway, departure_estimatedtime, departure_gate, departure_iatacode, departure_icaocode, departure_scheduledtime, departure_terminal, flight_iatanumber, flight_icaonumber, flight_number, status, "type") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        
            
        postgres_hook = PostgresHook(postgres_conn_id="aviation_postres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        print(flights_data)
        cur.executemany(query, flights_data)
        
        conn.commit()
        cur.close()
        conn.close()
    
    # insert_flights(load_flights_today())
    #loadData(transformData(extractMunicipios()))
    
    ## OK: loadMesorregioes(extractMesorregioes())
    
    #loadMicrorregioes(extractMicrorregioes())
    
get_airports()