import datetime
import pendulum
import os

import requests as r
import unidecode
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import psycopg2
import sys
import pandas as pd
from psycopg2.extras import execute_values
from zoneinfo import ZoneInfo

fields = ['identifier', 'aircraft_icao24', 'aircraft_icaocode', 'aircraft_regnumber', 'airline_iatacode', 'airline_icaocode', 'airline_name', 'arrival_actualrunway', 'arrival_actualtime', 'arrival_baggage', 'arrival_delay', 'arrival_estimatedrunway', 'arrival_estimatedtime', 'arrival_gate', 'arrival_iatacode', 'arrival_icaocode', 'arrival_scheduledtime', 'arrival_terminal', 'codeshared_airline_iatacode', 'codeshared_airline_icaocode', 'codeshared_airline_name', 'codeshared_flight_iatanumber', 'codeshared_flight_icaonumber', 'codeshared_flight_number', 'departure_actualrunway', 'departure_actualtime', 'departure_baggage', 'departure_delay', 'departure_estimatedrunway', 'departure_estimatedtime', 'departure_gate', 'departure_iatacode', 'departure_icaocode', 'departure_scheduledtime', 'departure_terminal', 'flight_iatanumber', 'flight_icaonumber', 'flight_number', 'status', "type"]

tz = ZoneInfo('America/Sao_Paulo')
current_date = datetime.datetime.now(tz=tz)
_day = current_date.strftime('%d')
_month = current_date.strftime('%m')
_year = current_date.strftime('%Y')
_hour = current_date.hour

@dag(
    dag_id="raw_process",
    start_date=pendulum.datetime(2026, 1, 9, tz="UTC"),
    tags=["xp-project", "process"],
    # schedule='@daily'
)
def raw_process():    
    @task
    def load_flights_today():
        BRONZE_FOLDER = os.getenv('BRONZE_LAYER_FOLDER')
        
        iata_codes = ['VCP', 'GRU', 'CGH', 'BSB']
        # current_date = datetime.datetime.now()
        # _day = current_date.strftime('%d')
        # _month = current_date.strftime('%m')
        # _year = current_date.strftime('%Y')
        # _hour = current_date.hour
        # # _minute = current_date.strftime('%M')
        
        tag = 'evening' if _hour >= 12 else 'morning'
        hist = []
        for airport in iata_codes:
            data = open(f"{BRONZE_FOLDER}/{airport}_{_year}{_month}{_day}_{tag}.json", "r")
            hist.extend(json.loads(data.read())['data'])
                
            data.close()
            
        # return hist
        print("LEN ANTES: ",len(hist))
        res = transform_flights(hist)
        
        return res
    
    def transform_flights(flights):
        data_res = []
        for i in range(len(flights)):
            data_res.append(recursive_fields(flights[i]).copy())
                
        return data_res
    
    def recursive_fields(row, fd="", all_fields={}):
        all_fields_new = all_fields
        for _field in dict(row).keys():
            if type(row[_field]) == dict:
                field_concat = _field if fd == "" else f"{fd}_{_field}"
                all_fields = recursive_fields(row[_field], field_concat, all_fields_new)
            else:
                if fd == "": final = _field.lower()
                else: final = f"{fd}_{_field}".lower()
                value = 'None' if row[_field] == None else row[_field]
                
                all_fields_new[final.lower()] = value
        
        return all_fields_new
    
    def format_data(p_data):
        all_data = []
        for row in p_data[:]:
            # line = {}
            line = []
            try:
                
                line.append(f"{str(row['departure_iatacode'])}_{str(row['flight_number'])}")
                for f in fields[1:]: # ignoring first field. 
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
    def insert_flights(flights_data):
        
        query = f"""INSERT INTO public.raw_flights ({','.join(fields)}) VALUES ({','.join(["%s" for i in fields])})"""

        postgres_hook = PostgresHook(postgres_conn_id="aviation_postres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        try:
            new_data = format_data(flights_data)
            cur.executemany(query, new_data)
            # execute_values(cur, query, new_data)
        except Exception as error:
            print(error)
            sys.exit(1)
        conn.commit()
            
        cur.close()
        conn.close()
    
    insert_flights(load_flights_today())
    
raw_process()