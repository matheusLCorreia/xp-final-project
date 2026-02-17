import datetime
import pendulum
import os

import requests as r
import unidecode
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import psycopg2
from psycopg2.extras import Json
import sys
import pandas as pd
from psycopg2.extras import execute_values


fields = ['aircraft_icao24', 'aircraft_icaocode', 'aircraft_regnumber', 'airline_iatacode', 'airline_icaocode', 'airline_name', 'arrival_actualrunway', 'arrival_actualtime', 'arrival_baggage', 'arrival_delay', 'arrival_estimatedrunway', 'arrival_estimatedtime', 'arrival_gate', 'arrival_iatacode', 'arrival_icaocode', 'arrival_scheduledtime', 'arrival_terminal', 'codeshared_airline_iatacode', 'codeshared_airline_icaocode', 'codeshared_airline_name', 'codeshared_flight_iatanumber', 'codeshared_flight_icaonumber', 'codeshared_flight_number', 'departure_actualrunway', 'departure_actualtime', 'departure_baggage', 'departure_delay', 'departure_estimatedrunway', 'departure_estimatedtime', 'departure_gate', 'departure_iatacode', 'departure_icaocode', 'departure_scheduledtime', 'departure_terminal', 'flight_iatanumber', 'flight_icaonumber', 'flight_number', 'status', "type"]


# @dag(
#     dag_id="flight_schedule",
#     start_date=pendulum.datetime(2026, 1, 9, tz="UTC"),
#     catchup=False,
#     dagrun_timeout=datetime.timedelta(minutes=60),
#     tags=["xp-project", "current_day"]
# )
def current_flight_schedule():    
    # @task
    def load_flights_today():
        BRONZE_FOLDER = "/home/matheusco/Documents/POS_GRAD_DOCS/CONTEUDOS/CONTEUDOS/PROJETO_APLICADO/docker/airflow/data/bronze"# os.getenv('BRONZE_LAYER_FOLDER')
        
        iata_codes = ['VCP', 'GRU', 'CGH', 'BSB']
        current_date = datetime.datetime.now()
        _day = current_date.strftime('%d')
        _month = current_date.strftime('%m')
        _year = current_date.strftime('%Y')
        # _hour = current_date.strftime('%H')
        # _minute = current_date.strftime('%M')
        
        hist = []
        for airport in iata_codes:
            data = open(f"{BRONZE_FOLDER}/{airport}_{_year}{_month}{_day}.json", "r")
            hist.extend(json.loads(data.read())['data'])
                
            data.close()
            
        # return hist
        return transform_flights(hist)
    
    def transform_flights(flights):
        transformed = []
        for row in flights:
            line = recursive_fields(row)
            print("AQUI 1", line)
            transformed.append(line)
        
        return transformed
    
    def recursive_fields(row, fd="", all_fields={}):
        # all_fields = {}
        for _field in dict(row).keys():
            if type(row[_field]) == dict:
                field_concat = _field if fd == "" else f"{fd}_{_field}"
                recursive_fields(row[_field], field_concat, all_fields)
            else:
                if fd == "": final = _field.lower()
                else: final = f"{fd}_{_field}".lower()
                value = 'null' if row[_field] == None else row[_field]
                
                all_fields[final.lower()] = value
        
        return all_fields
    
    def format_data(data):
        all_data = []
        for row in data:
            # line = {}
            line = []
            # _keys = [_k for _k in list(row.keys())]
            for f in fields:
                # if f in ['arrival_delay', 'departure_delay']:
                #     if row[f] in [None, 'null']: line.append('null')
                #     else: line.append(int(row[f]))
                # else:
                print("==>> ", f in row.keys())    
                if f in list(row.keys()):
                    if row[f] in [None, 'null']:
                        print("aqui 11")
                        line.append(None)
                    else:
                        print("aqui 22")
                    #     line.append(None if row[f] == "" else row[f])
                        line.append(row[f])
                else:
                    line.append(None)
            
            print("=> ", line)
            all_data.append(line)
            del line
            
        return all_data
    # @task
    def insert_flights(flights_data):
        
        query = f"""INSERT INTO public.flights ({','.join(fields)}) VALUES %s"""
        print(query)
        # postgres_hook = PostgresHook(postgres_conn_id="aviation_postres")
        # conn = postgres_hook.get_conn()
        # cur = conn.cursor()
        
        try:
            print(flights_data[:2])
            new_data = format_data(flights_data)
            # print(new_data[:5])
            
            # execute_values(cur, query, new_data)
        except Exception as error:
            print(error)
            sys.exit(1)
        # conn.commit()
            
        # cur.close()
        # conn.close()
    
    insert_flights(load_flights_today())
    #loadData(transformData(extractMunicipios()))
    
    ## OK: loadMesorregioes(extractMesorregioes())
    
    #loadMicrorregioes(extractMicrorregioes())
    
current_flight_schedule()