import json


fields = ['identifier', 'aircraft_icao24', 'aircraft_icaocode', 'aircraft_regnumber', 'airline_iatacode', 'airline_icaocode', 'airline_name', 'arrival_actualrunway', 'arrival_actualtime', 'arrival_baggage', 'arrival_delay', 'arrival_estimatedrunway', 'arrival_estimatedtime', 'arrival_gate', 'arrival_iatacode', 'arrival_icaocode', 'arrival_scheduledtime', 'arrival_terminal', 'codeshared_airline_iatacode', 'codeshared_airline_icaocode', 'codeshared_airline_name', 'codeshared_flight_iatanumber', 'codeshared_flight_icaonumber', 'codeshared_flight_number', 'departure_actualrunway', 'departure_actualtime', 'departure_baggage', 'departure_delay', 'departure_estimatedrunway', 'departure_estimatedtime', 'departure_gate', 'departure_iatacode', 'departure_icaocode', 'departure_scheduledtime', 'departure_terminal', 'flight_iatanumber', 'flight_icaonumber', 'flight_number', 'status', "type"]

def recursive(row, fd="", all_fields={}):
    all_fields_new = all_fields
    for _field in dict(row).keys():
        if type(row[_field]) == dict:
            field_concat = _field if fd == "" else f"{fd}_{_field}"
            all_fields = recursive(row[_field], field_concat, all_fields_new)
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
        print("=> ", row['departure_iatacode'], row['flight_number'])
        line.append(f"{str(row['departure_iatacode'])}_{row['flight_number']}")
        for f in fields:
            # if f in ['arrival_delay', 'departure_delay']:
            #     if row[f] in [None, 'null']: line.append('null')
            #     else: line.append(int(row[f]))
            # else:
            if f in list(row.keys()):
                if row[f] in [None, 'None', 'null', ""]:
                    line.append(None)
                else:
                #     line.append(None if row[f] == "" else row[f])
                    line.append(row[f])
            else:
                line.append(None)
        all_data.append(line)
        del line
        
    return all_data
    
fl = open('/home/matheusco/Documents/POS_GRAD_DOCS/CONTEUDOS/CONTEUDOS/PROJETO_APLICADO/docker/airflow/data/bronze/BSB_20260216.json', 'r')
teste = json.loads(fl.read())['data']

data = []

for t in teste[:]:
    data.append(recursive(t).copy())


format_data(data)