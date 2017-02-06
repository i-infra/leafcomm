import requests
import base64
from json import dumps as ej
measurement_endpoint = "http://localhost:8000/logmeasurement"
error_endpoint = "http://localhost:8000/logerror"

import tsd

def logerror(timestamp, raw):
    requests.post(error_endpoint, data={'time':timestamp, 'raw':base64.b64encode(raw)})
    return True

def logmeasurement(timestamp, silver_result):
    uid = (silver_result['channel']+1*1024)+silver_result['uid']
    t_unit = 'degC'
    h_unit = 'RH'
    t_value = silver_result['temperature']
    h_value = silver_result['humidity']
    requests.post(measurement_endpoint, data={'time': timestamp, 'sensor_uid': uid, 'units': t_unit, 'value': t_value})
    requests.post(measurement_endpoint, data={'time': timestamp, 'sensor_uid': uid, 'units': h_unit, 'value': h_value})
    return True

def getmeasurements(start=0, stop=-1):
    samples = requests.get(measurement_endpoint, {'start': start, 'stop': stop}).json()
    labels = 'timestamp sensor_uid units value'.split()
    samples = [dict(zip(labels, sample)) for sample in samples]
    for sample in samples:
       sample['units'] = tsd.unit_list[sample['units']]
    return samples
