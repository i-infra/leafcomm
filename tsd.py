import sqlite3 as sql
import json
import random
import time

unit_list = ['none', 'degc', 'rh', 'kpa', 'watt']
unit_human = ['None', 'Degrees Celsius', 'Relative Humidity', 'Kilopascals', 'Watts']
class TimeSeriesDatastore(object):

    def __init__(self, db='sproutwave_v0.db'):
        self.conn = sql.connect('sproutwave_v0.db')
        init_readings = 'CREATE TABLE IF NOT EXISTS readings (Timestamp REAL, Sensor INT, Units INT, Value REAL)'
        self.conn.execute(init_readings)
        create_index = 'CREATE INDEX IF NOT EXISTS time ON readings (Timestamp ASC)'
        self.conn.execute(create_index)
        self.conn.commit()
        self.cursor = self.conn.cursor()

    def get_measurement_vectors(self, start=0, stop=-1):
        if stop == -1:
            stop = time.time()
        selector = 'SELECT * FROM readings WHERE Timestamp BETWEEN %s AND %s' % (
            str(start), str(stop))
        samples = self.cursor.execute(selector)
        labels = 'timestamp sensor_uid units value'.split()
        values = dict(zip(labels, zip(*samples)))
        values['units'] = [unit_list[unit] for unit in values['units']]
        return values

    def get_measurements(self, start=0, stop=-1):
        if stop == -1:
            stop = time.time()
        selector = 'SELECT * FROM readings WHERE Timestamp BETWEEN %s AND %s' % (
            str(start), str(stop))
        samples = self.cursor.execute(selector)
        labels = 'timestamp sensor_uid units value'.split()
        samples = [dict(zip(labels, sample)) for sample in samples]
        for sample in samples:
           sample['units'] = unit_list[sample['units']]
        return samples

    def add_measurement(self, timestamp, sensor_uid, units, value):
        if units.lower() in unit_list:
            units = unit_list.index(units.lower())
        elif units == None:
            units = 0
        else:
            units = -1
        if type(value) != float:
            value = float(value)
        if type(sensor_uid) != int:
            sensor_id = int(sensor_id)
        inserter = "INSERT INTO readings VALUES(?, ?, ?, ?)"
        data = (timestamp, sensor_uid, units, value)
        print(data)
        self.cursor.executemany(inserter, [data])
        return self.conn.commit()


if __name__ in ["__main__", "__console__"]:
    tsd = TimeSeriesDatastore()
    for v in tsd.get_measurements():
        print(v)
