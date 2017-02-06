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
        init_errors = 'CREATE TABLE IF NOT EXISTS errors (Timestamp REAL, Raw Blob)'
        self.conn.execute(init_readings)
        self.conn.execute(init_errors)
        self.conn.commit()
        self.cursor = self.conn.cursor()

    def get_window(self, start=0, stop=-1):
        if stop == -1:
            stop = time.time()
        selector = 'SELECT * FROM readings WHERE Timestamp BETWEEN %s AND %s' % (
            str(start), str(stop))
        return self.cursor.execute(selector)

    def add_error(self, timestamp, raw):
        inserter = "INSERT INTO errors VALUES(?, ?)"
        self.cursor.executemany(inserter, [timestamp, raw])
        return self.conn.commit()

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
    tsd.add_measurement(time.time(), 0, 'degC', 37.0)
    for v in tsd.get_window():
        print(v)
