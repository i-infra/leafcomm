import sqlite3 as sql
import json
import random
import time

unit_list = ['none', 'degc', 'rh', 'pa', 'watt', 'ph', 'ms_per_cm', 'umol_per_m2_per_sec', 'ppm_co2', 'ppm_o', 'ppm_n', 'ppm_p', 'ppm_k', 'ppm_na', 'ppm_ca', 'ppm_mg', 'ppm_cl']
# Specs for Lettuce
# 18-25 degC
# RH 50-70 %
# 100000 pascal ~= 1 ATM
# pH 5.8-6.0
# 0 - 0.25 mS/cm - target water input conductivity
# PAR 100-200 umol/m2/s DLI: 17 mol / m**2 / day
# CO2 390 (ambient) - 1500 ppm w/ supplemental light
# Disolved Oxygen = 4-20 ppm (5-13 in nature)
# Nitrate = 189-225 ppm
# Phosphorus = 47 ppm
# Potassium = 281 - 351 ppm
# Sodium < 50 ppm
# Calcium = 170 - 212 ppm
# Magnesium = 48 - 65 ppm
# Chloride < 70 ppm
# Iron, Manganese, Zinc, Boron, Copper, Molydenum - 2ppm and below, constant levels
# http://ceac.arizona.edu/
# http://www.greenhouse.cornell.edu/crops/factsheets/hydroponic-recipes.pdf
# http://www.cornellcea.com/attachments/Cornell%20CEA%20Lettuce%20Handbook%20.pdf 
unit_human = ['None', 'Degrees Celsius', 'Relative Humidity', 'Pascals', 'Watts', 'pH', 'Conductivity', 'Photosynthetically Active Radiation (PAR)' 'PPM Carbon Dioxide', 'PPM Dissolved Oxygen', 'PPM Nitrate', 'PPM Phosphorus', 'PPM Potassium', 'PPM Sodium', 'PPM Calcium', 'PPM Magnesium', 'PPM Chloride']

# kludge to pull enum into local namespace
for i, unit in enumerate(unit_list):
    exec(unit+'='+str(i))
del i, unit

class TimeSeriesDatastore(object):

    def __init__(self, db = 'sproutwave_v0.db'):
        self.conn = sql.connect('sproutwave_v0.db')
        init_readings = 'CREATE TABLE IF NOT EXISTS readings (Timestamp REAL, Sensor INT, Units INT, Value REAL)'
        self.conn.execute(init_readings)
        create_index = 'CREATE INDEX IF NOT EXISTS time ON readings (Timestamp ASC)'
        self.conn.execute(create_index)
        self.conn.commit()
        self.cursor = self.conn.cursor()

    def get_measurement_vectors(self, start = 0, stop = -1):
        if stop == -1:
            stop = time.time()
        selector = 'SELECT * FROM readings WHERE Timestamp BETWEEN %s AND %s' % (
            str(start), str(stop))
        samples = self.cursor.execute(selector)
        labels = 'timestamp sensor_uid units value'.split()
        values = dict(zip(labels, zip(*samples)))
        values['units'] = [unit_list[unit] for unit in values['units']]
        return values

    def get_measurements(self, start = 0, stop = -1):
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

    def add_measurement(self, timestamp, sensor_uid, units, value, raw = False):
        if raw == True:
            unit_tag = units
        else:
            if units.lower() in unit_list:
                unit_tag = unit_list.index(units.lower())
            elif units == None:
                unit_tag = 0
            else:
                unit_tag = -1
        if type(value) != float:
            value = float(value)
        if type(sensor_uid) != int:
            sensor_id = int(sensor_id)
        inserter = "INSERT INTO readings VALUES(?, ?, ?, ?)"
        data = (timestamp, sensor_uid, unit_tag, value)
        self.cursor.executemany(inserter, [data])
        return self.conn.commit()

if __name__ in ["__main__", "__console__"]:
    tsd = TimeSeriesDatastore()
    for v in tsd.get_measurements():
        print(v)
