import sqlite3 as sql
import tempfile
import time

# TODO: use pint to refine this?
# used in meta tag to modify the set of compound units
si_units = ["m", "kg", "s", "A", "K", "mol", "cd"]
si_human = [
    "meter",
    "kilogram",
    "second",
    "ampere",
    "Degree Kelvin",
    "moles",
    "candela",
]
# compound units
unit_list = [
    "none",
    "degc",
    "rh",
    "pa",
    "watt",
    "ppm",
    "ph",
    "ms_per_cm",
    "umol_per_m2_s",
]
unit_human = [
    "None",
    "Degrees Celsius",
    "Relative Humidity",
    "Pascals",
    "Watts",
    "Parts Per Million",
    "pH",
    "Conductivity",
    "Photosynthetically Active Radiation (PAR)",
]
meta_tags = ["human:", "per:", "times:", "ChEBI:"]

# kludge to pull enum into local namespace
for i, unit in enumerate(unit_list):
    exec(unit + "=" + str(i))
del i, unit


class TimeSeriesDatastore(object):
    def __init__(self, db_name="time_series_datastore_v1.db"):
        self.db_name = db_name
        self.conn = sql.connect(db_name)
        init_readings = "CREATE TABLE IF NOT EXISTS readings (Timestamp REAL, Sensor INT, Units INT, Value REAL, Meta TEXT)"
        self.conn.execute(init_readings)
        create_index = "CREATE INDEX IF NOT EXISTS time ON readings (Timestamp ASC)"
        self.conn.execute(create_index)
        self.conn.commit()
        self.cursor = self.conn.cursor()

    def get_measurement_vectors(self, start=0, stop=-1):
        if stop == -1:
            stop = time.time()
        selector = "SELECT * FROM readings WHERE Timestamp BETWEEN %s AND %s" % (
            str(start),
            str(stop),
        )
        samples = self.cursor.execute(selector)
        labels = "timestamp sensor_uid units value meta".split()
        values = dict(zip(labels, zip(*samples)))
        values["units"] = [unit_list[unit] for unit in values["units"]]
        return values

    def get_measurements(self, start=0, stop=-1):
        if stop == -1:
            stop = time.time()
        selector = "SELECT * FROM readings WHERE Timestamp BETWEEN %s AND %s" % (
            str(start),
            str(stop),
        )
        samples = self.cursor.execute(selector)
        labels = "timestamp sensor_uid units value meta".split()
        samples = [dict(zip(labels, sample)) for sample in samples]
        for sample in samples:
            sample["units"] = unit_list[sample["units"]]
        return samples

    def add_measurement(
        self, timestamp, sensor_uid, units, value, raw=False, meta=None
    ):
        if raw == True:
            unit_tag = units
        else:
            if units.lower() in unit_list:
                unit_tag = unit_list.index(units.lower())
            elif units == None:
                unit_tag = 0
            else:
                unit_tag = -1
        if not isinstance(value, float):
            value = float(value)
        if not isinstance(sensor_uid, int):
            sensor_id = int(sensor_id)
        inserter = "INSERT INTO readings VALUES(?, ?, ?, ?, ?)"
        data = (timestamp, sensor_uid, unit_tag, value, meta)
        self.cursor.executemany(inserter, [data])
        return self.conn.commit()
