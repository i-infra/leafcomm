import decimal
import fractions
import math
import os
import sys
import time

from palettable.matplotlib import Viridis_10

import cairocffi as cairo
import pycha.line
import pycha.scatter
import ts_datastore

sensor_colors = Viridis_10.hex_colors
opacity = "80"
sensor_colors = [x + opacity for x in sensor_colors]
datastore = ts_datastore.TimeSeriesDatastore(
    f"{os.path.expanduser('~/.sproutwave/')}/sproutwave_v1.db"
)


def get_datasets(start=0, stop=-1, parameter="degc"):
    sensors = {}
    samples = datastore.get_measurements(start, stop)
    for sample in samples:
        sample["sensor_uid"] = "sensor" + str(sample["sensor_uid"] | 2)
        if sample["units"] == parameter:
            value = (sample["timestamp"] - start, sample["value"] * 9 / 5 + 32)
            if sample["sensor_uid"] not in sensors.keys():
                sensors[sample["sensor_uid"]] = [value]
            else:
                sensors[sample["sensor_uid"]].append(value)
    for sensor in [x for x in sensors.items()]:
        if len(sensor[1]) < 10:
            sensors.pop(sensor[0])
    return sensors


def plotter(filename, datasets, width, height):
    if len(datasets) == 0:
        print("no datasets for", filename)
        return None
    surface = cairo.SVGSurface(filename, width, height)
    longest = max([y[1][-1][0] for y in datasets])
    # starts tick guess at number of hours
    tick_count = longest / 3600
    if tick_count < 1:
        tick_count *= 12
        offset = 5
        time_label = "min ago"
    elif tick_count < 3:
        tick_count *= 4
        offset = 15
        time_label = "min ago"
    elif tick_count > 48:
        tick_count /= 24
        offset = 1
        time_label = "day(s) ago"
    elif tick_count > 12:
        tick_count /= 4
        offset = 4
        time_label = "hrs ago"
    else:
        offset = 1
        time_label = "hrs ago"
    tick_count_int = decimal.Decimal(tick_count).quantize(
        decimal.Decimal(1), rounding=decimal.ROUND_HALF_EVEN
    )
    options = {
        "axis": {
            "x": {
                "ticks": [
                    dict(
                        v=((tick_count - i) * longest / tick_count),
                        label=str(fractions.Fraction(i * offset)) + time_label,
                    )
                    for i in range(int(tick_count_int))
                    if i > 0
                ]
            },
            "y": {"tickCount": 4, "range": (0, 100), "label": "Deg F"},
            "tickFontSize": 14,
            "legendFontSize": 14,
            "labelFontSize": 28,
        },
        "background": {"color": "#ffffffff", "lineColor": "#444444"},
        "colorScheme": {"name": "fixed", "args": {"colors": sensor_colors}},
        "legend": {},
        "stroke": {"shadow": False, "color": "#aaaaaaff", "width": 6},
    }
    chart = pycha.scatter.ScatterplotChart(surface, options)
    chart.addDataset(datasets)
    chart.render()
    return surface


def plot_temperatures(hours=1, width=1024, height=768):
    datasets = get_datasets(time.time() - hours * 60 * 60)
    filename = "timespan-" + str(hours) + "hrs.svg"
    surface = plotter(filename, datasets.items(), width, height)


hour_scales = [1, 2, 4, 12, 24, 7 * 24]

if __name__ == "__main__":
    for hours in hour_scales:
        plot_temperatures(hours)
