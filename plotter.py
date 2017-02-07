import time
import tsd

import sys

import cairocffi as cairo
import fractions

import decimal
import math

import pycha.line
import pycha.scatter

sensor_colors = ['#70B336FF', '#7CEAFFFF', '#CFA8FFFF', '#FFABCBFF', '#661479FF']

datastore = tsd.TimeSeriesDatastore()
def get_datasets(start=0, stop=-1, parameter = 'degc'):
    sensors = {}
    samples = datastore.get_measurements(start, stop)
    for sample in samples:
        sample['sensor_uid'] = 'sensor'+str(sample['sensor_uid'])
        if sample['units'] == parameter:
            value = (sample['timestamp']-start, sample['value']*9/5+32)
            if sample['sensor_uid'] not in sensors.keys():
                sensors[sample['sensor_uid']] = [value]
            else:
                sensors[sample['sensor_uid']].append(value)
    for sensor in [x for x in sensors.items()]:
        if len(sensor[1]) < 10:
            sensors.pop(sensor[0])
    return sensors



def plotter(datasets, width, height):
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)

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
    tick_count_int = decimal.Decimal(tick_count).quantize(decimal.Decimal(1), rounding=decimal.ROUND_HALF_EVEN)
    options = {
        'axis': {
            'x': {
                'ticks': [dict(v=((tick_count-i)*longest/tick_count), label= str(fractions.Fraction(i*offset))+time_label) for i in range(int(tick_count_int)) if i > 0]
            },
            'y': {
                'tickCount': 4,
                'range': (0, 100),
                'label': 'Deg F',
            },
            'tickFontSize' : 14,
            'legendFontSize': 14,
            'labelFontSize': 28
        },
        'background': {
            'color': '#ffffffff',
            'lineColor': '#444444'
        },
        'colorScheme': {
            'name': 'fixed',
            'args': {
                'colors': sensor_colors
            },
        },
        'legend': {
        },
        'stroke': {'shadow': False, 'color': '#aaaaaaff', 'width':6}
    }
    chart = pycha.scatter.ScatterplotChart(surface, options)
    chart.addDataset(datasets)
    chart.render()
    return surface

def plot_temperatures(hours=1, width = 1200, height = 800):
    output = 'linechart.png'
    datasets = get_datasets(time.time()-hours*60*60)
    surface = plotter(datasets.items(), width, height)
    surface.write_to_png(f"images/timespan-{hours}hrs.png")


hour_scales = [1, 2, 4, 12, 24, 7*24]

if __name__ in ['__main__', '__console__']:
    while True:
        for hours in hour_scales:
            plot_temperatures(hours)
        time.sleep(60)
