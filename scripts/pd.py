import tsd
t = tsd.TimeSeriesDatastore()
vs = t.get_measurement_vectors()
import pandas as pd
vs['timestamp'] = pd.to_datetime(vs['timestamp'], unit='s')
timestamps = vs.pop('timestamp')
df = pd.DataFrame(vs,index=timestamps)
resampled = df[lambda d: (d.sensor_uid == 1045)][lambda d: d.units == 'degc'].resample('15Min')['value'].mean()
print(resampled.to_json(date_unit="s", double_precision=2))

