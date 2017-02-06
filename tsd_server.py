from aiohttp import web
import asyncio
import uuid
import base64

import json
import os
import ssl

ej = json.dumps
dj = json.loads

import tsd

class LogMeasurement(web.View):
    async def get(self):
        if 'start' in self.request.rel_url.query.keys():
            start = float(self.request.rel_url.query['start'])
        else:
            start = 0
        if 'stop' in self.request.rel_url.query.keys():
            stop = float(self.request.rel_url.query['stop'])
        else:
            stop = -1
        return web.Response(text=ej([x for x in self.request.app['tsd'].get_window(start, stop)]))

    async def post(self):
        params = await self.request.post()
        params = dict([x for x in params.items()])
        print(params)
        params['value'] = float(params['value'])
        params['uid'] = int(params['sensor_uid'])
        self.request.app['tsd'].add_measurement(params['time'], params['uid'], params['units'], params['value'])
        return web.Response(text=ej(params))


class LogError(web.View):

    async def post(self):
        params = await self.request.post()
        params = dict([x for x in params.items()])
        self.request.app['tsd'].add_error(params['time'], base64.b64decode(params['raw']))
        return web.Response(text=ej(params))


loop = asyncio.get_event_loop()

app = web.Application()
app['tsd'] = tsd.TimeSeriesDatastore()
app.router.add_get('/logmeasurement', LogMeasurement)
app.router.add_post('/logmeasurement', LogMeasurement)
app.router.add_post('/logerror', LogError)

async def init(loop, port = 8000, verbose = True):
    handler = app.make_handler()
    app['verbose'] = verbose
    srv = await loop.create_server(handler, 'localhost', port)
    print('serving on', srv.sockets[0].getsockname())
    return srv

def main(port = 8000, verbose = True):
    loop.run_until_complete(init(loop, port=port, verbose=verbose))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
