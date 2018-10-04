from node_proxy import *
import _constants

async def test_ws():
    async with aiohttp.client.ClientSession() as client:
        test_ws = await client.ws_connect(url=f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/ws')
        human_name, uid = get_hardware_uid()
        await test_ws.send_bytes(uid)
        while True:
            msg = await test_ws.receive()
            print(msg)
            if msg.type in [aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING]:
                break
while True:
    try:
        handlebars.multi_spawner(test_ws).join()
    except KeyboardInterrupt:
        break
