from node_proxy import *

async def test_ws():
    async with aiohttp.client.ClientSession() as client:
        test_ws = await client.ws_connect('https://data.sproutwave.com:8019/ws')
        human_name, uid = get_hardware_uid()
        await test_ws.send_bytes(uid)
        while True:
            msg = await test_ws.receive()
            if msg.type in [aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING]:
                print(msg)
                break
            else:
                print(msg)
while True:
    try:
        spawner(test_ws).join()
    except KeyboardInterrupt:
        break
