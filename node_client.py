from node_proxy import *

async def test_ws():
    with aiohttp.client.ClientSession() as client:
        test_ws = await client.ws_connect('https://data.sproutwave.com:8444/ws')
        human_name, uid = get_hardware_uid()
        test_ws.send_bytes(uid)
        while True:
            msg = await test_ws.receive()#timeout=1)
            if msg.type in [aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING]:
                break
            else:
                print(msg)

spawner(test_ws).join()
