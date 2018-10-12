from node_proxy import *
from node_core import *
import _constants

async def test_signup():
    redis_connection = await init_redis()
    pubkey_bytes, session_box = create_ephemeral_box_from_pubkey_bytes()
    packer, unpacker = get_packer_unpacker(session_box, redis_connection, pubkey_bytes)
    msg = pubkey_bytes+(await packer({'hello': 'friend'}))
    async with aiohttp.client.ClientSession() as client:
        async with client.post(url=f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/signup', data=msg) as resp:
            resp_bytes = await resp.read()
            print(resp_bytes)


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

handlebars.multi_spawner(test_signup).join()
handlebars.multi_spawner(test_ws).join()
