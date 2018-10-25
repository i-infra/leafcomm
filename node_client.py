from node_proxy import *
from node_core import *
import _constants

async def test_signup():
    redis_connection = await init_redis()
    packer, unpacker = get_packer_unpacker(redis_connection, _constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512('test 2password'.encode()).decode()
    signup_message = dict(zip('email name nodeSecret passwordHash passwordHint phone'.split(), f'test@test.com test_name {uid.hex()} {password_hash} test_password 8675309'.split()))
    print(signup_message)
    msg = await packer(signup_message)
    async with aiohttp.client.ClientSession() as client:
        async with client.post(url=f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/signup', data=msg) as resp:
            resp_bytes = await resp.read()
            print(await unpacker(resp_bytes))


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
