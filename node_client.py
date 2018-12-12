from node_proxy import *
from node_core import *
import _constants


async def test_signup():
    redis_connection = await init_redis()
    packer, unpacker = get_packer_unpacker(redis_connection, _constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512('test password'.encode()).decode()
    signup_message = dict(
        zip('email name nodeSecret passwordHash passwordHint phone'.split(),
            f'test@test.com test_name {uid.hex()} {password_hash} test_password 8675309'.split()))
    print('signup -> ', signup_message)
    msg = await packer(signup_message)
    async with aiohttp.client.ClientSession() as client:
        async with client.post(url=f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/signup', data=msg) as resp:
            resp_bytes = await resp.read()
            print('signup <- ', await unpacker(resp_bytes))


async def test_login():
    redis_connection = await init_redis()
    packer, unpacker = get_packer_unpacker(redis_connection, _constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512('test password'.encode()).decode()
    signup_message = dict(zip('email passwordHash'.split(), f'test@test.com {password_hash}'.split()))
    msg = await packer(signup_message)
    async with aiohttp.client.ClientSession() as client:
        async with client.post(url=f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/login', data=msg) as resp:
            resp_bytes = await resp.read()
            print('login <- ', await unpacker(resp_bytes))
        human_name, uid = get_hardware_uid()
        msg = await packer(human_name)
        async with client.post(url=f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/latest', data=msg) as resp:
            resp_bytes = await resp.read()
            print('latest <- ', await unpacker(resp_bytes))


handlebars.multi_spawner(test_signup).join()
handlebars.multi_spawner(test_login).join()
