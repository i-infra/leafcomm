from node_proxy import *
from node_core import *
import _constants

loop = asyncio.get_event_loop()
aiohttp_client_session = aiohttp.ClientSession(loop=loop)

URL_BASE = f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}'
SIGNUP_URL = f'{URL_BASE}/signup'
LOGIN_URL = f'{URL_BASE}/login'
LATEST_URL = f'{URL_BASE}/latest'

async def test_signup():
    packer, unpacker = await get_packer_unpacker(_constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512('test password'.encode()).decode()
    signup_message = dict(zip('email name nodeSecret passwordHash passwordHint phone'.split(),
            f'tester@test.com test_name {uid.hex()} {password_hash} test_password 8675309'.split()))
    print('signup ->', signup_message)
    print('signup <-', await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, SIGNUP_URL, signup_message))

async def test_latest():
    packer, unpacker = await get_packer_unpacker(_constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512('test password'.encode()).decode()
    login_message = dict(zip('email passwordHash'.split(), f'tester@test.com {password_hash}'.split()))
    print('login <-', await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, LOGIN_URL, login_message))
    print('latest <-', await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, LATEST_URL, ''))


handlebars.multi_spawner(test_signup).join()
handlebars.multi_spawner(test_latest).join()
