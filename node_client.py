from node_proxy import *
from node_core import *
import _constants

loop = asyncio.get_event_loop()
aiohttp_client_session = aiohttp.ClientSession(loop=loop)

URL_BASE = f"{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}"
SIGNUP_URL = f"{URL_BASE}/signup"
LOGIN_URL = f"{URL_BASE}/login"
LATEST_URL = f"{URL_BASE}/latest"


async def test_signup():
    packer, unpacker = await get_packer_unpacker(_constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512("test password".encode(), encoder=nacl.encoding.RawEncoder)
    signup_message = dict(
        email="tester4@test.com", name="test_name", nodeSecret=uid.hex(), passwordHash=password_hash, passwordHint="test_password", phone="8675309"
    )
    print("signup ->", signup_message)
    print("signup <-", await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, SIGNUP_URL, signup_message))


async def test_latest():
    packer, unpacker = await get_packer_unpacker(_constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512("test password".encode(), encoder=nacl.encoding.RawEncoder)
    login_message = dict(email="tester4@test.com", passwordHash=password_hash)
    print("login <-", await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, LOGIN_URL, login_message))
    print("latest <-", await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, LATEST_URL, ""))

async def test_alerts():
    packer, unpacker = await get_packer_unpacker(_constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512("test password".encode(), encoder=nacl.encoding.RawEncoder)
    login_message = dict(email="tester4@test.com", passwordHash=password_hash)
    print("login <-", await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, LOGIN_URL, login_message))
    print("alerts <-", await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, ALERTS_URL, {'alerts':[]}))


async def test_race():
    packer, unpacker = await get_packer_unpacker(_constants.upstream_pubkey_bytes)
    human_name, uid = get_hardware_uid()
    password_hash = nacl.hash.sha512("test password".encode(), encoder=nacl.encoding.RawEncoder)
    login_message = dict(email="tester4@test.com", passwordHash=password_hash)
    print("login <-", await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, LOGIN_URL, login_message))
    latests = [make_wrapped_http_request(aiohttp_client_session, packer, unpacker, LATEST_URL, "") for _ in range(10)]
    import random

    random.shuffle(latests)
    futures = [asyncio.ensure_future(latest) for latest in latests]
    print("raced 10, ", len([x for x in await asyncio.gather(*futures) if x]), "succeeded")


handlebars.multi_spawner(test_signup).join()
handlebars.multi_spawner(test_race).join()
handlebars.multi_spawner(test_alerts).join()
handlebars.multi_spawner(test_latest).join()
