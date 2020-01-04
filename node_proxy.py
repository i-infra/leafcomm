import asyncio
import dataclasses
import importlib
import logging
import pathlib
import ssl
import sys

import aiohttp
from aiohttp import web

import _constants
import user_datastore
from node_comms import *

logger = get_logger(__name__, debug="--debug" in sys.argv)


class ProxyDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop, connection):
        self.loop = loop
        self.connection = connection
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.loop.create_task(pseudopub(self.connection, ["udp_inbound"], None, data))


async def register_node(request):
    connection = request.app["redis"]
    posted_bytes = await request.read()
    pubkey_bytes, uid = await unwrap_message(posted_bytes, connection)
    await connection.hset(
        f"{redis_prefix}_node_pubkey_uid_mapping", pubkey_bytes.hex(), uid
    )
    await connection.hset(
        f"{redis_prefix}_node_earliest_timestamp_pubkey",
        pubkey_bytes.hex(),
        time.time(),
    )
    encrypted_message = await wrap_message(pubkey_bytes, b"\x01", connection)
    return web.Response(body=encrypted_message)


async def redistribute(loop=None):
    redis_connection = await init_redis("proxy")
    async for udp_reading in pseudosub(redis_connection, "udp_inbound"):
        udp_bytes = udp_reading.value
        pubkey_bytes, update_msg = await unwrap_message(udp_bytes, redis_connection)
        uid = await redis_connection.hget(
            f"{redis_prefix}_node_pubkey_uid_mapping", pubkey_bytes.hex()
        )
        now = time.time()
        await redis_connection.hset(
            f"{redis_prefix}_latest_value_frame", uid, cbor.dumps(update_msg)
        )
        await redis_connection.hset(
            f"{redis_prefix}_latest_message_timestamp", uid, now
        )
        await pseudopub(redis_connection, [f"updates_{uid.decode()}"], now, update_msg)


async def check_received(request):
    connection = request.app["redis"]
    posted_bytes = await request.read()
    pubkey_bytes, uid = await unwrap_message(posted_bytes, connection)
    uid_hex = (
        await connection.hget(
            f"{redis_prefix}_node_pubkey_uid_mapping", pubkey_bytes.hex()
        )
        or b""
    ).decode()
    earliest_timestamp_pubkey = await connection.hget(
        f"{redis_prefix}_node_earliest_timestamp_pubkey", pubkey_bytes.hex()
    )
    latest_msg_timestamp = await connection.hget(
        f"{redis_prefix}_latest_message_timestamp", uid
    )
    earliest_timestamp = float(earliest_timestamp_pubkey or 0)
    timestamp_latest_msg = float(latest_msg_timestamp or 0)
    msg = (earliest_timestamp, timestamp_latest_msg)
    encrypted_message = await packer_unpacker_cache[pubkey_bytes][0](msg)
    return web.Response(body=encrypted_message)


async def start_authenticated_session(request):
    connection = request.app["redis"]
    users_db = request.app["users_db"]
    posted_bytes = await request.read()
    pubkey_bytes, msg = await unwrap_message(posted_bytes, connection)
    if "signup" in request.rel_url.path:
        signup_required_fields = (
            "email name nodeSecret passwordHash passwordHint phone".split()
        )
        for key in signup_required_fields:
            assert msg.get(key)
        user_signin_status, user_info = users_db.check_user(
            msg.get("email"), msg.get("passwordHash")
        )
        if user_signin_status == user_datastore.Status.NO_SUCH_USER:
            user_signin_status = users_db.add_user(
                msg.get("name"),
                msg.get("email"),
                msg.get("phone"),
                msg.get("passwordHash"),
                msg.get("passwordHint"),
                msg.get("nodeSecret"),
            )
            _, user_info = users_db.check_user(
                msg.get("email"), msg.get("passwordHash")
            )
        if user_info is not None:
            serialized_user = dataclasses.asdict(user_info)
        else:
            serialized_user = "None"
        msg = ("signup", str(user_signin_status), serialized_user)
    if "login" in request.rel_url.path:
        login_required_fields = ["email", "passwordHash"]
        for key in login_required_fields:
            assert msg.get(key)
        user_signin_status, user_info = users_db.check_user(
            msg.get("email"), msg.get("passwordHash")
        )
        if user_info is not None:
            serialized_user = dataclasses.asdict(user_info)
        else:
            serialized_user = "None"
        msg = ("login", str(user_signin_status), serialized_user)
    if user_signin_status == user_datastore.Status.SUCCESS:
        await connection.hset(
            f"{redis_prefix}_user_pubkey_uid_mapping",
            pubkey_bytes.hex(),
            user_info.node_id,
        )
        await connection.hset(
            f"{redis_prefix}_user_pubkey_email_mapping",
            pubkey_bytes.hex(),
            user_info.email,
        )
        await connection.hset(
            f"{redis_prefix}_user_timestamp_authed_pubkey",
            pubkey_bytes.hex(),
            time.time(),
        )
    encoded_encrypted_message = await wrap_message(
        pubkey_bytes, msg, connection, b64=True
    )
    return web.Response(
        body=encoded_encrypted_message, content_type="application/base64"
    )


async def get_latest(request):
    connection = request.app["redis"]
    posted_bytes = await request.read()
    pubkey_bytes, msg = await unwrap_message(posted_bytes, connection)
    uid = await connection.hget(
        f"{redis_prefix}_user_pubkey_uid_mapping", pubkey_bytes.hex()
    )
    response = "NO DATA YET"
    if uid is not None:
        latest = await connection.hget(f"{redis_prefix}_latest_value_frame", uid)
        if latest is not None:
            response = cbor.loads(latest)
    encoded_encrypted_message = await wrap_message(
        pubkey_bytes, response, connection, b64=True
    )
    return web.Response(
        body=encoded_encrypted_message, content_type="application/base64"
    )



import sns_abstraction


IN = 1
OUT = -1

class Actuator:
    def __init__(self, direction, work_units, flow_measurement, actuator_name, description, system_name):
        self.direction = direction
        self.input_unit, self.output_unit = work_units
        if flow_measurement:
            raise NotImplementedError("flow measurement is not yet implemented")
        self.description = description
        self.system_name = system_name
        self.name = actuator_name

DEGC = 1
RH = 2

class BangBangWorker:
    def __init__(self, name = "refridgerator controller", target_sensor = 1279, target_units = DEGC, low=6, high=8, actuator = None, redis_connection = None):
        self.name = name
        self.high = high
        self.low = low
        self.target_sensor = target_sensor
        self.target_units = target_units
        self.actuator = actuator
        self.redis_connection = redis_connection
        self.actuator = Actuator(direction = IN, work_units = ("watts", "temperature"), flow_measurement = False, actuator_name = "compressor", description = "electric motor compressing a gas and pumping heat from a system", system_name = "refridgerator")
        self.key_name = f"{name}@{target_sensor}({low},{high})x{self.actuator.name}"
        self.logger = get_logger(self.key_name)
        self.last_updated = 0

    async def get_state(self):
        state = await self.redis_connection.get(self.key_name)
        self.logger.debug(f'got state: {state}')
        return state

    async def set_state(self, value):
        self.logger.debug(f'setting state: {value}')
        return await self.redis_connection.set(self.key_name, value)

    async def update_state(self, sensor_reading):
        ts, ulid = sensor_reading.timestamp, sensor_reading.ulid
        self.last_updated = ts
        _, sensor_uid, units, value = sensor_reading.value
        if sensor_uid == self.target_sensor and units == self.target_units:
            compressor_on = bool(int(await self.get_state()))
            tempDegF = int(value * 9 / 5 + 32)
            self.logger.info(f"compressor_on: {compressor_on}, temp: {tempDegF}degF")
            if value > self.high and not compressor_on:
                sns_abstraction.send_as_sms(f"temp: {tempDegF}degF, turning on. {ts}")
                return True
            if value < self.low and compressor_on:
                sns_abstraction.send_as_sms(f"temp: {tempDegF}degF, turning off. {ts}")
                return False


async def run_alerts(loop=None):
    redis_connection = await init_redis("")
    bbw = BangBangWorker(redis_connection=await init_redis(""))
    async for reading in pseudosub(redis_connection, "feedback_channel"):
        results = await bbw.update_state(reading)
        if results != None:
            logger.debug(f"alerter_results {results}")
            await pseudopub(redis_connection, ["feedback_commands"], None, results)


async def run_controls(loop=None):
    redis_connection = await init_redis("")
    bbw = BangBangWorker(redis_connection=await init_redis(""))
    async for command in pseudosub(redis_connection, "feedback_commands"):
        if command != None:
            command_value = int(bool(command.value))
            logger.debug(f"command_value {command_value}")
            #native_spawn(["sunxi-pio", "-m", f"PB9={command_value},2"])
            #open("command.value", "w").write(f"{command_value}")
            await bbw.set_state(command_value)


async def set_alerts(request):
    connection = request.app["redis"]
    posted_bytes = await request.read()
    pubkey_bytes, msg = await unwrap_message(posted_bytes, connection)
    logger.info(f"set_alerts: {msg}")
    uid = await connection.hget(
        f"{redis_prefix}_user_pubkey_uid_mapping", pubkey_bytes.hex()
    )
    alerts = None
    if msg:
        serialized_alerts = cbor.dumps(msg)
        await connection.hset(
            f"{redis_prefix}_uid_alert_mapping", uid, serialized_alerts
        )
        maybe_email = await connection.hget(
            f"{redis_prefix}_user_pubkey_email_mapping".pubkey_bytes.hex()
        )
        if maybe_email and alerts:
            request.app["users_db"].update_alerts(maybe_email, serialized_alerts)
        alerts = msg
    elif msg == None:
        maybe_alerts = await connection.hget(f"{redis_prefix}_uid_alert_mapping", uid)
        if maybe_alerts:
            alerts = cbor.loads(maybe_alerts)
    if not alerts:
        response = f"NO ALERTS YET: got {str(msg)}"
    else:
        response = alerts
    encoded_encrypted_message = await wrap_message(
        pubkey_bytes, response, connection, b64=True
    )
    return web.Response(
        body=encoded_encrypted_message, content_type="application/base64"
    )


def create_app(loop):
    async def start_background_tasks(app):
        app["redis"] = await init_redis("proxy")
        protocol = ProxyDatagramProtocol(loop, await init_redis("proxy"))
        app["udp_task"] = loop.create_task(
            loop.create_datagram_endpoint(
                lambda: protocol, local_addr=("0.0.0.0", _constants.upstream_port)
            )
        )
        app["redistribute_task"] = loop.create_task(redistribute())
        app["run_alerts"] = loop.create_task(run_alerts())
        app["run_alerts"] = loop.create_task(run_controls())
        app["users_db"] = user_datastore.UserDatabase(db_name=f"{data_dir}/users_db.db")
        app["packer_unpacker_cache"] = {}

    app = web.Application()
    app.router.add_post("/alerts", set_alerts)
    app.router.add_post("/latest", get_latest)
    app.router.add_post("/register", register_node)
    app.router.add_post("/check", check_received)
    app.router.add_post("/signup", start_authenticated_session)
    app.router.add_post("/login", start_authenticated_session)
    app.on_startup.append(start_background_tasks)
    return app


if __name__ == "__main__":
    pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    redis_server_process = start_redis_server(
        redis_socket_path=data_dir + "proxysproutwave.sock"
    )
    loop = asyncio.get_event_loop()
    app = create_app(loop)
    if _constants.upstream_protocol == "https":
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(
            certfile=local_dir + "/resources/fullchain.pem",
            keyfile=local_dir + "/resources/privkey.pem",
        )
        context.set_ciphers("RSA")
    else:
        context = None
    web.run_app(app, host="0.0.0.0", port=_constants.upstream_port, ssl_context=context)
