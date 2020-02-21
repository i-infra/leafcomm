import asyncio
import datetime

import etek_codes
import pattern_flipper2
import sns_abstraction
import ts_datastore
from node_core import *

IN = 1
OUT = -1
DEFAULT_STATE = 0


class Actuator:
    def __init__(
        self,
        direction,
        default_state,
        work_units,
        flow_measurement,
        actuator_name,
        description,
        system_name,
    ):
        self.direction = direction
        self.default_state = default_state
        self.input_unit, self.output_unit = work_units
        if flow_measurement:
            raise NotImplementedError("flow measurement is not yet implemented")
        self.description = description
        self.system_name = system_name
        self.name = actuator_name


# TODO: add timeout, verification of commands by observing derivative change sign


class BangBangWorker:
    def __init__(
        self,
        name="bang-bang controller",
        actuator=None,
        target_sensor=1279,
        target_units=ts_datastore.degc,
        low=1,
        high=2,
        target_outlet_index=2,
        redis_connection=None,
    ):
        self.name = name
        self.high = high
        self.low = low
        self.target_sensor = target_sensor
        self.target_units = target_units
        self.actuator = actuator
        self.redis_connection = redis_connection
        self.key_name = f"{name}@{target_sensor}-{self.actuator.name}"
        self.logger = get_logger(self.key_name)
        self.last_update_ts = None
        self.last_command_ts = None
        self.last_command_value = None
        self.outlet_code = etek_codes.codes_0203[target_outlet_index]
        asyncio.create_task(self.set_state(self.actuator.default_state))

    async def get_state(self):
        state = await self.redis_connection.get(self.key_name)
        self.logger.debug(f"got state: {state}")
        return state

    async def set_state(self, value):
        self.logger.debug(f"setting state: {value}")
        self.last_command_value = value
        await pattern_flipper2.push_message_fl2000(
            pattern_flipper2.build_message_outlet_fl2000(self.outlet_code, value),
            redis_connection=self.redis_connection,
        )
        return await self.redis_connection.set(self.key_name, int(value))

    async def update_state(self, sensor_reading):
        ts, ulid = sensor_reading.timestamp, sensor_reading.ulid
        _, sensor_uid, units, value = sensor_reading.value
        # bail if update isn't for us
        if sensor_uid != self.target_sensor or units != self.target_units:
            return
        self.last_update_ts = ts
        state_from_redis = (await self.get_state()) or False
        currently_on = bool(int(state_from_redis))
        tempDegF = int(value * 9 / 5 + 32)
        self.logger.info(f"currently_on: {currently_on}, temp: {tempDegF}degF")
        if value > self.high and not currently_on:
            # sns_abstraction.send_as_sms(f"temp: {tempDegF}degF, turning on. {ts}")
            return True
        if value < self.low and currently_on:
            # sns_abstraction.send_as_sms(f"temp: {tempDegF}degF, turning off. {ts}")
            return False
        # if currently_on and (ts - self.last_command_ts) > 600 and (value - self.last_command_value)


async def run_fridge_controller(loop=None):
    redis_connection = await init_redis("")
    compressor = Actuator(
        actuator_name="compressor",
        direction=OUT,  # compressor pumps heat (measured as temperature) out of system
        default_state=L,  # default state is off
        work_units=("watts", "temperature"),
        flow_measurement=False,
        description="electric motor compressing a gas and pumping heat from a system",
        system_name="refridgerator",
    )
    fridge_controller = BangBangWorker(
        redis_connection=await init_redis(), actuator=compressor
    )
    async for reading in pseudosub(redis_connection, "feedback_channel"):
        target_state = await fridge_controller.update_state(reading)
        if target_state != None:
            await fridge_controller.set_state(target_state)
            # await pseudopub(redis_connection, ["feedback_commands"], None, target_state)


async def wait_for(dt):
    # sleep until the specified datetime
    while True:
        now = datetime.datetime.now()
        remaining = (dt - now).total_seconds()
        if remaining < 86400:
            break
        # asyncio.sleep doesn't like long sleeps, so don't sleep more
        # than a day at a time
        await asyncio.sleep(86400)
    await asyncio.sleep(remaining)


async def run_at(dt, coro):
    await wait_for(dt)
    return await coro
