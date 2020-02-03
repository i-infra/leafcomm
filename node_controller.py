import sns_abstraction
from node_core import *
import asyncio

IN = 1
OUT = -1
DEFAULT_STATE = 0


class Actuator:
    def __init__(
        self,
        direction,
        work_units,
        flow_measurement,
        actuator_name,
        description,
        system_name,
    ):
        self.direction = direction
        self.input_unit, self.output_unit = work_units
        if flow_measurement:
            raise NotImplementedError("flow measurement is not yet implemented")
        self.description = description
        self.system_name = system_name
        self.name = actuator_name


class BangBangWorker:
    def __init__(
        self,
        name="refridgerator controller",
        target_sensor=1279,
        target_units=ts_datastore.degc,
        low=6,
        high=8,
        actuator=None,
        redis_connection=None,
    ):
        self.name = name
        self.high = high
        self.low = low
        self.target_sensor = target_sensor
        self.target_units = target_units
        self.actuator = actuator
        self.redis_connection = redis_connection
        self.actuator = Actuator(
            direction=IN,
            work_units=("watts", "temperature"),
            flow_measurement=False,
            actuator_name="compressor",
            description="electric motor compressing a gas and pumping heat from a system",
            system_name="refridgerator",
        )
        self.key_name = f"{name}@{target_sensor}({low},{high})x{self.actuator.name}"
        self.logger = get_logger(self.key_name)
        self.last_updated = 0
        asyncio.create_task(self.set_state(DEFAULT_STATE))

    async def get_state(self):
        state = await self.redis_connection.get(self.key_name)
        self.logger.debug(f"got state: {state}")
        return state

    async def set_state(self, value):
        self.logger.debug(f"setting state: {value}")
        return await self.redis_connection.set(self.key_name, value)

    async def update_state(self, sensor_reading):
        ts, ulid = sensor_reading.timestamp, sensor_reading.ulid
        _, sensor_uid, units, value = sensor_reading.value
        if sensor_uid == self.target_sensor and units == self.target_units:
            self.last_updated = ts
            state_from_redis = (await self.get_state()) or False
            compressor_on = bool(int(state_from_redis))
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
            # native_spawn(["sunxi-pio", "-m", f"PB9={command_value},2"])
            # open("command.value", "w").write(f"{command_value}")
            await bbw.set_state(command_value)
