import asyncio
import datetime

import arrow
import dateutil.rrule
import etek_codes
import pattern_streamer
import pytimeparse
import recurrent
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


class BangBangWorker:
    def __init__(
        self,
        name="bang-bang controller",
        actuator=None,
        target_sensor=1235,
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
        self.last_value = 0
        self.outlet_code = etek_codes.codes_0203[target_outlet_index]
        asyncio.create_task(self.set_state(self.actuator.default_state))

    async def get_state(self):
        state = await self.redis_connection.get(self.key_name)
        self.logger.debug(f"got state: {state}")
        return state

    async def set_state(self, value):
        self.logger.debug(f"setting state: {value}")
        self.last_command_value = value
        self.last_command_ts = time.time()
        await pattern_streamer.push_message_fl2000(
            pattern_streamer.build_message_outlet_fl2000(self.outlet_code, value),
            redis_connection=self.redis_connection,
        )
        return await self.redis_connection.set(self.key_name, int(value))

    async def update_state(self, sensor_reading):
        if sensor_reading and sensor_reading.value:
            ts, ulid = sensor_reading.timestamp, sensor_reading.ulid
            _, sensor_uid, units, value = sensor_reading.value
        else:
            sensor_uid = None
        # bail if update isn't for us
        if sensor_uid != self.target_sensor or units != self.target_units:
            return
        self.last_update_ts = ts
        # startup deriv should be 0 not value
        self.derivative = value - self.last_value or value
        self.last_value = value
        state_from_redis = (await self.get_state()) or False
        currently_on = bool(int(state_from_redis))
        tempDegF = int(value * 9 / 5 + 32)
        self.logger.info(f"currently_on: {currently_on}, temp: {tempDegF}degF")
        target = None
        if value > self.high and not currently_on:
            # sns_abstraction.send_as_sms(f"temp: {tempDegF}degF, turning on. {ts}")
            target = True
        if value < self.low and currently_on:
            # sns_abstraction.send_as_sms(f"temp: {tempDegF}degF, turning off. {ts}")
            target = False
        expected_sign = numpy.sign(
            self.actuator.direction * {False: -1.0, True: 1.0}[currently_on]
        )
        if (ts - self.last_command_ts) > 180 and numpy.sign(
            self.derivative
        ) != expected_sign:
            logger.debug(
                "detected incorrect actuator state, probably from a missed command - fixing"
            )
            # target_state = {self.actuator.direction: True, self.actuator.direction * -1: False}[expected_sign]
            target = currently_on
        if target is not None:
            await self.set_state(target)
        return target




class TimerWorker:
    def __init__(
        self,
        redis_connection,
        target_outlet_index,
        rrule_on="every day at 10am",
        rrule_off=None,
        duration=None,
    ):
        self.redis_connection = redis_connection
        rrulestr_parser = functools.partial(
            dateutil.rrule._rrulestr()._parse_rfc,
            dtstart=arrow.now().floor("hour").datetime,
        )
        if isinstance(rrule_on, str) and "RRULE" not in rrule_on:
            rrule_str = recurrent.RecurringEvent().parse(rrule_on)
            # specify a clean start time to enable timezone aware math between rrules and arrows
            rrule_on = rrulestr_parser(rrule_str)
        self.rrule_on = rrule_on
        self.next_on = self.rrule_on.after(arrow.now())
        self.rrule_off = rrule_off
        if rrule_off and duration:
            raise ValueError("Can't pass both an off-time and a duration!")
        self.duration = duration
        if duration and isinstance(duration, str):
            self.duration = pytimeparse.parse(duration)
            self.next_off = arrow.Arrow.fromdatetime(self.next_on).shift(
                seconds=self.duration
            )
        if isinstance(rrule_off, str):
            if "RRULE" not in rrule_off:
                rrule_str = recurrent.RecurringEvent().parse(rrule_off)
            else:
                rrule_str = rrule_off
            self.rrule_off = rrulestr_parser(rrule_str)
            self.next_off = self.rrule_off.after(self.next_on)
            self.duration = (self.next_off - self.next_on).seconds
        self.target_outlet_index = target_outlet_index
        self.outlet_code = etek_codes.codes_0203[target_outlet_index]
        rrule_fragment = str(self.rrule_on).split("\n")[-1]
        self.key_name = f"{rrule_fragment}+{self.duration}s@{target_outlet_index}"
        self.logger = get_logger(self.key_name)
        self.on_task = None
        self.off_task = None
        self.last_command_value = None
        self.last_command_ts = 0
        self.logger.info(f"inited timer! {self.rrule_on} {self.rrule_off} {duration}")

    async def set_state(self, value):
        self.logger.debug(f"setting state: {value}")
        self.last_command_value = value
        self.last_command_ts = time.time()
        await pattern_streamer.push_message_fl2000(
            pattern_streamer.build_message_outlet_fl2000(self.outlet_code, value),
            redis_connection=self.redis_connection,
        )
        return await self.redis_connection.set(self.key_name, int(value))

    async def update_state(self, sensor_reading=None):
        if not self.on_task or self.on_task.done():
            self.next_on = self.rrule_on.after(arrow.now())
            self.on_task = asyncio.create_task(
                run_at(self.next_on, self.set_state, True)
            )
            self.on_task.set_name(
                f"outlet {self.target_outlet_index} on at {self.next_on}"
            )
            self.logger.info(f"created {self.on_task}")
        if self.next_off is not None and (not self.off_task or self.off_task.done()):
            if not self.last_command_ts and self.rrule_off:
                self.next_off = self.rrule_off.after(self.next_on)
            elif self.rrule_off is not None:
                self.next_off = self.rrule_off.after(
                    arrow.Arrow.fromtimestamp(self.last_command_ts)
                )
            elif self.duration is not None:
                self.next_off = arrow.Arrow.fromdatetime(self.next_on).shift(
                    seconds=self.duration
                )
            self.off_task = asyncio.create_task(
                run_at(self.next_off, self.set_state, False)
            )
            self.off_task.set_name(
                f"outlet {self.target_outlet_index} off at {self.next_off}"
            )
            self.logger.info(f"created {self.off_task}")
        self.logger.debug(f"{self.on_task}, {self.off_task}")


class AlerterWorker:
    def __init__(
        self,
        redis_connection,
        name="alerter worker",
        target_sensor_uid=1221,
        target_units=ts_datastore.degc,
        low=1,
        high=2,
        target_number_to_message="+15133278483",
    ):
        self.redis_connection = redis_connection
        self.name = name
        self.high = high
        self.low = low
        self.target_sensor = target_sensor_uid
        self.target_units = target_units
        self.key_name = f"{name}@{target_sensor_uid}-{(low,high)}"
        self.logger = get_logger(self.key_name)
        self.last_update_ts = 0
        self.last_command_ts = 0
        self.derivative = 0
        self.last_value = 0
        self.target_number_to_message = target_number_to_message
        self.last_command_deriv = None

    async def update_state(self, sensor_reading):
        ts, ulid = sensor_reading.timestamp, sensor_reading.ulid
        _, sensor_uid, units, value = sensor_reading.value
        # bail if update isn't for us
        if sensor_uid != self.target_sensor or units != self.target_units:
            return
        # startup deriv should be 0 not value
        self.derivative = 0.1 * self.derivative + 0.9 * (
            (value - (self.last_value or value)) / (ts - self.last_update_ts)
        )
        self.last_value = value
        self.last_update_ts = ts
        tempDegF = int(value * 9 / 5 + 32)
        self.logger.info(f"derivative: {self.derivative}, temp: {tempDegF}degF")
        # last update ts
        if ts - self.last_command_ts > 600 and (
            (value < self.low) or (value > self.high)
        ):
            sns_abstraction.send_as_sms(
                f"current temperature: {tempDegF}degF: alerting outside of {self.low} to {self.high}"
            )
            self.last_command_value = value
            self.last_command_ts = time.time()
            self.last_command_deriv = self.derivative
            return True


async def run_alerters():
    redis_connection = await init_redis("")
    # TODO: load alerters from Redis
    alerter_redis_connection = await init_redis("")
    rc = alerter_redis_connection
    alerters = [
        AlerterWorker(rc, target_sensor_uid=1221, low=28, high=30),
        AlerterWorker(rc, target_sensor_uid=1235, low=0, high=3),
    ]
    # TODO:replace upstream_channel with dedicated channel
    async for reading in pseudosub(redis_connection, "upstream_channel"):
        if reading == None or reading.value == None:
            continue
        alerted = [await alerter.update_state(reading) for alerter in alerters]
        if any(alerted):
            alerter = alerters[alerted.index(True)]
            alerter.logger.info("alerted!")


async def run_controllers():
    redis_connection = await init_redis("")
    # TODO: load controllers from Redis
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
    controllers = [fridge_controller]
    controllers += [
        TimerWorker(
            redis_connection=await init_redis(),
            target_outlet_index=0,
            rrule_on="daily at 10a",
            rrule_off="daily at 2:30am",
        )
    ]
    controllers += [
        TimerWorker(
            redis_connection=await init_redis(),
            target_outlet_index=3,
            rrule_on="daily at 10a",
            rrule_off="daily at 2:30am",
        )
    ]
    controllers += [
        TimerWorker(
            redis_connection=await init_redis(),
            target_outlet_index=4,
            rrule_on="daily at 8a",
            duration="12h",
        )
    ]
    async for reading in pseudosub(redis_connection, "feedback_channel"):
        for controller in controllers:
            await controller.update_state(reading)


async def send_outlet_command(
    outlet_index, value, redis_connection=None, outlet_codes=etek_codes.codes_0203
):
    return await pattern_streamer.push_message_fl2000(
        pattern_streamer.build_message_outlet_fl2000(outlet_codes[outlet_index], value),
        redis_connection=redis_connection or await init_redis(""),
    )


async def wait_for(dt):
    # sleep until the specified datetime
    while True:
        now = arrow.now()
        remaining = (dt - now).total_seconds()
        if remaining < 86400:
            break
        # asyncio.sleep doesn't like long sleeps, so don't sleep more
        # than a day at a time
        await asyncio.sleep(86400)
    await asyncio.sleep(remaining)


async def run_at(dt, coro, *args):
    await wait_for(dt)
    return await coro(*args)
