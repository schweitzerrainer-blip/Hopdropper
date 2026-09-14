import asyncio
import json
import logging
import time
from datetime import datetime

from cbpi.api import (
    CBPiActor,
    CBPiStep,
    Property,
    StepResult,
    action,
    parameters,
)
from cbpi.api.dataclasses import NotificationType
from cbpi.api.timer import Timer

logger = logging.getLogger(__name__)

HOPDROPPER_ACTOR_NAME = "HopDropperActor"
HOPDROPPER_STEP_NAME = "HopDropperStep"


@parameters([
    Property.Text(label="Topic", configurable=True, default_value="hopdropper/drop",
                  description="MQTT Topic, auf das der Hopdropper hört"),
    Property.Select(label="Slot", options=[1, 2, 3, 4, 5],
                    description="Hopfen-Slot / Servo-Nummer des Hopdroppers (1-5)"),
    Property.Number(label="DropTime", configurable=True, default_value=3,
                    description="Sekunden, die der Slot geöffnet bleibt. 0 = manuell schließen"),
    Property.Text(label="PayloadOn", configurable=True, default_value="",
                  description="Optionales eigenes ON-Payload. Leer = JSON {\"slot\": x, \"state\": \"on\"}"),
    Property.Text(label="PayloadOff", configurable=True, default_value="",
                  description="Optionales eigenes OFF-Payload. Leer = JSON {\"slot\": x, \"state\": \"off\"}"),
])
class HopDropperActor(CBPiActor):
    """Ein Slot eines MQTT Hopdroppers. Ein Actor pro Slot anlegen."""

    def __init__(self, cbpi, id, props):
        CBPiActor.__init__(self, cbpi, id, props)
        self.power = 100
        self.auto_off_task = None

    async def on_start(self):
        self.topic = self.props.get("Topic", "hopdropper/drop")
        self.slot = int(self.props.get("Slot", 1))
        self.drop_time = float(self.props.get("DropTime", 3))
        self.payload_on = self.props.get("PayloadOn", "")
        self.payload_off = self.props.get("PayloadOff", "")
        self.state = False

    async def on_stop(self):
        self.cancel_auto_off()
        await self.off()

    def cancel_auto_off(self):
        if self.auto_off_task is not None and not self.auto_off_task.done():
            self.auto_off_task.cancel()
        self.auto_off_task = None

    async def publish(self, state):
        if self.cbpi.satellite is None:
            logger.warning("Hopdropper %s: MQTT ist in CraftBeerPi nicht aktiviert", self.id)
            return
        custom = self.payload_on if state else self.payload_off
        if custom:
            payload = custom
        else:
            payload = json.dumps({"slot": self.slot, "state": "on" if state else "off"})
        await self.cbpi.satellite.publish(self.topic, payload, False)

    async def on(self, power=None):
        if power is not None:
            self.power = power
        self.cancel_auto_off()
        await self.publish(True)
        self.state = True
        if self.drop_time > 0:
            self.auto_off_task = asyncio.create_task(self.auto_off())

    async def auto_off(self):
        try:
            await asyncio.sleep(self.drop_time)
            await self.cbpi.actor.off(self.id)
        except asyncio.CancelledError:
            pass

    async def off(self):
        self.cancel_auto_off()
        await self.publish(False)
        self.state = False

    async def set_power(self, power):
        self.power = power
        await self.cbpi.actor.actor_update(self.id, power)

    def get_state(self):
        return self.state

    @action(key="Hopfen abwerfen", parameters=[])
    async def drop(self):
        await self.cbpi.actor.on(self.id)

    async def run(self):
        while self.running:
            await asyncio.sleep(1)


@parameters([
    Property.Kettle(label="Kettle", description="Kessel"),
    Property.Sensor(label="Sensor", description="Temperatursensor zum Starten des Timers"),
    Property.Number(label="Temp", configurable=True,
                    description="Ab dieser Temperatur startet der Timer"),
    Property.Number(label="Timer", configurable=True, default_value=60, description="Kochzeit in Minuten"),
    Property.Select(label="AutoMode", options=["Yes", "No"],
                    description="Kettlelogic automatisch ein- und ausschalten -> Yes"),
    Property.Select(label="LidAlert", options=["Yes", "No"],
                    description="Hinweis zum Abnehmen des Deckels vor dem Kochen"),
    Property.Select(label="First_Wort", options=["Yes", "No"],
                    description="Hinweis auf Vorderwürzehopfung"),
    Property.Text(label="First_Wort_text", configurable=True,
                  description="Name der Vorderwürzehopfen"),
    Property.Number(label="Hop_1", configurable=True,
                    description="Restzeit in Minuten für Hopfengabe 1"),
    Property.Text(label="Hop_1_text", configurable=True, description="Name Hopfengabe 1"),
    Property.Actor(label="Hop_1_Actor"),
    Property.Number(label="Hop_2", configurable=True,
                    description="Restzeit in Minuten für Hopfengabe 2"),
    Property.Text(label="Hop_2_text", configurable=True, description="Name Hopfengabe 2"),
    Property.Actor(label="Hop_2_Actor"),
    Property.Number(label="Hop_3", configurable=True,
                    description="Restzeit in Minuten für Hopfengabe 3"),
    Property.Text(label="Hop_3_text", configurable=True, description="Name Hopfengabe 3"),
    Property.Actor(label="Hop_3_Actor"),
    Property.Number(label="Hop_4", configurable=True,
                    description="Restzeit in Minuten für Hopfengabe 4"),
    Property.Text(label="Hop_4_text", configurable=True, description="Name Hopfengabe 4"),
    Property.Actor(label="Hop_4_Actor"),
    Property.Number(label="Hop_5", configurable=True,
                    description="Restzeit in Minuten für Hopfengabe 5"),
    Property.Text(label="Hop_5_text", configurable=True, description="Name Hopfengabe 5"),
    Property.Actor(label="Hop_5_Actor"),
    Property.Number(label="Hop_6", configurable=True,
                    description="Restzeit in Minuten für Hopfengabe 6"),
    Property.Text(label="Hop_6_text", configurable=True, description="Name Hopfengabe 6"),
    Property.Actor(label="Hop_6_Actor"),
])
class HopDropperStep(CBPiStep):
    """Kochschritt wie der CBPi Boil Step, wirft die Hopfengaben über den Hopdropper ab."""

    HOP_COUNT = 6

    @action(key="Timer starten", parameters=[])
    async def start_timer(self):
        if self.timer.is_running is not True:
            self.cbpi.notify(self.name, "Timer gestartet", NotificationType.INFO)
            self.timer.start()
            self.timer.is_running = True
        else:
            self.cbpi.notify(self.name, "Timer läuft bereits", NotificationType.WARNING)

    @action(key="5 Minuten hinzufügen", parameters=[])
    async def add_timer(self):
        if self.timer.is_running is True:
            self.cbpi.notify(self.name, "5 Minuten hinzugefügt", NotificationType.INFO)
            await self.timer.add(300)
        else:
            self.cbpi.notify(self.name, "Timer muss laufen, um Zeit hinzuzufügen",
                             NotificationType.WARNING)

    async def NextStep(self, **kwargs):
        await self.next()

    async def on_timer_done(self, timer):
        self.summary = ""
        if self.kettle is not None:
            self.kettle.target_temp = 0
        if self.AutoMode is True:
            await self.setAutoMode(False)
        self.cbpi.notify(self.name, "Kochen beendet", NotificationType.SUCCESS)
        await self.push_update()
        await self.next()

    async def on_timer_update(self, timer, seconds):
        self.summary = Timer.format_time(seconds)
        self.remaining_seconds = seconds
        await self.push_update()

    async def on_start(self):
        self.summary = "Warte auf Zieltemperatur"
        self.AutoMode = self.props.get("AutoMode", "No") == "Yes"
        self.lid_alert = self.props.get("LidAlert", "No") == "Yes"
        self.lid_temp = 88 if self.get_config_value("TEMP_UNIT", "C") == "C" else 190
        self.lid_flag = True
        self.kettle = self.get_kettle(self.props.get("Kettle", None))
        self.remaining_seconds = None
        self.hops_added = [False] * self.HOP_COUNT
        self.slot_actors = self.find_slot_actors()
        if self.kettle is not None:
            self.kettle.target_temp = int(self.props.get("Temp", 0))
        if self.AutoMode is True:
            await self.setAutoMode(True)
        if self.timer is None:
            self.timer = Timer(int(self.props.get("Timer", 0)) * 60,
                               on_update=self.on_timer_update,
                               on_done=self.on_timer_done)
        self.timer.is_running = False
        if self.props.get("First_Wort", "No") == "Yes":
            self.cbpi.notify(self.name, "Vorderwürzehopfen: %s"
                             % self.props.get("First_Wort_text", ""), NotificationType.INFO)
        await self.push_update()

    def find_slot_actors(self):
        """Ordnet Slotnummer -> Actor-ID anhand der konfigurierten Hopdropper-Actoren zu."""
        slots = {}
        try:
            for actor in self.cbpi.actor.data:
                if actor.type != HOPDROPPER_ACTOR_NAME:
                    continue
                slot = actor.props.get("Slot", None)
                if slot is not None:
                    slots[int(slot)] = actor.id
        except Exception as e:
            logger.error("Hopdropper-Actoren konnten nicht ermittelt werden: %s", e)
        return slots

    async def on_stop(self):
        if self.timer is not None:
            await self.timer.stop()
        self.summary = ""
        if self.AutoMode is True:
            await self.setAutoMode(False)
        await self.push_update()

    async def reset(self):
        self.timer = Timer(int(self.props.get("Timer", 0)) * 60,
                           on_update=self.on_timer_update,
                           on_done=self.on_timer_done)
        self.hops_added = [False] * self.HOP_COUNT

    async def setAutoMode(self, auto_state):
        try:
            if self.kettle is None:
                return
            if (self.kettle.instance is None or self.kettle.instance.state is False) and auto_state is True:
                await self.cbpi.kettle.toggle(self.kettle.id)
            elif self.kettle.instance.state is True and auto_state is False:
                await self.cbpi.kettle.stop_logic(self.kettle.id)
            await self.push_update()
        except Exception as e:
            logger.error("Kettlelogic %s konnte nicht geschaltet werden: %s",
                         self.props.get("Kettle", None), e)

    async def check_hop_timer(self, number):
        if self.hops_added[number - 1]:
            return
        value = self.props.get("Hop_%s" % number, None)
        if value is None or value == "":
            return
        try:
            value = float(value)
        except (TypeError, ValueError):
            logger.warning("Ungültige Hopfenzeit Hop_%s: %r", number, value)
            return
        if self.remaining_seconds is None or self.remaining_seconds > (value * 60 + 1):
            return
        self.hops_added[number - 1] = True
        hop_name = self.props.get("Hop_%s_text" % number, "") or "Hopfengabe %s" % number
        actor = self.props.get("Hop_%s_Actor" % number, None) or self.slot_actors.get(number)
        if actor is not None:
            await self.cbpi.actor.on(actor)
            self.cbpi.notify(self.name, "%s abgeworfen" % hop_name, NotificationType.INFO)
        else:
            self.cbpi.notify(self.name, "%s hinzufügen" % hop_name, NotificationType.INFO)

    async def run(self):
        while self.running:
            await asyncio.sleep(1)
            sensor_value = self.get_sensor_value(self.props.get("Sensor", None))
            current_temp = sensor_value.get("value") if sensor_value is not None else None
            if current_temp is None:
                continue
            if current_temp >= int(self.props.get("Temp", 0)) and self.timer.is_running is not True:
                self.timer.start()
                self.timer.is_running = True
                completion = datetime.fromtimestamp(
                    time.time() + int(self.props.get("Timer", 0)) * 60)
                self.cbpi.notify(self.name, "Timer gestartet. Ende ca. %s"
                                 % completion.strftime("%H:%M"), NotificationType.INFO)
            else:
                for number in range(1, self.HOP_COUNT + 1):
                    await self.check_hop_timer(number)
            if self.lid_alert and self.lid_flag and current_temp >= self.lid_temp:
                self.lid_flag = False
                self.cbpi.notify(self.name, "Bitte den Deckel abnehmen",
                                 NotificationType.WARNING)
        return StepResult.DONE


def setup(cbpi):
    """Registrierung der Plugin-Komponenten in CraftBeerPi 4."""
    cbpi.plugin.register(HOPDROPPER_ACTOR_NAME, HopDropperActor)
    cbpi.plugin.register(HOPDROPPER_STEP_NAME, HopDropperStep)
