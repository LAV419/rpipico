from mqtt_as import MQTTClient
from mqtt_local import config
import uasyncio as asyncio
import dht, machine, ujson

d = dht.DHT22(machine.Pin(15))
relay = machine.Pin(2, machine.Pin.OUT)
led_board = machine.Pin("LED", machine.Pin.OUT) 

event_mqtt = asyncio.Event()
mqtt_messages = []

id = "".join("{:02X}".format(b) for b in machine.unique_id())
print(id)

def load_params():
    try:
        with open("params.json", "r") as f:
            params = ujson.load(f)
    except (OSError, ValueError):
        params = {"setpoint": 25, "periodo": 10, "modo": "auto", "rele": 0}
        save_params(params)
    return params

def save_params(params):
    with open("params.json", "w") as f:
        ujson.dump(params, f)

params = load_params()
relay.value(params["rele"])

SERVER = config['server']

def mqtt_handler(topic, msg, retained):
    global mqtt_messages
    mqtt_messages.append((topic.decode(), msg.decode()))
    event_mqtt.set()

async def process_mqtt_messages():
    global mqtt_messages, params
    while True:
        await event_mqtt.wait()
        while mqtt_messages:
            topic, msg = mqtt_messages.pop(0)
            print(f"Procesando: {topic} -> {msg}")

            if topic.endswith("/setpoint"):
                params["setpoint"] = int(msg)
            elif topic.endswith("/periodo"):
                params["periodo"] = int(msg)
            elif topic.endswith("/modo"):
                params["modo"] = msg
            elif topic.endswith("/rele"):
                asyncio.create_task(rele(msg))
            elif topic.endswith("/destello"):
                asyncio.create_task(destello())
            
            save_params(params)
        event_mqtt.clear()

async def destello():
    for _ in range(5):
        led_board.value(1) 
        await asyncio.sleep(0.5)
        led_board.value(0) 
        await asyncio.sleep(0.5)

async def rele(msg):
    if msg == "rele":
        params["rele"] = 1 - params["rele"]
        relay.value(params["rele"])
        await asyncio.sleep(0.5)


async def wifi_han(state):
    print("WiFi", "conectado" if state else "desconectado")
    await asyncio.sleep(1)

async def conn_han(client):
    topics = ["setpoint", "periodo", "modo", "rele", "destello"]
    for t in topics:
        await client.subscribe(f"{id}/{t}", 1)
        await asyncio.sleep(0)

async def main(client):
    await client.connect()
    asyncio.create_task(process_mqtt_messages())
    await asyncio.sleep(2)

    while True:
        d.measure()
        temp = d.temperature()
        hum = d.humidity()

        if params["modo"] == "auto":
            relay.value(1 if temp < params["setpoint"] else 0)
        
        payload = ujson.dumps({
            "temperatura": temp,
            "humedad": hum,
            "setpoint": params["setpoint"],
            "periodo": params["periodo"],
            "modo": params["modo"],
            "rele": relay.value(),
        })
        await client.publish(f"{id}", payload, qos=1)

        await asyncio.sleep(params["periodo"])

config.update({
    "subs_cb": mqtt_handler,
    "server": SERVER,
    "connect_coro": conn_han,
    "wifi_coro": wifi_han,
    "ssl": True,
})

MQTTClient.DEBUG = True
client = MQTTClient(config)

try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()
