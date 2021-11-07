import asyncio
import os, random, time, base64
import signal
import aioredis
import json
redis = aioredis.from_url("redis://localhost",  db=0)

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

from gmqtt import Client as MQTTClient
# gmqtt also compatibility with uvloop  
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

STOP = asyncio.Event()

async def process_packet(client, payload, topic):
    # Convert payload packet to json
    json_payload = json.loads(payload)
    if 'payload' in json_payload:
        # Within the payload split the packet by ,        
        raw_payload = json_payload['payload']

    if 'data' in json_payload:
        raw_payload = json_payload['data']['_raw']
        logging.info(raw_payload)
        decodedBytes = base64.b64decode(raw_payload)
        raw_payload = decodedBytes.decode("utf-8").rstrip()

    raw_payload_split_checksum = raw_payload.split('*')
    # We can check the checksum validaty.

    payload_tokens = raw_payload_split_checksum[0].split(',')

    #  Parse packet according to flight document
    #  - First token will be payload ID/name
    payload_id = payload_tokens[0].lower()
    if payload_id[2:] == str(topic[4:]):
        logging.info('Topic matches Payload ID') 
        payload_key = '{}-{}'.format(payload_id[2:], time.time())
        await redis.set(str(payload_key), payload)
    else:
        logging.error('Topic does not match Payload Name')
        return

    flightdoc = 'flightdoc-{}'.format(payload_id[2:])
    result = await redis.exists(str(flightdoc))
    if result == 1:
        telem_string = await redis.get(str(flightdoc))
        telem_string = telem_string.decode('utf-8')       
        logging.info('Using saved flight doc')
    else:
        logging.error('Missing Flight Doc')
        return
#        telem_string = '$$TEST,latitude,longitude,altitude,temperature_external'

    telem_string_tokens = telem_string.lower().split(',')

    parsed_dict = {}
    parsed_dict['parse_time'] = time.time()

    for index, token in enumerate(payload_tokens, start=0): 
        if token[0:2] == '$$':
            logging.info('Found ID: {}'.format(token))
            parsed_dict['payload_id'] = token[2:]
        else:
            try:
                parsed_dict[telem_string_tokens[index]] = token
            except:
                pass

    parsed_json_string = json.dumps(str(parsed_dict))
    logging.info(parsed_json_string)
    client.publish('parsed/{}'.format(payload_id[2:]), str(parsed_json_string), qos=0)

def on_connect(client, flags, rc, properties):
    print('Connected')
    client.subscribe('raw/#', qos=0)

def on_message(client, topic, payload, qos, properties):
    print('RECV MSG: {} {}'.format(payload, topic))
    task = loop.create_task(process_packet(client, payload, topic.lower()))

def on_disconnect(client, packet, exc=None):
    print('Disconnected')

def on_subscribe(client, mid, qos, properties):
    print('SUBSCRIBED')

def ask_exit(*args):
    STOP.set()

async def main(broker_host, token):
    client = MQTTClient("client-id")

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

#    client.set_auth_credentials(token, None)
    await client.connect(broker_host, 1883)


    await STOP.wait()
    await client.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    host = '78.129.241.16'
#    token = os.environ.get('FLESPI_TOKEN')
    token = 'test'

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main(host, token))
