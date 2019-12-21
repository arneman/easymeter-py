import sys
import re
import serial
import traceback
import logging
import time
import multiprocessing
import json
import paho.mqtt.client as mqtt

from config import CONFIG

KEYWORDS = {
    'A+': {'keyword': '1-0:1.8.0', 'dtype': float}, # Meter reading import
    #'A-': '1-0:2.8.0', # Meter reading export
    'L1': {'keyword': '1-0:21.7.255', 'dtype': float}, # Power L1
    'L2': {'keyword': '1-0:41.7.255', 'dtype': float}, # Power L2
    'L3': {'keyword': '1-0:61.7.255', 'dtype': float}, # Power L3
    'In': {'keyword': '1-0:1.7.255', 'dtype': float}, # Power total in
    'SERIAL': {'keyword': '0-0:96.1.255', 'dtype': str}, # Serial number
    #'Out': '1-0:1.7.255', # Power total out
}

def read():
    with serial.Serial(port=CONFIG['dev'], baudrate=9600, bytesize=7, parity='E', timeout=2) as ser:
        while True:
            reading = ser.read(1000).decode("utf-8")
            ser.flushInput()
            if reading.startswith('/'):
                return reading
            # Try again if reading was unsuccessful

def extract(keyword, reading):
    pattern = KEYWORDS[keyword]['keyword']
    match = re.search(r"%s.*?\((.*?)(?:\*(.*?))?\)" % pattern, reading)
    value, unit = match.groups()
    value = KEYWORDS[keyword]['dtype'](value)
    return value, unit


def worker_read_meter(task_queue):
    while True:
        try:
            reading = read()
            print(reading, len(reading))
            if reading and len(reading) == 270:
                results ={'ts': time.time()} 
                for key in KEYWORDS:
                    value, unit = extract(key, reading)
                    results[key] = value
                    print(key,value,unit)
                task_queue.put(results)
        except Exception as e:
            traceback.print_tb(e)
        #time.sleep(0.1)

def worker_publish_mqtt(task_queue):
    client = mqtt.Client()

    def mqtt_connect():
        if CONFIG['mqtt']['auth']['enabled']:
              client.username_pw_set(CONFIG['mqtt']['auth']['username'],
                                     CONFIG['mqtt']['auth']['password'])

        client.connect(host=CONFIG['mqtt']['host'], 
                       port=CONFIG['mqtt']['port'],
                       keepalive=CONFIG['mqtt']['keepalive'],
                       bind_address="")
 
    def mqtt_publish(payload):
        client.publish(topic=CONFIG['mqtt']['topic'], 
                       payload=json.dumps(reading),
                       qos=CONFIG['mqtt']['qos'],
                       retain=CONFIG['mqtt']['retain'])

    mqtt_connect()
    
    while True:
        try:
            if not task_queue.empty():
                reading = task_queue.get()
                mqtt_connect()
                mqtt_publish(reading)
                print('worker_publish_mqtt', reading)
        except Exception as e:
            traceback.print_tb(e)
        time.sleep(0.1)

def run():
    task_queue = multiprocessing.Queue()
    #multiprocessing.log_to_stderr(logging.ERROR)

    multiprocessing.Process(
            target=worker_read_meter, args=(task_queue,)).start()

    multiprocessing.Process(
            target=worker_publish_mqtt, args=(task_queue,)).start()

if __name__ == '__main__':
    run()



## Adapt acutal value
#if keyword == 'In':
#    value = max(value, 0)
#elif keyword == 'Out':
#    value = max(-value, 0)




""" 
/ESY5Q3DA1004 V3.02

1-0:0.0.0*255(0273011003684)
1-0:1.8.0*255(00026107.7034231*kWh)
1-0:21.7.255*255(000200.13*W)
1-0:41.7.255*255(000122.31*W)
1-0:61.7.255*255(000014.01*W)
1-0:1.7.255*255(000336.45*W)
1-0:96.5.5*255(82)
0-0:96.1.255*255(1ESY1011003684) """
