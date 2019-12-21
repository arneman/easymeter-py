import sys
import re
import serial
import logging
import time
import multiprocessing
import json


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


def worker_read_meter(task_queues):
    task_queues = task_queues[:-1]  #remove last entry because is a list with all other queues (=the argument for this worker)
    logger = multiprocessing.get_logger()
    while True:
        try:
            reading = read()
            if reading and len(reading) == 270:
                reading_dict ={'ts': time.time()} 
                for key in KEYWORDS:
                    value, unit = extract(key, reading)
                    reading_dict[key] = value
                    logger.debug((key,value,unit))
                #put the reading_dict into all publishing queues
                for queue in task_queues:
                    queue.put(reading_dict)
        except:
            logger.exception('Error in worker_read_meter')

def worker_publish_mqtt(task_queue):
    import paho.mqtt.client as mqtt
    logger = multiprocessing.get_logger()
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
        mqtt_connect()
        return client.publish(topic=CONFIG['mqtt']['topic'], 
                              payload=json.dumps(reading),
                              qos=CONFIG['mqtt']['qos'],
                              retain=CONFIG['mqtt']['retain'])

    while True:
        try:
            if not task_queue.empty():
                reading = task_queue.get()
                mqtt_publish(reading)
                logger.debug('worker_publish_mqtt' + json.dumps(reading))
        except:
            logger.exception('Error in worker_publish_mqtt')
        time.sleep(0.1)

def worker_sqlite(task_queue):
    raise NotImplementedError

def worker_logfile(task_queue):
    raise NotImplementedError

def run():
    multiprocessing.log_to_stderr(CONFIG['loglevel'])
    multiprocessing.get_logger().setLevel(CONFIG['loglevel'])

    #target functions for publishing services
    targets ={'mqtt': worker_publish_mqtt,
              'logfile': worker_logfile,
              'sqlite': worker_sqlite} 

    #prepare workers (create queues, link target functions)
    worker_args = [] 
    worker_targets = [] 
    for key in targets:
        if CONFIG[key]['enabled']:
            worker_args.append(multiprocessing.Queue())
            worker_targets.append(targets[key])
    #now add worker_read_meter and give him a ref to all queues as argument
    worker_args.append(worker_args)
    worker_targets.append(worker_read_meter)

    #start workers
    processes = [] 
    for idx,_ in enumerate(worker_targets):
        p = multiprocessing.Process(target=worker_targets[idx],
                                    args=(worker_args[idx],))
        p.daemon = True #main process kills children before it will be terminated
        p.start()
        processes.append(p)

    # because we use deamon=True, the main process has to be kept alive
    while True:
        time.sleep(1)

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
