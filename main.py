import sys
import re
import serial
import logging
import time
import multiprocessing
import json
import datetime
import os


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

TS_FORMAT = '%Y-%m-%d %H:%M:%S'

SQLITE_CREATE = """CREATE TABLE IF NOT EXISTS meter_data
                   ( meter TEXT
                    ,l1 NUMERIC
                    ,l2 NUMERIC
                    ,l3 NUMERIC
                    ,load NUMERIC
                    ,kwh NUMERIC
                    ,TS TEXT DEFAULT CURRENT_TIMESTAMP);"""

def read():
    with serial.Serial(port=CONFIG['dev'], baudrate=9600, bytesize=7, parity='E', timeout=2.5, exclusive=True) as ser:
        #reading = ser.read(500).decode("utf-8")
        reading = ser.read_until(b'!').decode("utf-8")
        ser.reset_input_buffer()

        if reading.startswith('/'):
            return (True, reading)
        time.sleep(0.5) #wait to reach the right cycle
        return (False, reading)

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
            success, reading = read()
            logger.debug(f'reading: {reading}, len: {len(reading)}')

            if success: # and len(reading) == 270:
                if CONFIG['utc']:
                    ts = datetime.datetime.utcnow()
                else:
                    ts = datetime.datetime.now()
                reading_dict ={'ts': ts.strftime(TS_FORMAT)} 
                for key in KEYWORDS:
                    value, unit = extract(key, reading)
                    reading_dict[key] = value
                    logger.debug((key,value,unit))
                #put the reading_dict into all publishing queues
                for queue in task_queues:
                    queue.put(reading_dict)
            else:
                logger.error(f'reading failed {reading}')
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
    import sqlite3
    logger = multiprocessing.get_logger()

    while True:
        try:
            # TODO: Take care the queue doesnt get too large (in case of insert issues here)

            if task_queue.qsize() >= CONFIG['sqlite']['min_rows_insert']:
                #get readings and build sqlite filenames (maybe different fnames because of timestamp)
                readings = {}
                while not task_queue.empty():
                    reading = task_queue.get()
                    logger.debug(reading)
                    #build sqlite filename with timestamp
                    ts = datetime.datetime.strptime(reading['ts'], TS_FORMAT)
                    reading['ts_datetime'] = ts
                    fname = ts.strftime(CONFIG['sqlite']['fname'])
                    logger.debug(fname)
                    
                    #put into dict
                    if fname not in readings:
                        readings[fname] =[]
                    readings[fname].append(reading)
                
                #insert readings with bulk insert statements
                for fname in readings:
                    create_new = False
                    if not os.path.exists(fname):
                        #create new db
                        create_new = True

                    #connect to db
                    conn = sqlite3.connect(fname)
                    c = conn.cursor()
                    logger.debug(f'connected to {fname}')

                    #build insert stmnt
                    sql = """INSERT INTO meter_data
                             (meter, l1, l2, l3, kwh, ts) 
                             VALUES (?,?,?,?,?,?);"""
                    params = [(reading['SERIAL'], reading['L1'],
                               reading['L2'], reading['L3'], reading['A+'], 
                               reading['ts_datetime'].strftime('%Y-%m-%d %H:%M:%S'))
                              for reading in readings[fname]] # [(row 1 col 1, row 1 col 2, ...), (...), ... ]

                    try:
                        if create_new:
                            logger.debug('setting up new db...')
                            c.execute(SQLITE_CREATE)

                        #logger.debug(sql)
                        #logger.debug(params)
                        c.executemany(sql, params)
                        conn.commit()
                        logger.debug(f'insert into {fname} was successful. '
                                     f'Inserted {len(readings[fname])} readings.')
                    except:
                        logger.exception(f'insert into {fname} failed')

                        #add to queue again
                        for reading in readings[fname]:
                            logger.debug(f'add {reading} to queue again')
                            task_queue.put(reading)
                    
                    #close db
                    c.close()
                    conn.close()
                    logger.debug(f'closed connection to {fname}')
                
        except:
            logger.exception('Error in worker_sqlite')
        time.sleep(1)

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
