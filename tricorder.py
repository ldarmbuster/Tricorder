#!/usr/bin/env python

###Tricorder
###Purpose: To parse a log file and keep a running mean of every process that is stored in a pickle file and database.


import dataset
import datetime
import dateutil.parser
import os
from pygtail import Pygtail
import re
import socket
from time import sleep
from welford from Welford

try:
    import cPickle as pickle
except:
    import pickle

os.chdir(os.path.dirname(os.path.abspath(__file__)))

hostname = socket.gethostname()

process_dict = {

}


def get_db():
    global db
    global table
    try:
        next(db.query('SELECT 1;'))
        echo('DB GOOD')
    except Exception as e:
        db = dataset.connect('mysql://tricorder:serverAddress:port/tricorder', engine_kwargs={'pool_recycle': 3600})
        table = db['action_performance']
        next(db.query('SELECT 1;'))
        echo('REFRESHED DB')

def echo(line):
    timestamp = datetime.datetime.now().isoformat()
    log_line = '[{0}] {1}'.format(timestamp, line)
    print(log_line)

def process_line(line):
    timestamp, action, duration = line.strip().split('|')
    action = re.sub(r':+\d+$', '', action)
    ts_date, ts_time, ts_ms = timestamp.split()
    dt_timestamp = dateutil.parser.parse('{0}T{1}'.format(ts_date, ts_time).replace('/','-'))
    if action not in process_dict.keys():
        process_dict[action] = [Welford()]
        print('New entry added')
    else:
        print('Entry already exists')
        pass

def process_log():
    pt = Pygtail('/opt/log/location/logfile.log', offset_file='perf.log.offset')
    while True:
        for line in pt.readlines():
            try:
                process_line(line)
            except Exception as e:
                print('Failed line:', line.strip())
                print(e)
                pass
        next(db.query('SELECT 1;'))
        sleep(10)

def dict_db():
    global db
    global table
    for key, value in process_dict.iteritems():
        try:
            table.upsert(dict(action=key, welford_mean_time=value, host=hostname))
        except:
            pass

def pickle_dict():
    with open('process_dict', 'w') as f:
        pickle.dump(process_dict, f)


def main():
    global db
    global table
    while True:
        echo('I am...')
        get_db()
        process_log()
        dict_db()
        pickle_dict()
        echo("working.")
        sleep(30)

db, table = None, None
get_db()

if __name__ == '__main__':
    main()
