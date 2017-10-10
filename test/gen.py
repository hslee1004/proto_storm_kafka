#!/usr/bin/python

import json
import random
# from datetime import datetime, timedelta
import datetime
import time
import pytz
from pytz import timezone
import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--max', type=int, dest='max_count', default=10)
args = parser.parse_args()
max_count = args.max_count

event_type = [
    'login',
    'logout',
    'game_installation_attempt',
    'game_installation_success',
    'game_installation_cancelled',
    'game_started',
    'game_stopped',
    'game_installation_paused',
    'game_installation_resumed',
    'game_installation_failed'
]

event = {
  'received_time': '20171003T003949.533Z',
  'target_version': '',
  'event_type': 'game_in_progress',
  'client_ip': '171.97.47.16',
  'user_no': 76586554,
  'source_version': '',
  'cmts': '{}',
  'product_id': '10000',
  'install_type': '',
  'patcher_type': '',
  'channel_id': 'nxth',
  'user_agent': 'NexonLauncher.nxl-17.07.02-164-312d35d',
  'device_id': 'a8afbfac27476c82e4e3a0865ff5829c5c726ba48dfc5db1d632a9a75920b72c'
}

def datetime_format_utc(utc_datetime):
    usec = utc_datetime.microsecond
    return "%s.%03dZ" % (utc_datetime.strftime("%Y%m%dT%H%M%S"), round(usec / 1000.))

cnt  = 0
while cnt <= max_count:
    event['received_time'] = datetime_format_utc(datetime.datetime.utcnow())
    event['event_type'] = event_type[random.randint(0, len(event_type)-1)]
    event['user_no'] = random.randint(10000000, 20000000)
    event['client_ip'] = '208.85.104.%s' % random.randint(200,220)
    event['product_id'] = '1000%s' % random.randint(0,9)
    event['user_agent'] = 'NexonLauncher.nxl-17.07.0%s-164-312d35d' % random.randint(0,9)
    event['device_id'] = '%sa8afbfac27476c82e4e3a0865ff5829c5c726ba48dfc5db1d632a9a75920b' % random.randint(0,20)
    js = json.dumps(event)
    print js
    cnt +=1
    time.sleep(0.01)
