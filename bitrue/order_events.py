# -*- coding: utf-8 -*-

import sys, traceback
from threading import Condition, Thread
import threading
import time

try:
    import simplejson as json
except:
    import json

from kafka import KafkaConsumer, metrics
from kafka.errors import KafkaError

class MessageProcesserMix(object):

    def process_message(self, msg):
        raise NotImplementedError("%s not implement process_message" %(self.__class__.__name__))


class ConsumerThread(Thread):

    def __init__(self, topics, processer, **config):
        super(ConsumerThread, self).__init__()
        self.processer = processer
        self.config = config
        self.topics = topics
        self.timer_stop = threading.Event()
        self.timer = None
        self.consumer = None

        if 'auto_offset_reset' not in self.config:
            self.config['auto_offset_reset'] = 'latest'
        if 'consumer_timeout_ms' not in self.config:
            self.config['consumer_timeout_ms'] = 3000

    
    def subscribe(self):
        self.consumer = KafkaConsumer(*self.topics, **self.config)
        # self.consumer.subscribe(self.topics)
        self.timer = StatsReports(60, self.consumer, self.timer_stop, True)
        self.timer.start()
        self.start()
    
    
    
    def run(self):
        try:
            while True:
                msg = self.consumer.poll(100)
                if msg and self.processer:
                    self.processer.process_message(msg)
                # print(msg)

        except KeyboardInterrupt:
            print("detected interrupt. Canceling")
            pass
        except Exception as ex:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            sys.exit(1)
        finally:
            self.timer_stop.set()
            self.consumer.close()
    

class StatsReports(Thread):

    def __init__(self, interval, consumer, event=None, raw_metrics=False) -> None:
        super(StatsReports, self).__init__()
        self.interval = interval
        self.consumer = consumer
        self.event = event
        self.raw_metrics = raw_metrics

    def print_stats(self):
        metrics = self.consumer.metrics()
        if self.raw_metrics:
            print(metrics)
        else:
            print('{records-consumed-rate} records/sec ({bytes-consumed-rate} B/sec),'
                  ' {fetch-latency-avg} latency,'
                  ' {fetch-rate} fetch/s,'
                  ' {fetch-size-avg} fetch size,'
                  ' {records-lag-max} max record lag,'
                  ' {records-per-request-avg} records/req'
                  .format(**metrics['consumer-fetch-manager-metrics']))
    
    def print_final(self):
        self.print_stats()

    def run(self):
        while self.event and not self.event.wait(self.interval):
            self.print_stats()
        else:
            self.print_final()

class PrintProcessMessage(MessageProcesserMix):

    def __init__(self) -> None:
        super().__init__()
    
    def process_message(self, msg):
        # print(msg.keys())
        records = msg.values()
        # rec:
        # [ConsumerRecord(topic='order.event.topic.btcusdt', partition=0, offset=13768242, timestamp=1620807768007, timestamp_type=0, key=None, value=b'{"mid":0,"ts":1620807768007,"symbol":"btcusdt","et":"CREATE","od":{"id":147120366257471488,"uid":10118,"s":"SELL","p":57073.4000,"v":0.0132,"frm":null,"frt":null,"fcr":null,"fcn":null,"ffv":null,"ffcn":null,"fflv":null,"src":"ROBOT","ct":1620807768007,"ot":null}}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=263, serialized_header_size=-1)]
        # print(records)
        # print("\n\n#####\n\n")
        for rec in records:
            # print(rec)
            # print("\n\n****n\n")
            for e in rec:
                # print(e)
                # print("\n\n$$$$$$$\n\n")
                # b'{"mid":0,"ts":1620807768007,"symbol":"btcusdt","et":"CREATE","od":{"id":147120366257471488,"uid":10118,"s":"SELL","p":57073.4000,"v":0.0132,"frm":null,"frt":null,"fcr":null,"fcn":null,"ffv":null,"ffcn":null,"fflv":null,"src":"ROBOT","ct":1620807768007,"ot":null}}
                msg_elements = json.loads(e.value.decode())
                # ord_fields = msg_elements['od']
                # if msg_elements['et'] == 'CREATE':
                #     print("%s-%d: %f@%f %s %d %d" %(msg_elements['symbol'], ord_fields['id'], ord_fields['v'], ord_fields['p'], ord_fields['s'], ord_fields['uid'], ord_fields['ct']))
                # else:
                #     print("%s-%d, %s %d" %(msg_elements['symbol'], ord_fields['id'], ord_fields['s'], ord_fields['uid']))
                print(msg_elements)


if __name__ == '__main__':
    bootstrap_servers = ["b-1.financialnew.82lysy.c3.kafka.ap-southeast-1.amazonaws.com:9092","b-2.financialnew.82lysy.c3.kafka.ap-southeast-1.amazonaws.com:9092"]
    auto_offset_reset = "latest"  # "earliest"
    group_id = "py_echo"
    # topics = ['order.event.topic.btcusdt', 'match.topic.trade']
    topics = ['match.topic.trade', 'match.topic.bbo']  # 'order.event.topic.btcusdt'
    config = {'bootstrap_servers': bootstrap_servers, 'group_id': group_id, 'auto_offset_reset':auto_offset_reset}

    prt_msg = PrintProcessMessage()

    consumer = ConsumerThread(topics, prt_msg, **config)
    consumer.subscribe()

    while True:
        time.sleep(1.0)
    
