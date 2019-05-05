# Moves data from RabbitMQ queue to Cassandra
#  Runs in background continuously
#  Pushes data to Cassandra for each msg RabbitMQ gets from data collector
#
# ssh into the Cassandra server:
#  ssh cassandra
#
# RabbitMQ vizualization:
# http://host:15672/
# user: guest
# pass: guest
#
# to change this, kill the process: rabbitoscassandra.py
# python rabbitoscassandra.py
#
########################################################

import os
import sys
import time
import cql
import pika
import cPickle as pickle
#from pymongo import Connection
import json
import traceback
import cbcassandra

# Cassandra connection
chost = 'localhost'
#cport = 9160

keyspace = "demo1"
#keyspace = "osarchive"
#keyspace = "Cloubrain_Monitor_Archive"   # Production openstack archive

cass = cbcassandra.CBcassandra(chost, keyspace)
cass.setup_schema()

""" # MongoBD
dbhost = 'datacollector.cloubrain.com'
dbconnection = Connection(dbhost,27017)
db = dbconnection['openstack']
collection = db['stats_new']"""

# RabbitMQ connection
rabbithost = 'datacollector.cloubrain.com'
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbithost))
channel = connection.channel()
exchange = 'openstack_fuel12'
queue_name= 'openstack_fuel_cass1'
channel.queue_bind(exchange=exchange, queue=queue_name)

print ' [*] Waiting for messages'

def callback(ch, method, properties, body):
    post= json.loads(body)
    #cass = CBcassandra(chost, keyspace)
    try:
        ip = ''
        dcname = ''
        try:
                ip = post["ip"]
        except:
                print traceback.print_exc()
                pass
        try:
                dcname = post["DCname"]
        except:
                print traceback.print_exc()
                pass
        record = {}
        record["ip"] = ip
        record["DCname"] = dcname
        #record["time_current"] = time.time()
        record["data"] = post
        
        cass.store(record)
        #cass.read_osarchive()
    except:
        print traceback.print_exc()
        pass
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=queue_name)
channel.start_consuming()
