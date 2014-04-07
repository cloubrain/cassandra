#!/usr/bin/env python2
#
# Cloubrain Cassandra
#  Pete C Perlegos, Cloubrain Inc. 2014
#

import time
from cassandra.cluster import Cluster
import cPickle as pickle
import datetime
#import logging

class CBcassandra(object):
    '''
    Class to write data to Cassandra
    '''
    def __init__(self, chost, keyspace="demo2"):
        """ initialize Class objects """
        self.chost = chost
        self.keyspace = keyspace
        # Tables: see cassandra_schema.txt
        self.osarchivebulk = "data_archive_openstackbulk"
        self.osarchive = "data_archive_openstack"
        self.simple = "DataSimple"
        self.datastore = "DataStore"
        self.ds_meta = "ds_metadata" # Row Partion of DataStore

    def getclustsess(self, keyspace=None):
        """ Return a Cluster instance and a session object """
        cluster = Cluster([self.chost])
        if keyspace:
            session = cluster.connect(keyspace)
        else:
            session = cluster.connect()
        return cluster, session

    def setup_schema(self):
        """ Creates our schema. Leave the flexiblility for now. """
        cluster, cur = self.getclustsess()
        CQLString = ("CREATE KEYSPACE " + self.keyspace +
                 " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};")
        cur.execute(CQLString)
        cur.set_keyspace(self.keyspace)
        tables = {}
        # Specify the schema for any repos we need
        tables[self.osarchivebulk] = ("CREATE TABLE " + self.osarchivebulk +
                            " (DCname_yymmddhh text, DCname text, "
                            "ip text, time_current timestamp, vmsdata blob, "
                            "PRIMARY KEY (DCname_yymmddhh, time_current));")
        tables[self.osarchive] = ("CREATE TABLE " + self.osarchive +
                        " (DCname_yymmddhh text, DCname text, ip text, "
                        "time_current timestamp, hostlist blob, hypervisors blob, "
                        "hostnames blob, hostdata blob, vmsdata blob, "
                        "PRIMARY KEY (DCname_yymmddhh, time_current));")
        #cur.execute(tables[self.osarchivebulk])
        #cur.execute(tables[self.osarchive])

        # This ignores Row Partition for now. Partition = 0.
        # I added platform: vmware, openstack, aws, rackpace, ...
        # I will make a worker to try to migrate a day of info. If it looks good, we can do more.
        tables[self.simple] = ("""CREATE TABLE """ + self.simple + """ (
                        dc_name text,
                        time_bucket text,
                        partition int,
                        platform text,
                        vm_name text,
                        measurement_time timestamp,
                        perf_name text,
                        perf_data int,
                        PRIMARY KEY ((dc_name, time_bucket, partition), perf_name, vm_name, measurement_time)
                        );""")
        cur.execute(tables[self.simple])
        self.createindex(cur, self.simple, "dc_name")
        self.createindex(cur, self.simple, "time_bucket")
        self.createindex(cur, self.simple, "platform")
        self.createindex(cur, self.simple, "perf_name")
        self.createindex(cur, self.simple, "vm_name")
        print "Keyspace created: " + self.keyspace
        cur.shutdown()

    def teardown(self):
        cluster, cur = self.getclustsess()
        # remove the keyspace so we can add it again to test
        cur.execute("DROP KEYSPACE " + self.keyspace + ";")
        print "Keyspace dropped: " + self.keyspace
        cur.shutdown()

    def createindex(self, cur, tableName, indexName):
        # remove the keyspace so we can add it again to test
        cur.execute("CREATE INDEX ON " + tableName + "(" + indexName + ");")

    def insert_osarchivebulk(self, cur, vmdata):
        #cluster, cur = self.getclustsess(self.keyspace)
        query = "INSERT INTO " + self.osarchivebulk + " (DCname_yymmddhh, DCname, ip, time_current, vmsdata) VALUES (%(DCname_yymmddhh)s, %(DCname)s, %(ip)s, %(time_current)s, %(vmsdata)s)"
        timenow = time.gmtime()
        current_hour = time.strftime("%Y%m%d%H", timenow)
        dcname = vmdata["DCname"]
        rowkey = dcname + "_" + current_hour
        print rowkey
        timen = datetime.datetime.utcnow()
        print timen

        values = {'DCname_yymmddhh': rowkey,
              'DCname': dcname,
              'ip': vmdata["ip"],
              'time_current': timen,
              'vmsdata': bytearray(pickle.dumps(vmdata["data"]), encoding="hex")}
        cur.execute(query, values)
        #cur.shutdown()
        print "INSERT into " + self.osarchivebulk

    def insert_osarchive(self, cur, vmdata):
        #cluster, cur = self.getclustsess(self.keyspace)
        query = "INSERT INTO " + self.osarchive + " (DCname_yymmddhh, DCname, ip, time_current, hostlist, hypervisors, hostnames, hostdata, vmsdata) VALUES (%(DCname_yymmddhh)s, %(DCname)s, %(ip)s, %(time_current)s, %(hostlist)s, %(hypervisors)s, %(hostnames)s, %(hostdata)s, %(vmsdata)s)"
        timenow = time.gmtime()
        current_hour = time.strftime("%Y%m%d%H", timenow)
        dcname = vmdata["DCname"]
        rowkey = dcname + "_" + current_hour
        print rowkey
        timen = datetime.datetime.utcnow()
        print timen
        vmsdata = vmdata["data"]

        values = {'DCname_yymmddhh': rowkey,
                  'DCname': dcname,
                  'ip': vmdata["ip"],
                  'time_current': timen,
                  'hostlist': bytearray(pickle.dumps(vmsdata["hostlist"]), encoding="hex"),
                  'hypervisors': bytearray(pickle.dumps(vmsdata["hypervisors"]), encoding="hex"),
                  'hostnames': bytearray(pickle.dumps(vmsdata["hostnames"]), encoding="hex"),
                  'hostdata': bytearray(pickle.dumps(vmsdata["hostdata"]), encoding="hex"),
                  'vmsdata': bytearray(pickle.dumps(vmsdata["vms"]), encoding="hex")}
        cur.execute(query, values)
        #cur.shutdown()
        print "INSERT into " + self.osarchivebulk

    def insert_datasimple(self, cur, vmdata):
        #cluster, cur = self.getclustsess(self.keyspace)
        query = "INSERT INTO " + self.simple + " (dc_name, time_bucket, partition, platform, vm_name, measurement_time, perf_name, perf_data) VALUES (%(DCname)s, %(timebucket)s, %(partition)s, %(platform)s, %(vmname)s, %(mtime)s, %(perfname)s, %(perfdata)s)"
        datatime = vmdata["time_current"]
        datadate = datetime.datetime.fromtimestamp(datatime)
        yymmddhh = datadate.strftime("%Y%m%d%H")
        #print yymmddhh
        #print vmdata
        timen = datetime.datetime.utcnow()

        values = {'DCname': vmdata["DCname"],
              'timebucket': yymmddhh,
              'partition': 0,
              'platform': vmdata["platform"],
              'vmname': vmdata["vmname"],
              'mtime': datadate,
              'perfname': vmdata["pname"],
              'perfdata': vmdata["pvalue"]}
        
        #print query
        #print values
        cur.execute(query, values)
        #cur.shutdown()
        #print "INSERT into " + self.simple

    def return_insert(self, vmdata):
        query = "INSERT INTO " + self.simple + " (dc_name, time_bucket, partition, platform, vm_name, measurement_time, perf_name, perf_data) "
        v = "VALUES (%(DCname)s, %(timebucket)s, %(partition)s, %(platform)s, %(vmname)s, %(mtime)s, %(perfname)s, %(perfdata)s)"
        datatime = vmdata["time_current"]
        datadate = datetime.datetime.fromtimestamp(datatime)
        yymmddhh = datadate.strftime("%Y%m%d%H")
        timen = datetime.datetime.utcnow()
        values = {'DCname': vmdata["DCname"],
              'timebucket': yymmddhh,
              'partition': 0,
              'platform': vmdata["platform"],
              'vmname': vmdata["vmname"],
              'mtime': datadate,
              'perfname': vmdata["pname"],
              'perfdata': vmdata["pvalue"]}
        #print (query, values)
        #values = "VALUES (" + vmdata["DCname"] + ", " + yymmddhh + ", 0, " + vmdata["platform"] + ", " + vmdata["vmname"] + ", " + str(datadate) + ", " + vmdata["pname"] + ", " + str(vmdata["pvalue"]) + "); "
        qv = query + values
        print qv
        return qv

    def batch_insert(self, cur, qstr):
        query = "BEGIN BATCH " + qstr + "APPLY BATCH;"
        print query
        cur.execute(query)

    def store(self, vmdata):
        cluster, cur = self.getclustsess(self.keyspace)
        #self.insert_osarchivebulk(cur, vmdata)
        #self.insert_osarchive(cur, vmdata)
        # vizdata = self.computeVisualData(vmdata)
        #self.insert_visual(vizdata)
        self.insert_datasimple(cur, vmdata)
        cur.shutdown()

    def read_osarchivebulk(self):
        cluster, cur = self.getclustsess(self.keyspace)
        rows = cur.execute("SELECT * FROM " + self.osarchivebulk + ";")
        for row in rows:
            print(row)
        cur.shutdown()
        rowitem = rows[0]
        originaldict = pickle.loads(str(rowitem.vmsdata))
        print originaldict

    def read_osarchive(self):
        cluster, cur = self.getclustsess(self.keyspace)
        rows = cur.execute("SELECT * FROM " + self.osarchive + ";")
        for row in rows:
            print(row)
        cur.shutdown()
        rowitem = rows[0]
        originaldict = pickle.loads(str(rowitem.vmsdata))
        print originaldict

    def read_simple(self):
        cluster, cur = self.getclustsess(self.keyspace)
        rows = cur.execute("SELECT * FROM " + self.simple + ";")
        for row in rows:
            print(row)
        cur.shutdown()

"""
### TEST ###
keyspace = "demo2"
#keyspace = "osarchive"
#keyspace = "Cloubrain_Monitor_Archive"   # Production openstack archive

cass = cbcassandra.CBcassandra(chost, keyspace)
#cass.teardown()
cass.setup_schema()
"""

"""
CREATE TABLE DataStore (
  dc_name text,
  time_bucket int,
  partition int,
  perf_name text,
  vm_name text,
  measurement_time timestamp,
  perf_data text,
  PRIMARY KEY ((dc_name, time_bucket, partition), perf_name, vm_name, measurement_time)
);
CREATE INDEX ON DataStore(dc_name);
CREATE INDEX ON DataStore(time_bucket);
CREATE INDEX ON DataStore(perf_name);
CREATE INDEX ON DataStore(vm_name);

CREATE TABLE dsMetaData (
  dc_name text,
  time_bucket int,
  partition int,
  num_partitions int,
  max_len int,
  oldest_ts timestamp,
  PRIMARY KEY ((dc_name, time_bucket, partition))
);
CREATE INDEX ON dsMetaData(dc_name);
CREATE INDEX ON dsMetaData(time_bucket);
"""


