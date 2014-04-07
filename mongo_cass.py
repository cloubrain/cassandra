# Moves data from MongoDB:vmware to Cassandra
#  Runs in background continuously
#  Gets data in chunks from Mongo and writes to Cassandra
#
# ssh into the Cassandra server:
#  ssh cassandra2
#
########################################################

import os
import sys
import time
import cbmongo
import cbcassandra

# Cassandra connection
chost = 'localhost'
#cport = 9160
keyspace = "demo11"
#keyspace = "osarchive"
#keyspace = "Cloubrain_Monitor_Archive"   # Production openstack archive
cass = cbcassandra.CBcassandra(chost, keyspace)
cass.setup_schema()

# MongoBD
dcname = "dkan-cluster-1-dc-19"  # 21Vms 26.1M. 100K takes 
dcname = "dkan-cluster-1-da-96"  # 30VMs 27.5M. 46K takes 20hrs.
dcname = "dkan-cluster-1-da-20"  # 46VMs 55.8M. 100K takes 
dcname = "dkan-standalone-esxi"  # 18VMs 17.7M. 100K takes 
dcname = "dkan-cluster-2-DC"     # 11VMs 10.0M. 100K takes
  # 3000/800: 10min
  # 3000/400: 15min
  # 2000/400: 9min
  # 2000/200: 10min
  # 2000/400(batch): 
cbmongo = cbmongo.CBmongo(dcname)  #init CBmongo
#cbdb.testdb()  # tests functions
#cbdb.getStats()
print cbmongo.getClusters('')

perfdata = {}
perfdata["DCname"] = dcname
perfdata["vmname"] = ''
perfdata["time_current"] = 0
perfdata["platform"] = 'vmware'
perfdata["pname"] = ''
perfdata["pvalue"] = 0

datanum = 2000
datachunk = 200 # chunk of 200-400 (x16 perfmetrics) is efficient
place = 0

# vms = cbmongo.getData(dcname, 0, 10)

print ' [*] Migrating data from MongoDB to Cassandra'

while place < datanum:
    vmdata = cbmongo.getData(dcname, place, place+datachunk)
    place = place+datachunk+1
    print place
    cluster, cur = cass.getclustsess(keyspace)

    for vm in vmdata:
        perfdata["vmname"] = vm
        for st in vmdata[vm]:
            perfdata["time_current"] = st["time_current"]
            #batchstr = ""
            #cur.execute("BEGIN BATCH ")
            for perf in st["perfMetrics"]:
                perfdata["pname"] = perf['perfkey']
                v = perf['v']
                perfdata["pvalue"] = int(float(v))
                #cass.store(perfdata)
                #insertstr = cass.return_insert(perfdata)
                #batchstr = batchstr + insertstr
                cass.insert_datasimple(cur, perfdata)
            #cass.batch_insert(cur, batchstr)
            #cur.execute("APPLY BATCH;")
    
    cur.shutdown()
    time.sleep(1)

cass.read_simple()
cass.teardown()
print "* DONE *"
