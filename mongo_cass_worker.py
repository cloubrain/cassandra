# Moves data from MongoDB:vmware to Cassandra
#  Runs in background continuously
#  Gets data in chunks from Mongo and writes to Cassandra
#
########################################################

import os
import sys
import time
import traceback
import cPickle as pickle
import cbmongo
import cbcassandra

chost = 'localhost'
keyspace = 'test'
dcname = "dkan-cluster-1-dc-19"
datanum = 10000
datachunk = 1000 # chunk of 1-2k (x16 perfmetrics) is efficient
place = 0

chost = sys.argv[1]
keyspace = sys.argv[2]
dcname = sys.argv[3]
place = int(sys.argv[4])
datanum = int(sys.argv[5])
datachunk = int(sys.argv[6])
#subprocess.Popen(["python", "mongo_cass_worker.py", chost, keyspace, dc, place, datanum, datachunk])

# Cassandra connection
cass = cbcassandra.CBcassandra(chost, keyspace)

# MongoBD
cbm = cbmongo.CBmongo(dcname)  #init CBmongo

perfdata = {}
perfdata["DCname"] = dcname
perfdata["vmname"] = ''
perfdata["time_current"] = 0
perfdata["platform"] = 'vmware'
perfdata["pname"] = ''
perfdata["pvalue"] = 0

def insertBatch(vmdata, keyspace):
    cluster, cur = cass.getclustsess(keyspace)
    for vm in vmdata:
        perfdata["vmname"] = vm
        for st in vmdata[vm]:
            perfdata["time_current"] = st["time_current"]
            for perf in st["perfMetrics"]:
                perfdata["pname"] = perf['perfkey']
                v = perf['v']
                perfdata["pvalue"] = int(float(v))
                timebucket = cass.insert_datasimple(cur, perfdata)
    cur.shutdown()
    return timebucket

#print ' [*] Migrating data from MongoDB to Cassandra'
timebucket_list = []
timebucket = ""
filen = 'tb_' + dcname + '.tb'
"""with open(filen, 'rb') as handle:
    timebucket_list = pickle.loads(handle.read())
"""
f = open(filen, 'rb')
timebucket_list = pickle.loads(f.read())
f.close()

ts = time.time()
i = 0
while i < datanum:
    vmdata = cbm.getData(dcname, place, datachunk)
    i = i+datachunk
    #print "****** " + dcname
    #print place
    try:
        timebucket = insertBatch(vmdata, keyspace)
    except:
        time.sleep(17)
        try:
            timebucket = insertBatch(vmdata, keyspace)
        except:
            print traceback.print_exc()
            pass
        pass
    print timebucket
    if timebucket not in timebucket_list:
        print "*******************"
        print dcname
        print timebucket_list
        timebucket_list.append(timebucket)
        print timebucket_list
        print "*******************"
    place = place + datachunk
    time.sleep(3)

#cass.read_simple()
#cass.teardown()
#print "* DONE *"
tf = time.time()
tt = tf-ts
#print tt

print "*********-----------**********"
print dcname
print timebucket_list
print "*********-----------**********"

# write the timebucket_list to a file
"""with open(filen, 'wb') as handle:
    pickle.dump(timebucket_list, handle)
"""
f = open(filen, 'wb')
pickle.dump(timebucket_list, f)
f.close()
#print timebucket_list


