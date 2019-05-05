# Moves data from MongoDB:vmware to Cassandra
#  Runs in background continuously
#  Gets data in chunks from Mongo and writes to Cassandra
#
# scp files into the Cassandra server:
#  mongo_cass.py, mongo_cass_worker.py, cbcassandra.py, cbmongo.py
#
# ssh into the Cassandra server:
#  ssh cbcass
#  nohup python mongo_cass.py > my.out &
#
# When done get the bucketlist files:
#  mkdir tbout
#  mv *.tb tbout
#  tar -zcvf tb.tar.gz tbout
#  from local machine: scp cbcass:tb.tar.gz .
#         tar -zxvf tb.tar.gz
#
########################################################

import os, subprocess
import sys
import time
#import traceback
import cPickle as pickle
import cbcassandra

# Cassandra connection
chost = 'localhost'
keyspace = "vmwarchive1"
#keyspace = "Cloubrain_Monitor_Archive"   # Production openstack archive
cass = cbcassandra.CBcassandra(chost, keyspace)
cass.setup_schema()

# MongoBD info
#dclist = ["dkan-cluster-1-dc-19", "dkan-cluster-1-da-96", "dkan-cluster-1-da-20", "dkan-standalone-esxi", "dkan-cluster-2-DC"]
#dcentries = [26000000, 27000000, 55000000, 17500000, 200000]
#dcname = "dkan-cluster-1-dc-19"  # 21Vms 26.1M
#dcname = "dkan-cluster-1-da-96"  # 30VMs 27.5M
#dcname = "dkan-cluster-1-da-20"  # 46VMs 55.8M
#dcname = "dkan-standalone-esxi"  # 18VMs 17.7M  Problems in the transfer at 620K
#dcname = "dkan-cluster-2-DC"     # 11VMs 10.0M  Problems in the transfer at 200K

dclist = ["dkan-cluster-1-dc-19", "dkan-cluster-1-da-96", "dkan-cluster-1-da-20", "dkan-standalone-esxi"]
dcentries = [1140000, 1780000, 2800000, 560000]
ratio = 0.2  ### do 1% for testing

dcinfo = {}
i = 0
for dc in dclist:
    dcinfo[dc] = dcentries[i]
    i = i + 1
print dcinfo

datanum = 20000 # Superchunk of data. 10-100K is good. Must be multiple of datachunk.
datachunk = 400 # chunk of 200-1000 (x16 perfmetrics) is efficient
place = 0

print ' [***] Migrating data from MongoDB to Cassandra'
timebucket_list = []
ts = time.time()

# Make a file for each dc
#  write the timebucket_list to a file
for dcname in dclist:
    filen = 'tb_' + dcname + '.tb'
    """with open(filen, 'wb') as handle:
        pickle.dump(timebucket_list, handle)
    """
    f = open(filen, 'wb')
    pickle.dump(timebucket_list, f)
    f.close()

while place < (27000000 * ratio):
    subplist = []
    for dc in dclist:
        if place < dcinfo[dc]:
            if dc is dclist[2]:  # dc2 is big so have 2 workers
                print dc + ' : ' + str(place)
                subp = subprocess.Popen(["python", "mongo_cass_worker.py", chost, keyspace, dc, str(place*2), str(datanum), str(datachunk)], stdout=subprocess.PIPE)
                subplist.append(subp)
                subpp = subprocess.Popen(["python", "mongo_cass_worker.py", chost, keyspace, dc, str(place*2+datanum), str(datanum), str(datachunk)], stdout=subprocess.PIPE)
                subplist.append(subpp)
            else:
                print dc + ' : ' + str(place)
                subp = subprocess.Popen(["python", "mongo_cass_worker.py", chost, keyspace, dc, str(place), str(datanum), str(datachunk)], stdout=subprocess.PIPE)
                subplist.append(subp)
        else:
            print dc + ' : DONE'
        time.sleep(5)  # stagger the subprocesses 5s
    place = place + datanum
    print "Data points transfered: " + str(place)
    print time.time()
    ### Make list of subp wait for each. If time > 10min restart.
    exit_codes = [p.wait() for p in subplist]  # Might get stuck if a worker gets stuck
    print time.time()
    print "**********"
    time.sleep(30)  # take a break 30-600s

#cass.read_simple()
#cass.teardown()
print "*** DONE ***"
tf = time.time()
tt = tf-ts
print "Time to transfer:"
print tt
print str(tt/3600) + " hours"
print "*** DONE ***"

for dcname in dclist:
    filen = 'tb_' + dcname + '.tb'
    f = open(filen, 'rb')
    timebucket_list = pickle.loads(f.read())
    f.close()
    print dcname
    print timebucket_list

"""
filen = 'tb_' + dcname + '.tb'
with open(filen, 'wb') as handle:
    pickle.dump(timebucket_list, handle)
print timebucket_list
"""
