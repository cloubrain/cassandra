#####################################################################
# cbmongo.py
# cbmongo class gets data from mongoDB on our server
#
# TYPE: python cbmongo.py
#  Example code at bottom
#
# Copyright (C) 2014 by Cloubrain, Inc.
# Pete C. Perlegos
#
######################################################################

import os
import sys
import time
import pymongo, copy
#import matplotlib.pyplot as plt
import cPickle as pickle

class CBmongo():
	def __init__(self, mydc=''):
		self.myhost = "datacollector.cloubrain.com"  # m1.large instance, 200GB mongoDB
		self.DCname = "dkan-cluster-1-dc-19"   #DC name for which you want to extract. Might have multiple clusters.
		self.DCname = "dkan-cluster-1-da-96"
		self.DCname = "dkan-cluster-1-da-20"
		self.DCname = mydc
		print "***************"
		print "myhost = " + self.myhost
		print "***************"
		self.dbname = "vmware2"
		#self.dbname = "openstack"
		self.statscollname = "stats_new"

	def testdb(self):
		self.getStats()
		print "*** Print all cluster names ***"
		clusters = self.getClusters('')
		print clusters
		print "***************"

		vmdata = {}  # vmdata sorted by clusterName: dict of dicts
		clusters = clusters[3:]      # select clusters you want
		for cl in clusters:  # get all VM data
			#print "* Getting data:", cl
			vmdata[cl] = self.getData(cl, 0, 1000)
		print "*** WAIT then print all VM data ***"
		time.sleep(5)
		for clp in vmdata.keys(): # print all VM data
			print "*** NO PRINTING:", clp
			self.printData(vmdata[clp])
		print "***************"
		print "Done!"
		print "***************"

	def getStats(self):
		print "*** DB Stats ***"
		dbcon = pymongo.Connection(self.myhost)	        # opens connection to the DB
		print "* Available Databases:"
		mydbs = dbcon.database_names()
		print mydbs
		print "***************"
		print "DB: " + self.dbname
		dbpointer = dbcon[self.dbname]		        # DB pointer
		statresp = dbpointer.command({'dbstats': 1})
		#print statresp
		dbcon.close()  # Need to close DBconnection or it gets killed
		print "DB Storage Size in GBs:", float(statresp["storageSize"])/(1024*1024*1024)
		print "DB File Size in GBs:", float(statresp["fileSize"])/(1024*1024*1024)
		print "***************"

	def getClusters(self, DCname):
		dbcon = pymongo.Connection(self.myhost)	        # opens connection to the DB
		dbpointer = dbcon[self.dbname]		        # DB pointer
		clusters = dbpointer.vm.distinct('DCname')
		dbcon.close()  # Need to close DBconnection or it gets killed
		return clusters

	def getData(self, DCname, start=0, end=999999999999):
		# Returns dict with all data from given cluster sorted by VM
		#  DCname: cluster you want data from
		#  start: place in time to go back (0=now, 1000=back in time)
		#  end: number of data points you want: (end-start)/#VMs = timeSteps/VM
		# Each time entry is a dict: perfMetrics returns a list of all the metrics
		vms = {}
		dbcon = pymongo.Connection(self.myhost)	        # opens connection to the DB
		dbpointer = dbcon[self.dbname]		        # DB pointer
		dbstatscoll = dbpointer[self.statscollname]	# selecting the relevant collection
		vmdb = dbpointer["vm"]
		print "*** Get VMs in DCcluster (" + DCname + ")"
		for vm in vmdb.find({"DCname" : DCname}):
			vms[vm["name"].strip()] = []			#storing all VMs ID into this dict

		numvms = len(vms)
		print "* # VMs in cluster:", numvms

		perfkeysdb = dbpointer["perfCounterIdMap"]
		perfkeys = {}
		for pk in perfkeysdb.find({"DCname": DCname}):
			perfkeys[pk["k"]] = pk["v"]    #storing (mapping) all performance metrics' keys with their name here
		
		print "* Extracting Data from DB: Takes time to transfer *"
		for c in dbstatscoll.find({"DCname":DCname}).sort([("time_current", -1)]).skip(start).limit(end):
			r = c
			perfMetrics = c["perfMetrics"]
			for x2 in perfMetrics:
				try:
				 	x2["perfkey"] = perfkeys[x2["k"]]
				except:
					pass
                        vms[c["name"]].append(copy.deepcopy(c))
		print "* COMPLETE: got data from DBcluster", DCname, "***"
		dbcon.close()  # Need to close DBconnection or it gets killed
		return vms

	def printData(self, vms):   # Good example for pulling data from new getData
		print "***************"
		print "*** Print VMs ***"   # Sorted by VM
		vmcount = 0   # VM counter
		for vm in vms:
			vmcount += 1    # vmcount increment for each VM
			print vm
			stcount = 0    # timestamp counter
			for st in vms[vm]:
				stcount += 1
				tm = st["time_current"]
				perf = st["perfMetrics"]
				print "* time: " + str(tm) + " :: " + str(perf)
			print "******"
			print "* data history count:", stcount
			print "***************"
		print "VMcount: " , vmcount
		print "***************"

	def saveData(self, ts, filen):   # Get data and save to file
		# ts: number of timesteps
		# filen: save file name
		vms = self.getData(self.DCname, 0, 21*ts)    # multiply ts by num VMs in cluster
		#filen = 'vmsdata.txt'
		with open(filen, 'wb') as handle:
			pickle.dump(vms, handle)
		print "Saved data to file:" + filen

	def loadData(self, filen):   # Load data from file
		# filen: name of file where you saved your dict of vm data
		# Returns vms from getData
		with open(filen, 'rb') as handle:
			vmsd = pickle.loads(handle.read())
		return vmsd

	def getPerfs(self, ts, filen, perfs):   # Get data
		# Returns: dict of VMs each of which contains: dict of lists of data points: time, perfName1, perfName2, ...
		#  vmsdata[vmName][perfName]: list of datapoints
		# ts: number of timesteps
		# perfs: list of requests perfMetrics by index: you always get 'time'
		# 0: disk.commandsAborted.summation
		# 1: mem.swapout.average 
		# 2: disk.busResets.summation 
		# 3: mem.swapped.average 
		# 4: mem.consumed.average 
		# 5: cpu.ready.summation
		# 6: mem.swapin.average 
		# 7: mem.overhead.average 
		# 8: mem.usage.average
		# 9: net.transmitted.average 
		# 10: net.usage.average 
		# 11: net.received.average
		# 12: cpu.usagemhz.average 
		# 13: mem.active.average 
		# 14: disk.write.average 
		# 15: disk.read.average
		print "***************"
		print "*** Get data ***"   # Sorted by VM
		#vms = self.getData(self.DCname, 0, 21*ts)    # multiply ts by num VMs in cluster
		vms = cbdb.loadData(filen)   # ts is not used when 
		vmsdata = {}
		vmcount = 0   # VM counter
		for vm in vms:
			vmcount += 1    # vmcount increment for each VM
			data = []
			for st in vms[vm]:   # get timestamps
				tm = st["time_current"]
				data.append(int(float(tm)))
			data.reverse()
			vmdata = {}
			vmdata['time'] = data
			for i in perfs:    # get every requested perfMetric
				data = []
				for st in vms[vm]:
					perf = st["perfMetrics"]
					pk = perf[i]    # you can get any perf metric like this
					v = pk['v']    # type is unicode
					v = int(float(v))
					while v > 100:  # Scale down for now
						v = v / 5
					data.append(v)
				data.reverse()
				perfName = str(pk['perfkey'])  # perfMetric called by perfkey
				vmdata[perfName] = data
				#print vm
				#print data
			vmsdata[vm] = vmdata
		print "VMcount: " , vmcount
		return vmsdata

	def getCount(self, DCname):
		# Returns the collection count(): entries for all DCs in DB
		dbcon = pymongo.Connection(self.myhost)	        # opens connection to the DB
		dbpointer = dbcon[self.dbname]		        # DB pointer
		dbstatscoll = dbpointer[self.statscollname]	# selecting the relevant collection
		print "* Getting count() for DB: " + self.dbname
		count = dbstatscoll.count()
		print count
		print "* Getting count() for DC: " + DCname
		count = dbstatscoll.find({"DCname":DCname}).count()  # Takes too long 137M records in collection
		print count
		dbcon.close()  # Need to close DBconnection or it gets killed


"""
# TEST CODE
myDC = "dkan-cluster-1-dc-19"  # 21VMs
#myDC = "dkan-cluster-1-da-96"  # 30VMs
#myDC = "dkan-cluster-1-da-20"  # 46VMs
#myDC = "dkan-standalone-esxi"  # 18VMs
#myDC = "dkan-cluster-2-DC"  # 11VMs
#myDC = "devstackR"
cbdb = CBmongo(myDC)  #init CBmongo
#cbdb.testdb()  # tests functions
cbdb.getStats()
cbdb.getCount(myDC)

#print cbdb.getClusters('')

#vms = cbdb.getData(myDC, 0, 10)
#print vms
#print "********"
#cbdb.printData(vms)
"""

