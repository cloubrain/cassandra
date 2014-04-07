cassandra
=========

Cassandra Interface  
  
Cloubrain optimizer integrated with OpenStack

How to Use
----------

python mongo_cass.py  


Control Plane
-------------

mongo_cass.py  
This initializes a datacenter using real data from OpenStack.  
Basic subcomponents:  cbmanager.py vmpm.py rabbitget.py  

cbmongo.py	
Pulls data from mongoDB in chunks.
cbm = cbmongo.CBmongo(dcname)  
vmdata = cbm.getData(dcname, place, place+datachunk)
  # returns datachunk data entries: vmdata is a dict sorted by vm

rabbitget.py   50 lines  
Gets latest data from RabbitMQ and returns it  
usage: rabbitget.getData()  

