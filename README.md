cassandra
=========

Cassandra Interface  
  

How to Use
----------

python mongo_cass.py  


Control Plane
-------------

mongo_cass.py  
Worker to manage the migration of data from mongoDB to Cassandra.  
Specify:  
-dcname: the are 5 choices with the number of total entries  
-datanum: ammount of data entries to migrate  
-datachunk: size of chunks (200-400 is good)  
-place: data entry to begin with  
Basic subcomponents:  cbmongo.py cbcassandra.py  
  
cbmongo.py	
Pulls data from mongoDB in chunks.  
cbm = cbmongo.CBmongo(dcname)  
vmdata = cbm.getData(dcname, place, place+datachunk)  
  # returns datachunk data entries: vmdata is a dict sorted by vm  
  
cbcassandra.py  
Python interface for Cassandra.  
-Specifies the schema  
-INSERT operators  
  
