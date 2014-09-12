# -*- coding: utf-8 -*-
from cassandra.cluster import Cluster
from copy import deepcopy

__author__ = 'horia margarit'


class Cassandra(object):

    def __init__(self, hostnames, keyspace=None):
        if not isinstance(hostnames, (list, tuple)):
            hostnames = [hostnames]
        if not ((keyspace is None) or isinstance(keyspace, basestring)):
            keyspace = repr(keyspace)
        self.__hostnames = deepcopy(hostnames)
        self.__keyspace = deepcopy(keyspace)

    def __enter__(self):
        cass = Cluster(self.__hostnames)
        self.__sess = cass.connect(self.__keyspace)
        return self.__sess

    def __exit__(self, exc_type, exc_value, traceback):
        self.__sess.shutdown()
        del self.__sess
        if not ((exc_type is None) and (exc_value is None) and (traceback is None)):
            raise exc_type(exc_value)
        return True


if __name__ == '__main__':
    hostnames = ("23.253.55.224", )
    table = "vmwarchive20"
    with Cassandra(hostnames, keyspace=table) as database:
        query = "DESCRIBE TABLE datasimple"
        result = database.execute(query)
        print(result)

