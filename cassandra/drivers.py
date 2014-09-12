# -*- coding: utf-8 -*-
from cassandra.query import SimpleStatement
from cassandra.cluster import PagedResult
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


class Cloubrain(object):

    def __init__(self, database, table, page_size=1000):
        self.__pg_size = page_size
        self.__table = table
        self.__db = database

    def vm_names(self, cluster):
        unique_names = set()
        query = "SELECT vm_name FROM {0} WHERE dc_name = {1} AND partition = 0"
        statement = SimpleStatement(query.format(self.__table, cluster), fetch_size=self.__pg_size)
        names = self.__db.execute(statement)
        if isinstance(names, PagedResult):
            for row in names:
                unique_names.add(row.vm_name.encode(encoding='utf-8'))
        else:
            unique_names.update(map(lambda r: r.vm_name.encode(encoding='utf-8'), names))
        return list(unique_names)


if __name__ == '__main__':
    hostnames = ("23.253.55.224", )
    keyspace = "vmwarchive20"
    table = "datasimple"
    with Cassandra(hostnames, keyspace=keyspace) as database:
        query = "DESCRIBE TABLE {0}".format(table)
        result = database.execute(query)
        print(result)

    cluster = "dkan-cluster-1-da-20"
    with Cassandra(hostnames, keyspace=keyspace) as database:
        driver = Cloubrain(database, table)
        print(driver.vm_names(cluster))

