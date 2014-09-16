# -*- coding: utf-8 -*-
from cassandra.query import SimpleStatement
from cassandra.cluster import PagedResult
from cassandra.cluster import Cluster
from blist import blist, sorteddict
from collections import defaultdict
from operator import itemgetter
from copy import deepcopy
from numpy import zeros

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


class DataSimple(Cloubrain):
    __FETCH_DATA_QUERY = "SELECT vm_name, measurement_time, perf_name, perf_data FROM {0} WHERE dc_name = {1}"
    __SCHEMA_DISCOVERY = "SELECT {0} FROM {1} WHERE partition = 0"
    __TABLE = "datasimple"

    def __init__(self, database, cluster_names=None, guest_names=None, page_size=1024, perf_names=None):
        if not ((cluster_names is None) or (guest_names is None) or (perf_names is None)):
            self._cluster_index = dict((name, idx) for idx, name in enumerate(cluster_names))
            self._guest_indices = dict((idx, dict((name, index) for index, name in enumerate(names))) for idx, names in enumerate(guest_names))
            self._perf_indices = dict((idx, dict((name, index) for index, name in enumerate(names))) for idx, names in enumerate(perf_names))
        else:
            raise NotImplementedError
        super(DataSimple, self).__init__(database, page_size=page_size)

    def _build_data_fetch_query(self, cluster, start=None, stop=None):
        query = DataSimple.__FETCH_DATA_QUERY.format(DataSimple.__TABLE, cluster)
        if start is not None:
            query += " AND time_bucket >= {0}".format(start.strftime("%Y%m%d%H"))
        if stop is not None:
            query += " AND time_bucket < {0}".format(stop.strftime("%Y%m%d%H"))
        return query

    def __discover_schema(self, attribute, cluster=None):
        uniques = set()
        if attribute in ("perf_name", "vm_name", ) and cluster is not None:
            query = (DataSimple.__SCHEMA_DISCOVERY + " AND dc_name = {2}").format(attribute, DataSimple.__TABLE, cluster)
            statement = SimpleStatement(query, fetch_size=self._pg_size)
        elif attribute in ("dc_name", ) and cluster is None:
            query = DataSimple.__SCHEMA_DISCOVERY.format(attribute, DataSimple.__TABLE)
            statement = SimpleStatement(query, fetch_size=self._pg_size)
        else:
            statement = None
        if statement is not None:
            names = self._db.execute(statement)
            for row in names:
                uniques.add(itemgetter(0)(row))
        return blist(uniques)

    def add_to_schema(self, cluster, guests):
        raise NotImplementedError

    def drop_from_schema(self, cluster, guests):
        raise NotImplementedError


class Cloubrain(object):

    def __init__(self, database, page_size=1024):
        self._pg_size = page_size if isinstance(page_size, int) else 1024
        self._db = database

    def perf_data(self, cluster, start=None, stop=None):
        """
            Invariant: perf data is already pre-sorted on measurement time in ascending order!
        """
        key = self._cluster_index[cluster]
        host_index = self._host_indices[key]
        perf_index = self._perf_indices[key]
        n_hosts = len(host_index)
        n_perfs = len(perf_index)
        results = zeros((n_hosts, n_perfs, self._pg_size), dtype=float)
        query = self._build_data_fetch_query(cluster, start=start, stop=stop)
        statement = SimpleStatement(query, fetch_size=self._pg_size)
        for idx, row in enumerate(self._db.execute(statement)):
            results[host_index[row.vm_name], perf_index[row.perf_name], idx] = float(row.perf_data)
            if ((idx + 1) % self._pg_size) == 0:
                yield results
                results = zeros((n_hosts, n_perfs, self._pg_size), dtype=float)
        if ((idx + 1) % self._pg_size) != 0:
            yield results[:, :, :idx + 1]


if __name__ == '__main__':
    from datetime import datetime

    hostnames = ("23.253.55.224", )
    keyspace = "vmwarchive20"
    table = "datasimple"
    with Cassandra(hostnames, keyspace=keyspace) as database:
        query = "DESCRIBE TABLE {0}".format(table)
        result = database.execute(query)
        print(result)

    cluster = "dkan-cluster-1-da-20"
    start = datetime(2014, 1, 22, hour=3)
    stop = datetime(2014, 1, 22, hour=4)
    with Cassandra(hostnames, keyspace=keyspace) as database:
        driver = DataSimple(database)
        for data in driver.perf_data(cluster, start=start, stop=stop):
            print(data)

