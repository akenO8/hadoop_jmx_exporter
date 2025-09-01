#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import time
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY

from cmd import utils
from cmd.trino_coordinator import TrinoCoordinatorMetricCollector
from cmd.utils import get_module_logger
from cmd.hdfs_namenode import NameNodeMetricCollector
from cmd.hdfs_datanode import DataNodeMetricCollector
from cmd.hdfs_journalnode import JournalNodeMetricCollector
from cmd.yarn_resourcemanager import ResourceManagerMetricCollector
from cmd.hive_server import HiveServerMetricCollector
from cmd.yarn_nodemanager import NodeManagerMetricCollector
from cmd.trino_worker import TrinoWorkerMetricCollector

logger = get_module_logger(__name__)


def register_prometheus(cluster, args):
    if args.nns is not None and len(args.nns) > 0:
        nnc = NameNodeMetricCollector(cluster, args.nns)
        nnc.collect()
        REGISTRY.register(nnc)
        REGISTRY.register(DataNodeMetricCollector(cluster, nnc))
    if args.rms is not None and len(args.rms) > 0:
        rmc = ResourceManagerMetricCollector(cluster, args.rms, args.queue)
        rmc.collect()
        REGISTRY.register(rmc)
        REGISTRY.register(NodeManagerMetricCollector(cluster, rmc))
    if args.jns is not None and len(args.jns) > 0:
        REGISTRY.register(JournalNodeMetricCollector(cluster, args.jns))
    if args.hss is not None and len(args.hss) > 0:
        REGISTRY.register(HiveServerMetricCollector(cluster, args.hss))
    if args.tcs is not None and len(args.tcs) > 0:
        REGISTRY.register(TrinoCoordinatorMetricCollector(cluster, args.tcs))
    if args.tws is not None and len(args.tws) > 0:
        REGISTRY.register(TrinoWorkerMetricCollector(cluster, args.tws))


def main():
    args = utils.parse_args()
    host = args.host
    port = int(args.port)
    start_http_server(port, host)
    print "Listen at %s:%s" % (host, port)
    register_prometheus(args.cluster, args)
    while True:
        print("sleep 300s")
        time.sleep(300)


if __name__ == "__main__":
    main()
