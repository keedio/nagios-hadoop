#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80

import sys

#sys.path.append('gen-py')

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from storm import Nimbus
from storm.ttypes import *
from storm.constants import *



from utils import StringContext,krb_wrapper
import nagiosplugin
import argparse
import subprocess

def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Check zookeeper znodes existence and content")
    parser.add_argument('-n','--nimbus_serv',action='store',required=True)
    parser.add_argument('-p','--nimbus_port',action='store',type=int,default=6627)
    parser.add_argument('-t','--topology',action='store',default=None)
    parser.add_argument('--latency_warn',action='store',type=int)
    parser.add_argument('--latency_crit',action='store',type=int)
    parser.add_argument('--load_1d_warn',action='store',type=float,default=0.80)
    parser.add_argument('--load_3h_warn',action='store',type=float,default=0.90)
    parser.add_argument('--load_10m_warn',action='store',type=float,default=0.95)
    parser.add_argument('--load_1d_crit',action='store',type=float, default=0.95)
    parser.add_argument('--load_3h_crit',action='store',type=float, default=0.97)
    parser.add_argument('--load_10m_crit',action='store',type=float, default=0.99)
    args = parser.parse_args()
    return args

class StormTopology(nagiosplugin.Resource):
    def __init__(self,args):
        self.nimbus_serv=args.nimbus_serv
        self.nimbus_port=args.nimbus_port
        self.topology=args.topology
        self.topologies=dict()
        self.get_topologies()
        if self.topology is not None:
            self.topologies_to_check=[self.topology]
        else:
            self.topologies_to_check=self.topologies.keys()
        print self.topologies_to_check
        for topology in self.topologies_to_check:
            self.get_topology_status(topology)
        print self.topologies

    def get_topologies(self):
        try:
            socket      = TSocket.TSocket(self.nimbus_serv,self.nimbus_port)
            transport   = TTransport.TFramedTransport(socket)
            protocol    = TBinaryProtocol.TBinaryProtocol(transport)
            client      = Nimbus.Client(protocol)

            transport.open()

            summary = client.getClusterInfo()
            
            transport.close()
            for topology in summary.topologies:
                self.topologies[str(topology.name)]={'id':topology.id,'components':{}}

        except Thrift.TException, tx:
            print "%s" % (tx.message)

    def get_topology_status(self,topology):
        try:
            socket      = TSocket.TSocket(self.nimbus_serv,self.nimbus_port)
            transport   = TTransport.TFramedTransport(socket)
            protocol    = TBinaryProtocol.TBinaryProtocol(transport)
            client      = Nimbus.Client(protocol)

            transport.open()
            print topology
            executors=client.getTopologyInfo(self.topologies[topology]['id']).executors
            for executor in executors:
                component=executor.component_id
                self.topologies[topology]['components'].setdefault(component, {'bolts':[], 'spouts':[]})
                task_start=executor.executor_info.task_start
                task_end=executor.executor_info.task_end
                spout_stats=executor.stats.specific.spout
                bolt_stats=executor.stats.specific.bolt
                if bolt_stats:
                    self.topologies[topology]['components'][component]['bolts'].append({ 
                        'id':str(task_start) + '-' + str(task_end),
                        'stats' : self.boltToDict(bolt_stats)})
        except Thrift.TException, tx:
            print "%s" % (tx.message)

    def boltToDict(self,bolt_stats):
        out={'process_ms_avg':{},'executed':{},'execute_ms_avg':{},'acked':{},'failed':{},'load':{}}
        for period in [':all-time','86400','10800','600']:
            out['process_ms_avg'][period]=bolt_stats.process_ms_avg[period].values()[0] if bolt_stats.process_ms_avg[period] else 0
            out['executed'][period]=bolt_stats.executed[period].values()[0] if bolt_stats.executed[period] else 0
            out['execute_ms_avg'][period]=bolt_stats.execute_ms_avg[period].values()[0] if bolt_stats.execute_ms_avg[period] else 0
            out['failed'][period]=bolt_stats.failed[period].values()[0] if bolt_stats.failed[period] else 0
            out['acked'][period]=bolt_stats.acked[period].values()[0] if bolt_stats.acked[period] else 0
            out['load'][period]=out['process_ms_avg'][period]*out['executed'][period]/(int(period)*1000) if period != ":all-time" else 0
        return out

    def probe(self):
        
        for topology in self.topologies_to_check:
            for component_key, component_value in self.topologies[topology]['components'].iteritems():
                if component_value['bolts']:
                    for status in component_value['bolts']:
                        yield nagiosplugin.Metric('Load 1d %s-%s-%s' % (topology,component_key,status['id']),
                            status['stats']['load']['86400'],
                            context='1d load')
                        yield nagiosplugin.Metric('Load 3h %s-%s-%s' % (topology,component_key,status['id']),
                            status['stats']['load']['10800'],
                            context='3h load')
                        yield nagiosplugin.Metric('Load 10m %s-%s-%s' % (topology,component_key,status['id']),
                            status['stats']['load']['600'],
                            context='10m load')
        
@nagiosplugin.guarded
def main():
    timeout=10 # default
    args = parser()
    check = nagiosplugin.Check(StormTopology(args),
        nagiosplugin.ScalarContext('1d load',
            args.load_1d_warn,
            args.load_1d_crit),
        nagiosplugin.ScalarContext('3h load',
            args.load_3h_warn,
            args.load_3h_crit),
        nagiosplugin.ScalarContext('10m load',
            args.load_10m_warn,
            args.load_10m_crit))
    check.main(timeout=timeout)

if __name__ == '__main__':
    main()

