#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80

import sys

import stormStatus

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

class StormTopology(stormStatus.StormStatus,nagiosplugin.Resource):
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

