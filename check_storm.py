#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
# AUTHOR: Juan Carlos Fernandez <jcfernandez@redoop.org>

import stormStatus
import nagiosplugin
import argparse
import stringContext

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
	yield nagiosplugin.Metric('Nimbus connected', self.nimbus_connected, context = 'connected')
        for topology in self.topologies_to_check:
	    yield nagiosplugin.Metric('Topology %s connected' % topology, 
		self.topologies[topology]['connected'],
		context = 'connected')
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
	stringContext.StringContext('connected',
	    True,
	    fmt_metric='Connection status: {value}'),
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

