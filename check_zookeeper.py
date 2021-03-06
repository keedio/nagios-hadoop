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

import netcat
import stringContext
import re
import argparse
import nagiosplugin


def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Check zookeeper servers")
    parser.add_argument('-H','--hosts',action='store',required=True)
    parser.add_argument('-lw','--latency_warning',action='store',default=500)
    parser.add_argument('-lc','--latency_critical',action='store',default=1000)
    parser.add_argument('-v','--version', action='store', default="3.4.5")
    args = parser.parse_args()
    return args

class Zookeeper(nagiosplugin.Resource):
    @staticmethod
    def get_status(host,port):
        version=""
        latency=""
        mode=""
        status = netcat.netcat(host,int(port),'stat')
        for line in status.splitlines():
            m = re.match('^Zookeeper version:\s*(?P<VERSION>.+)',line)
            if m:
                version = m.group('VERSION')
            m = re.match('Mode:\s(?P<MODE>.+)',line)
            if m:
                mode = m.group('MODE')
            m = re.match('Latency min/avg/max:\s*\d+/(?P<AVG>\d+)/\d+',line)
            if m:
                latency=int(m.group('AVG'))
        return version,mode,latency

    def parse_status(self):
        self.status=dict()
        for entry in self.hosts:
            host,port=entry.split(':')
            self.status[host]=dict()
            self.status[host]['ok']=True if netcat.netcat(host,int(port),'ruok') == 'imok' else False
            self.status[host]['rw']=True if netcat.netcat(host,int(port),'isro') == 'rw' else False
            self.status[host]['version'],self.status[host]['mode'],self.status[host]['latency']=Zookeeper.get_status(host,port)

    def __init__(self,hosts):
       self.hosts=hosts.split(",") 
       self.parse_status()
     
    def probe(self):
        leader=0
        follower=0
        for host in self.hosts:
            host=host.split(':')[0]
            yield nagiosplugin.Metric('Running',self.status[host]['ok'],context="running")
            yield nagiosplugin.Metric('Writable',self.status[host]['rw'],context="writable")
            yield nagiosplugin.Metric('Version',self.status[host]['version'],context="version")
            yield nagiosplugin.Metric('Latency',self.status[host]['latency'],min=0,context="latency")
            if self.status[host]['mode'] == 'leader':
                leader+=1
            elif self.status[host]['mode'] == 'follower':
                follower+=1
        yield nagiosplugin.Metric('Mode','%d/%d' % (leader,follower), context="mode")

@nagiosplugin.guarded
def main():
    args = parser()
    check = nagiosplugin.Check(Zookeeper(args.hosts),
        stringContext.StringContext('running',
            True,
	    fmt_metric='ZK Server running {value}'),
        stringContext.StringContext('writable',
            True,
	    fmt_metric='Path writable {value}'),
        stringContext.StringContext('version',
            args.version,
	    fmt_metric='Runnng version is {value}'),
        nagiosplugin.ScalarContext('latency',
            args.latency_warning,
            args.latency_critical),
        stringContext.StringContext('mode',
            '1/%d' % (len(args.hosts.split(',')) - 1),
	    fmt_metric='leader/followers {value}'))
    check.main()

if __name__ == '__main__':
    main()
