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

import argparse
import nagiosplugin
import ooziestatus

def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Checks oozie")
    parser.add_argument('-p', '--principal', action='store', dest='principal')
    parser.add_argument('-s', '--secure',action='store_true')
    parser.add_argument('-k', '--keytab',action='store')
    parser.add_argument('--cache_file',action='store', default='/tmp/nagios.krb')
    parser.add_argument('-H','--host',action='store',default='localhost')
    parser.add_argument('-P','--port',action='store',type=int,default=14000)
    parser.add_argument('--coordinators',nargs="+", action='store')
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + version)
    parser.add_argument('-c','--critical', action='store', type=int, default=10)
    parser.add_argument('-w','--warning', action='store', type=int, default=10)
    parser.add_argument('--log_file',action='store',default='nrpe/')
    parser.add_argument('--query_size',action='store',type=int,default=5)
    args = parser.parse_args()
    if args.secure and (args.principal is None or args.keytab is None):
        parser.error("if secure cluster, both of --principal and --keytab required")
    return args

class Oozie(nagiosplugin.Resource):

    def __init__(self,args):
        params={}
        params['host'] = args.host
        params['port'] = args.port
        if args.principal:
            params['principal'] = args.principal
        if args.keytab:
            params['keytab'] = args.keytab
	params['cache_file'] = args.cache_file
	params['log_file'] = args.log_file
        params['secure'] = args.secure
        params['query_size'] = args.query_size
        self.oozie_status=ooziestatus.OozieStatus(params)
        if args.coordinators is not None:
            self.coordinators=args.coordinators
        else:
            self.coordinators=self.oozie_status.coordinators.keys()
 
    def probe(self):
	yield nagiosplugin.Metric('Running OozieServers', len(self.coordinators), context="coordinators")
        for coord in self.coordinators:
            total = self.oozie_status.coordinators[coord]['total']
            if self.oozie_status.coordinators[coord].get('SUCCEEDED'):
                success =  float(self.oozie_status.coordinators[coord].get('SUCCEEDED').get('count')*100/total)
            else:
                success = 0.0
            if self.oozie_status.coordinators[coord].get('WAITING'):
                waiting =  float(self.oozie_status.coordinators[coord].get('WAITING').get('count')*100/total)
            else:
                waiting = 0.0
            fails = 100.0 - success - waiting
            yield nagiosplugin.Metric('Fails ' + coord, fails ,context="errors") # - success - waiting,context="errors")

@nagiosplugin.guarded
def main():
    html_auth = None
    args = parser()
    check = nagiosplugin.Check(Oozie(args),
        nagiosplugin.ScalarContext('errors',
            args.warning,
            args.critical),
	nagiosplugin.ScalarContext('coordinators',
	    '1:',
	    '1:'))
    check.main()

if __name__ == '__main__':
    main()
