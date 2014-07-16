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

from utils import krb_wrapper,StringContext
import os
import argparse
import nagiosplugin
import re
import subprocess


def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Checks datanode")
    parser.add_argument('-p', '--principal', action='store', dest='principal')
    parser.add_argument('-s', '--secure',action='store_true')
    parser.add_argument('-k', '--keytab',action='store')
    parser.add_argument('--cache_file',action='store', default='/tmp/nagios.krb')
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + version)
    args = parser.parse_args()
    if args.secure and (args.principal is None or args.keytab is None):
        parser.error("if secure cluster, both of --principal and --keytab required")
    return args

class Hbase(nagiosplugin.Resource):

    def __init__(self,args):
        if args.secure:
            auth_token = krb_wrapper(args.principal,args.keytab,args.cache_file)
            os.environ['KRB5CCNAME'] = args.cache_file
 
        p = subprocess.Popen(['hbase','hbck'],stdout=subprocess.PIPE,stderr=None)
        try:
            output,err = p.communicate(timeout=30)
            self.status=None
            if err is None:
                for line in output.splitlines():
                    m = re.match('^\s*Status\s*:\s*(?P<STATUS>\w+)\s*',line)
                    if m:
                        self.status=m.group('STATUS')
            else:
                return 2,"Critical: "+err
        except subprocess.TimeoutExpired:
            p.kill()
            self.status="Timeout"
        if auth_token: auth_token.destroy()
     
    def probe(self):
        yield nagiosplugin.Metric('status',self.status,context="status")

@nagiosplugin.guarded
def main():
    args = parser()
    check = nagiosplugin.Check(Hbase(args),
        StringContext('status',
            'OK'))
    check.main()

if __name__ == '__main__':
    main()
