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

from requests_kerberos import HTTPKerberosAuth
from utils import krb_wrapper,StringContext
from nagiosplugin.state import Ok, Warn, Critical
import os
import argparse
import requests
import re
import subprocess
import nagiosplugin
import logging
import ast
import socket

def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Check some yarn pieces like rm, or scheduler")
    parser.add_argument('-p', '--principal', action='store', dest='principal')
    parser.add_argument('-s', '--secure',action='store_true')
    parser.add_argument('-k', '--keytab',action='store')
    parser.add_argument('--cache_file',action='store', default='/tmp/nagios.krb')
    parser.add_argument('--historyserver',action='store',default='localhost')
    parser.add_argument('--hs_port',action='store',type=int,default='19888')
    parser.add_argument('--alert',action='store',default='critical')
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + version)
    args = parser.parse_args()
    if args.secure and (args.principal is None or args.keytab is None):
        parser.error("if secure cluster, both of --principal and --keytab required")
    return args

class Historyserver(nagiosplugin.Resource):
    def status(self):
        try:
            response = requests.get("http://" + self.historyserver + ":" + str(self.hs_port) + "/ws/v1/history/info" , auth = self.html_auth)
        except:
            response = None
        if response is not None and response.ok:
            self.hs_status = ast.literal_eval(response.content)['historyInfo']
        else:
            self.hs_status['startedOn'] = -1

    def __init__(self,args):
        self.html_auth = None
        if args.secure:
            self.html_auth=HTTPKerberosAuth()
            auth_token = krb_wrapper(args.principal,args.keytab,args.cache_file)
            os.environ['KRB5CCNAME'] = args.cache_file
        if args.historyserver == 'localhost':
            self.historyserver = socket.getfqdn()
        else:
            self.historyserver = args.historyserver
        self.hs_port = args.hs_port
        self.hs_status = dict()
        self.status()
        if args.secure and auth_token: auth_token.destroy() 

    def probe(self):
        yield nagiosplugin.Metric('History server',self.hs_status['startedOn'] >= 0 , context="hs_status")

@nagiosplugin.guarded
def main():
    args = parser()
    check = nagiosplugin.Check(Historyserver(args),
        StringContext('hs_status',
            True))
    check.main()
        

if __name__ == '__main__':
    main()
