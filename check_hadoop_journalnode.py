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
import argparse
import requests
import re
import nagiosplugin
import ast
import os
import sys


def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Checks datanode")
    parser.add_argument('-p', '--principal', action='store', dest='principal')
    parser.add_argument('-s', '--secure',action='store_true')
    parser.add_argument('-k', '--keytab',action='store')
    parser.add_argument('--cache_file',action='store', default='/tmp/nagios.krb')
    parser.add_argument('--qjm',action='store',default='localhost')
    parser.add_argument('--process_warn',action='store',type=int,default=1000)
    parser.add_argument('--process_crit',action='store',type=int,default=5000)
    parser.add_argument('--sync_threshold_warn',action='store',type=int,default=50)
    parser.add_argument('--sync_threshold_crit',action='store',type=int,default=250)
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + version)
    args = parser.parse_args()
    if args.secure and (args.principal is None or args.keytab is None):
        parser.error("if secure cluster, both of --principal and --keytab required")
    return args

class Journalnode():
    def getValues(self):
        self.values={'LastWrittenTxId':-1,'RpcProcessingTimeAvgTime':-1}
        try:
            response = requests.get("http://" + self.journalnode + ":" + str(self.port) + "/jmx", auth=self.html_auth)
            if response.ok:
                for line in response.content.splitlines():
                    m = re.match('\s*"(?P<FIELD>(LastWrittenTxId)|(RpcProcessingTimeAvgTime))"\s*:\s*(?P<VAL>\d+)',line)
                    if m:
                        self.values[m.group('FIELD')]=int(m.group('VAL'))
            else:
                self.error_msg=self.journalnode + ":" +str(response.status_code)
        except Exception as ext:
            self.error_msg=self.journalnode + ":" + str(ext)

    def __init__(self,html_auth,journal,port):
        self.html_auth = html_auth
        self.error_msg = 'OK'
        self.journalnode=journal
        self.port=port
        self.getValues()

class QJM(nagiosplugin.Resource):
    def __init__(self,args):
        html_auth = None
        if args.secure:
            html_auth=HTTPKerberosAuth()
            auth_token = krb_wrapper(args.principal,args.keytab,args.cache_file)
            os.environ['KRB5CCNAME'] = args.cache_file
        self.qjm=[{'host':journal.split(':')[0], 'journalState':Journalnode(html_auth,journal.split(':')[0],journal.split(':')[1])} for journal in args.qjm.split(',')]
        if args.secure and auth_token: auth_token.destroy() 

    def probe(self):
        minTxId=sys.maxint
        maxTxId=-sys.maxint-1
        quorumNodes=0
        for journal in self.qjm:
            yield nagiosplugin.Metric('%s Connection status ' % journal.get('host'),journal.get('journalState').error_msg,context='connection')
            if journal.get('journalState').error_msg == "OK":
                quorumNodes+=1
                yield nagiosplugin.Metric('%s AVG Processing Time' % journal.get('host'),journal.get('journalState').values['RpcProcessingTimeAvgTime'],context="processing")
                txId=journal.get('journalState').values['LastWrittenTxId']
                if txId > maxTxId:
                    maxTxId = txId
                if txId < minTxId:
                    minTxId = txId
        yield nagiosplugin.Metric('Sync',maxTxId-minTxId,context="sync")
        yield nagiosplugin.Metric('Available quorum nodes',quorumNodes,context="quorum")


@nagiosplugin.guarded
def main():
    args = parser()
    check = nagiosplugin.Check(QJM(args),
        nagiosplugin.ScalarContext('processing',
            args.process_warn,
            args.process_crit),
        nagiosplugin.ScalarContext('sync',
            args.sync_threshold_warn,
            args.sync_threshold_crit),
        StringContext('connection',
            "OK"),
        nagiosplugin.ScalarContext('quorum',
            nagiosplugin.Range("%s:" % str(len(args.qjm.split(','))/2)),
            nagiosplugin.Range("%s:" % str(len(args.qjm.split(','))/2))))
    check.main()

if __name__ == '__main__':
    main()
