#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80
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

def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Check some yarn pieces like rm, or scheduler")
    parser.add_argument('-p', '--principal', action='store', dest='principal')
    parser.add_argument('-s', '--secure',action='store_true')
    parser.add_argument('-k', '--keytab',action='store')
    parser.add_argument('--cache_file',action='store', default='/tmp/nagios.krb')
    parser.add_argument('--rm',action='store',default='localhost')
    parser.add_argument('--port',action='store',type=int,default=8088)
    parser.add_argument('--alert',action='store',default='critical')
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + version)
    parser.add_argument('--lost_warn',action='store',default=1)
    parser.add_argument('--lost_crit',action='store',default=2)
    parser.add_argument('--unhealthy_warn',action='store',default=1)
    parser.add_argument('--unhealthy_crit',action='store',default=2)
    parser.add_argument('--rebooted_warn',action='store',default=1)
    parser.add_argument('--rebooted_crit',action='store',default=2)
    parser.add_argument('--apps_warn',action='store',default=100)
    parser.add_argument('--apps_crit',action='store',default=500)
    args = parser.parse_args()
    if args.secure and (args.principal is None or args.keytab is None):
        parser.error("if secure cluster, both of --principal and --keytab required")
    return args

class Resourcemanager(nagiosplugin.Resource):
    def status(self):
        response = requests.get("http://" + self.rm + ':' + str(self.port) + "/ws/v1/cluster" , auth=self.html_auth)
        if response.ok:
            self.clusterinfo = ast.literal_eval(response.content)['clusterInfo']
        else:
            self.clusterinfo['state']="ERROR"

        response = requests.get("http://" + self.rm + ':' + str(self.port) + "/ws/v1/cluster/metrics" , auth=self.html_auth)
        if response.ok:
            self.clustermetrics = ast.literal_eval(response.content)['clusterMetrics']
        else:
            self.clustermetrics['unhealthyNodes']=0  
            self.clustermetrics['lostNodes']=0
            self.clustermetrics['rebootedNodes']=0
            self.clustermetrics['appsPending']=0
        response = requests.get("http://" + self.rm + ':' + str(self.port) + "/ws/v1/cluster/nodes" , auth=self.html_auth)
        if response.ok:
            self.clusternodes=ast.literal_eval(response.content)['nodes']['node']
        # It is possible to request /schedulers but I didn't find any useful information for alerts

    def __init__(self,args):
	self.html_auth = None
        if args.secure:
            self.html_auth=HTTPKerberosAuth()
            auth_token = krb_wrapper(args.principal,args.keytab,args.cache_file)
	    os.environ['KRB5CCNAME'] = args.cache_file
        self.rm=args.rm
        self.port=args.port
        
        self.clusterinfo=dict()
        self.clustermetrics=dict()
        self.clusternodes=[]
        self.status()
	if auth_token: auth_token.destroy()
     
    def probe(self):
        yield nagiosplugin.Metric('RM Status',self.clusterinfo['state'],context="state")
        yield nagiosplugin.Metric('Unhealthy Nodes',self.clustermetrics['unhealthyNodes'],context="unhealthy")
        yield nagiosplugin.Metric('Lost Nodes',self.clustermetrics['lostNodes'],context="lost")
        yield nagiosplugin.Metric('Rebooted Nodes',self.clustermetrics['rebootedNodes'],context="rebooted")
        yield nagiosplugin.Metric('Apps Pending',self.clustermetrics['appsPending'],context="appsPending")
        for node in self.clusternodes:
            yield nagiosplugin.Metric(node['nodeHostName'],node['state'],context="nodeState")

@nagiosplugin.guarded
def main():
    args = parser()
    check = nagiosplugin.Check(Resourcemanager(args),
        StringContext('state',
            'STARTED'),
        StringContext('nodeState',
            'RUNNING'),
        nagiosplugin.ScalarContext('unhealthy',
            args.unhealthy_warn,
            args.unhealthy_crit),
        nagiosplugin.ScalarContext('lost',
            args.lost_warn,
            args.lost_crit),
        nagiosplugin.ScalarContext('rebooted',
            args.rebooted_warn,
            args.rebooted_crit),
        nagiosplugin.ScalarContext('appsPending',
            args.apps_warn,
            args.apps_crit))
    check.main()

if __name__ == '__main__':
    main()
