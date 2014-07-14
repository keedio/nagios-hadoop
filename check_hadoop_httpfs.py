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

html_auth = None
op={'create':'CREATE&overwrite=False',
    'status':'GETFILESTATUS',
    'delete':'DELETE&recursive=False',
    'put':'CREATE&overwrite=False',
    'read':'OPEN&offset=0&length=1024',
    'ls':'LISTSTATUS'}

def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Checks datanode")
    parser.add_argument('-p', '--principal', action='store', dest='principal')
    parser.add_argument('-s', '--secure',action='store_true')
    parser.add_argument('-k', '--keytab',action='store')
    parser.add_argument('--cache_file',action='store', default='/tmp/nagios.krb')
    parser.add_argument('--namenode',action='store',default='localhost')
    parser.add_argument('--port',action='store',type=int,default=14000)
    parser.add_argument('--path',action='store',required=True)
    parser.add_argument('--type',action='store')
    parser.add_argument('--owner',action='store')
    parser.add_argument('--group',action='store')
    parser.add_argument('--permission',action='store',type=str)
    parser.add_argument('--writable',action='store_true')
    parser.add_argument('--alert',action='store',default='critical')
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + version)
    args = parser.parse_args()
    if args.secure and (args.principal is None or args.keytab is None):
        parser.error("if secure cluster, both of --principal and --keytab required")
    if args.type is None and args.owner is None and args.group is None and args.permission is None and not args.writable:
        parser.error("At least one check is required")
    return args

class Httpfs(nagiosplugin.Resource):
    """
    Return dict structure with the format
    {
        'pathSuffix':VALUE<<string>>,
        'type':DIRECTORY|FILE,
        'length':bytes<<int>>,
        'owner':OWNER<<String>>,
        'group':GROUP<<String>>,
        'permission':UNIX_NUMERIC<<int>>,
        'accessTime':timestamp<<long>>,
        'modificationTime':timestamp<<long>>,
        'blockSize':bytes<<int>>,
        'replication':FACTOR<<int>>
    }
    """
    def filescanner(self):
        self.filestatus=dict()
        print "http://" + self.namenode + ':' + str(self.port) + "/webhdfs/v1" + self.path + "?op=" + op['status']
        response = requests.get("http://" + self.namenode + ':' + str(self.port) + "/webhdfs/v1" + self.path + "?op=" + op['status'] , auth=self.html_auth)
        if response.ok:
            self.filestatus = ast.literal_eval(response.content)['FileStatus']

    def checkWritable(self):
        response = requests.put("http://" + self.namenode + ':' + str(50070) + "/webhdfs/v1" + self.path + "?op=" + op['create'] , auth=self.html_auth,allow_redirects=False)
        response2=requests.put(response.headers['location'],auth=html_auth,data="TEST_DATA")
        self.filestatus['writable']=response2.ok
        if response2.ok:
            requests.delete("http://"+ self.namenode + ':' + str(50070) + "/webhdfs/v1" + self.path + "?op=" + op['delete'], auth=self.html_auth)


    def __init__(self,args):
        self.html_auth = None
        if args.secure:
            self.html_auth=HTTPKerberosAuth()
            auth_token = krb_wrapper(args.principal,args.keytab,args.cache_file)
            os.environ['KRB5CCNAME'] = args.cache_file
        self.namenode=args.namenode
        self.port=args.port
        self.path=args.path
        self.filescanner()
        self.type=args.type
        self.owner=args.owner
        self.group=args.group
        self.permission=args.permission
        self.writable=args.writable
        if self.writable:
           self.checkWritable()
        if auth_token: auth_token.destroy()
     
    def probe(self):
        if self.type is not None:
            yield nagiosplugin.Metric('type',self.filestatus['type'],context="type")
        if self.owner is not None:
            yield nagiosplugin.Metric('owner',self.filestatus['owner'],context="owner")
        if self.group is not None:
            yield nagiosplugin.Metric('group',self.filestatus['group'],context="group")
        if self.permission is not None:
            yield nagiosplugin.Metric('permission',self.filestatus['permission'],context="permission")
        if self.writable:
            yield nagiosplugin.Metric('writable',self.filestatus['writable'],context="writable")

@nagiosplugin.guarded
def main():
    args = parser()
    check = nagiosplugin.Check(Httpfs(args),
        StringContext('type',
            args.type,
            fmt_metric=' is a {value}'),
        StringContext('owner',
            args.owner,
            fmt_metric=' owner is {value}'),
        StringContext('group',
            args.group,
            fmt_metric=' group is {value}'),
        StringContext('permission',
            args.permission,
            fmt_metric=' has {value} permission'),
        StringContext('writable',
            True,
            fmt_metric='HTTPFS writable: {value}'))     
    check.main()
    if auth_token: auth_token.destroy() 

if __name__ == '__main__':
    main()
