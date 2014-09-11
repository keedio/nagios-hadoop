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
import kerberosWrapper
import stringContext
import os
import argparse
import requests
import nagiosplugin
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
    parser.add_argument('--admin',action='store', default='hdfs')
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
        response = requests.get("http://" + self.namenode + ':' + str(self.port) + "/webhdfs/v1" + self.path + "?user.name=" + self.admin + "&op=" + op['status'] , auth=self.html_auth)
        if response.ok:
            self.filestatus = ast.literal_eval(response.content)['FileStatus']

    def checkWritable(self):
        response = requests.put("http://" + self.namenode + ':' + str(self.port) + "/webhdfs/v1" + self.path + "?user.name=hdfs&op=" + op['create'] , auth=self.html_auth,allow_redirects=False)
        response2=requests.put(response.headers['location'],headers={'content-type':'application/octet-stream'},auth=html_auth,data="TEST_DATA")
        self.filestatus['writable']=response2.ok
        if response2.ok:
            requests.delete("http://"+ self.namenode + ':' + str(self.port) + "/webhdfs/v1" + self.path + "?user.name=" + self.admin +"&op=" + op['delete'], auth=self.html_auth)


    def __init__(self,args):
        self.html_auth = None
        if args.secure:
            self.html_auth=HTTPKerberosAuth()
            auth_token = kerberosWrapper.krb_wrapper(args.principal,args.keytab,args.cache_file)
            os.environ['KRB5CCNAME'] = args.cache_file
        self.namenode=args.namenode
        self.port=args.port
        self.path=args.path
        self.type=args.type
        self.owner=args.owner
        self.group=args.group
        self.permission=args.permission
        self.writable=args.writable
	self.admin=args.admin
        self.filescanner()
        if self.writable:
           self.checkWritable()
        if args.secure and auth_token: auth_token.destroy()
     
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
        stringContext.StringContext('type',
            args.type,
            fmt_metric=' is a {value}'),
        stringContext.StringContext('owner',
            args.owner,
            fmt_metric=' owner is {value}'),
        stringContext.StringContext('group',
            args.group,
            fmt_metric=' group is {value}'),
        stringContext.StringContext('permission',
            args.permission,
            fmt_metric=' has {value} permission'),
        stringContext.StringContext('writable',
            True,
            fmt_metric='HTTPFS writable: {value}'))     
    check.main()

if __name__ == '__main__':
    main()
