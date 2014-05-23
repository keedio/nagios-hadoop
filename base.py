#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80
from requests_kerberos import HTTPKerberosAuth
from utils import krb_wrapper
import os
import argparse
import requests
import nagiosplugin

html_auth = None

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
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + version)
    args = parser.parse_args()
    if args.secure and (args.principal is None or args.keytab is None):
        parser.error("if secure cluster, both of --principal and --keytab required")
    return args

class Httpfs(nagiosplugin.Resource):

    def __init__(self,html_auth,args):
     
    def probe(self):
        if self.type is not None:
            yield nagiosplugin.Metric('type',self.filestatus['type'],context="type")

@nagiosplugin.guarded
def main():
    args = parser()
    if args.secure:
        html_auth=HTTPKerberosAuth()
        auth_token = krb_wrapper(args.principal,args.keytab,args.cache_file)
        os.environ['KRB5CCNAME'] = args.cache_file
    check = nagiosplugin.Check(Httpfs(html_auth,args),
        StringContext('type',
            args.type,
            fmt_metric=' is a {value}'))
    check.main()
    if auth_token: auth_token.destroy() 

if __name__ == '__main__':
    main()
