#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80
from utils import krb_wrapper,StringContext
import os
import argparse
import nagiosplugin
import re
import subprocess

html_auth = None

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

    def __init__(self):
        p = subprocess.Popen(['hbase','hbck'],stdout=subprocess.PIPE,stderr=None)
        output,err = p.communicate()
        self.status=None
        if err is None:
            for line in output.splitlines():
                m = re.match('^\s*Status\s*:\s*(?P<STATUS>\w+)\s*',line)
                if m:
                    self.status=m.group('STATUS')
        else:
            return 2,"Critical: "+err
     
    def probe(self):
        yield nagiosplugin.Metric('status',self.status,context="status")

@nagiosplugin.guarded
def main():
    args = parser()
    if args.secure:
        auth_token = krb_wrapper(args.principal,args.keytab,args.cache_file)
        os.environ['KRB5CCNAME'] = args.cache_file
    check = nagiosplugin.Check(Hbase(),
        StringContext('status',
            'OK'))
    check.main()
    if auth_token: auth_token.destroy() 

if __name__ == '__main__':
    main()
