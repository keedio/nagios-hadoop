#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80

try:
    import simplejson as json
    assert json
except ImportError:
    import json

from requests_kerberos import HTTPKerberosAuth
from utils import krb_wrapper
from collections import defaultdict
import time

import requests
import os
import logging

logging.basicConfig(filename='/var/log/ganglia.oozie.log',level=logging.DEBUG)

class OozieStatus:
    api_url={
        'list_coordinators':"/oozie/v2/jobs?jobtype=coordinator",
        'actions_from_coordinator':"/oozie/v2/job/%s?offset=%d&len=%d"
    }

    def __init__(self,params):
        self.host = params.get('host','localhost')
        self.port = params.get('port','8080')
        self.secure = params.get('secure',False)
        self.html_auth=None
        self.query_size = params.get('query_size',50)

        if self.secure:
            self.principal = params.get('principal')
            self.keytab = params.get('keytab')
            self.cache_file = params.get('cache_file','/tmp/oozie_gmond.cc')
            self.html_auth=HTTPKerberosAuth()
            auth_token = krb_wrapper(self.principal,self.keytab,self.cache_file)
            os.environ['KRB5CCNAME'] = self.cache_file
        self.coordinators = self.get_coordinators()
        if auth_token: auth_token.destroy()

    def get_coordinators(self):
        coordinators = {}
        try:
            url = "http://" + self.host + ":" + str(self.port) + self.api_url['list_coordinators']
            response = requests.get(url, auth=self.html_auth)
            if not response.ok:
                return {}
            for coordinator in json.loads(response.content)['coordinatorjobs']:
                coordinators[coordinator['coordJobId']]=self.get_actions(coordinator['coordJobId'])
        except:
            logging.error('http request error: "%s"' % url)
            return {}
        return coordinators

    def get_actions(self,coordinator):
        accumulator=dict()
        accumulator['total']=0
        try:
            url = "http://" + self.host + ":" + str(self.port) + self.api_url['actions_from_coordinator'] % (coordinator,0,0)
            response = requests.get(url, auth=self.html_auth)
            if not response.ok:
                return {}
            total_actions=json.loads(response.content)['total']

            url = "http://" + self.host + ":" + str(self.port) + self.api_url['actions_from_coordinator'] % (coordinator,total_actions-self.query_size,self.query_size)
            response = requests.get(url, auth=self.html_auth)
            if not response.ok:
                return {}

            actions = json.loads(response.content)['actions']

            for action in actions:
                created=time.mktime(self.time_conversion(action['createdTime']))
                modified=time.mktime(self.time_conversion(action['lastModifiedTime']))
                runtime=modified-created
                if accumulator.get(action['status']) is None:
                    accumulator[action['status']]=defaultdict(int)
                accumulator[action['status']]['count']+=1
                accumulator[action['status']]['runtime']+=runtime
                accumulator['total']+=1
        except:
            logging.error('http request error: "%s"' % url)
            return {} 
        return accumulator
        
    def time_conversion(self,time_str):
        return time.strptime(str(time_str),'%a, %d %b %Y %H:%M:%S %Z')
