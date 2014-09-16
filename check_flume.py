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
import requests
import json
import nagiosplugin
import time

html_auth = None

def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Check flume agent")
    parser.add_argument('-t','--test',action='store',type=str,required=True)
    parser.add_argument('--timeout_war',action='store',type=int,default=60,help="Seconds without receiving any msg")
    parser.add_argument('--timeout_crit',action='store',type=int,default=600,help="Seconds without receiving any msg")
    parser.add_argument('--url',action='store',type=str,required=True)
    parser.add_argument('--sources',nargs='*',type=str,action='store')
    parser.add_argument('--channels',nargs='*',type=str,action='store')
    parser.add_argument('--sinks',nargs='*',type=str,action='store')
    parser.add_argument('--batch_size',type=int,action='store',default=200)
    parser.add_argument('--field_split',type=str,help="String used to split fields to identify a metric",action='store',default=':')
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + version)
    args = parser.parse_args()
    emptys = 0
    if args.sources is None:
        emptys+=1
    if args.channels is None:
        emptys+=1
    if args.sinks is None:
        emptys+=1
    if args.test == "living" and emptys > 2:
        parser.error("At least one metric to check is required, --sources --channels and/or --sinks")
    if args.test == "in=out" and emptys > 1:
        parser.error("At least two metrics to check is required, --sources --channels and/or --sinks")
    return args

class Flume(nagiosplugin.Resource):

    def __init__(self,args):
        self.test = args.test
        self.url = args.url
        self.sources=args.sources
        self.channels=args.channels
        self.sinks=args.sinks
        self.field_split = args.field_split
        self.getMetrics()

    def getMetrics(self):
        response = requests.get(self.url)
        self.metrics = json.loads(response.content)
     
    def probe(self):
        if self.test == 'in=out':
            if self.sources is not None:
                sources = self.add_metrics(self.sources)
            if self.channels is not None:
                channels = self.add_metrics(self.channels)
            if self.sinks is not None:
                sinks = self.add_metrics(self.sinks)
            if sources is not None:
    	        if channels is not None:
                    yield nagiosplugin.Metric('Source=>Channel',sources-channels,context="io")
                else: 
                    yield nagiosplugin.Metric('Source=>Sink',sources-sinks,context="io")
            if channels is not None and sinks is not None:
                yield nagiosplugin.Metric('Channel=>Sink',channels-sinks,context="io")
        if self.test == 'living':
            if self.sources is not None:
                for metric in self.sources:
            	    yield nagiosplugin.Metric(metric + ' Last msg', int(self.walk_dict(metric)),context = "LastMsg")
            if self.channels is not None:
                for metric in self.channels:
            	    yield nagiosplugin.Metric(metric + ' Last msg', int(self.walk_dict(metric)),context = "LastMsg")
            if self.sinks is not None:
                for metric in self.sinks:
        	        yield nagiosplugin.Metric(metric + ' Last msg', int(self.walk_dict(metric)),context = "LastMsg")

    def add_metrics(self,list_metrics):
        adder = 0
        for metric in list_metrics:
            adder+=int(self.walk_dict)
        return adder
    
    def walk_dict(self,path):
        subJson = self.metrics
        for field in path.split(self.field_split):
            subJson = subJson.get(field)
        return subJson
	    
@nagiosplugin.guarded
def main():
    args = parser()
    check = nagiosplugin.Check(Flume(args),
        nagiosplugin.ScalarContext('io',
            args.batch_size,
            fmt_metric='{value} msg differs'),
        nagiosplugin.ScalarContext('LastMsg',
            str(int(time.time())*1000-args.timeout_war*1000)+':',
            str(int(time.time())*1000-args.timeout_crit*1000)+':',
            fmt_metric='Last msg recived in {value}'))
    check.main()

if __name__ == '__main__':
    main()
