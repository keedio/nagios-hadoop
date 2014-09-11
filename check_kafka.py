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

import stringContext
import nagiosplugin
import argparse
import subprocess
import re

def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Check zookeeper znodes existence and content")
    parser.add_argument('-H','--hosts',action='store',required=True)
    parser.add_argument('-K','--kafka_list_bin',action='store',default='/usr/lib/kafka/bin/kafka-list-topic.sh')
    parser.add_argument('-T','--topic',action='store',required=True)
    args = parser.parse_args()
    return args

class KafkaTopics(nagiosplugin.Resource):
    def parse_topics(self,test):
        response = subprocess.Popen([self.kafka_list_bin,'--zookeeper', self.zkserver,test,'--topic', self.topic],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output,err = response.communicate()
	topics=""
	for line in output.splitlines():
	    m = re.match('\s*topic:\s+(?P<TOPIC>\w+)\s+partition:\s*(?P<PARTITION>\d+).*',line)
	    if m:
	        topics+=m.group('PARTITION') + " "
        return topics,err

    def __init__(self,args):
        self.zkserver = args.hosts
        self.kafka_list_bin = args.kafka_list_bin
	if args.topic:
	    self.topic=args.topic
        self.get_status()

    def get_status(self):
        self.under_replicated=self.parse_topics('--under-replicated-partitions')[0]
        self.unavailable=self.parse_topics('--unavailable-partitions')[0]

    def probe(self):
        yield nagiosplugin.Metric('under_replicated',self.under_replicated if self.under_replicated != "" else None,context="Under Replication")
        yield nagiosplugin.Metric('unavailable',self.unavailable if self.unavailable !="" else None,context="Unavailability")

@nagiosplugin.guarded
def main():
    timeout=30 # default
    args = parser()
    check = nagiosplugin.Check(KafkaTopics(args),
        stringContext.StringContext('Under Replication',None,fmt_metric='{value} partitions are under replicated'),
        stringContext.StringContext('Unavailability',None ,fmt_metric='{value} are unavailables' )	)
    check.main()
    #if args.secure: auth_token.destroy()

if __name__ == '__main__':
    main()
