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
import kerberosWrapper
import nagiosplugin
import argparse
import subprocess,os,re
import ast
from kazoo.client import KazooClient
import kazoo

def parser():
    version="0.1"
    parser = argparse.ArgumentParser(description="Check zookeeper znodes existence and content")
    parser.add_argument('-H','--hosts',action='store',required=True)
    parser.add_argument('-s','--secure',action='store_true')
    parser.add_argument('-p','--principal',action='store')
    parser.add_argument('-k','--keytab',action='store')
    parser.add_argument('-c','--cache_file',action='store',default='/tmp/nagios_zookeeper')
    parser.add_argument('-t','--test',action='store',required=True)
    parser.add_argument('-T','--topic',action='store')
    parser.add_argument('--hdfs_cluster_name',action='store')
    parser.add_argument('--check_topics',action='store_true')
    parser.add_argument('--warn_topics',action='store',default='1:50')
    parser.add_argument('--crit_topics',action='store',default='0:100')
    parser.add_argument('--warn_hosts',action='store',default='2:')
    parser.add_argument('--crit_hosts',action='store',default='1:')
    parser.add_argument('-z','--zk_client',action='store',default='/usr/bin/zookeeper-client')
    args = parser.parse_args()
    if args.secure and (args.principal is None or args.keytab is None):
        parser.error('If secure cluster, both of --principal and --keytab required')
    if args.test == 'hdfs' and args.hdfs_cluster_name is None:
        parser.error('If checking hdfs --hdfs_cluster_ name is required')
    return args

class ZookeeperZnode(nagiosplugin.Resource):
    def call_zk(self,cmd,url):
        print self.zk_client,'-server', self.zkserver, cmd, url
        response = subprocess.Popen([self.zk_client,'-server', self.zkserver, cmd, url],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output,err = response.communicate()
        return output.splitlines()[-1],err

    def __init__(self,args):
        if args.secure:
            auth_token = kerberosWrapper.krb_wrapper(args.principal, args.keytab,args.cache_file)
            os.environ['KRB5CCNAME'] = args.cache_file
        self.zkserver = args.hosts
        self.zk_client = args.zk_client
        self.test = args.test
        self.hdfs_cluster_name=args.hdfs_cluster_name
        self.tests = {'hdfs' : self.check_hdfs,
                'hbase' : self.check_hbase,
                'kafka' : self.check_kafka
        }
        self.check_topics=args.check_topics
        self.topic=args.topic
	if args.secure and auth_token: auth_token.destroy()
        self.zk = KazooClient(hosts=self.zkserver)
        self.zk.start()
 

    def probe(self):
        return self.tests[self.test]()
    
    #def znode_exist(self,path):
    #    result=zk.exists(path)
    #    zk.stop()
    #    return result
    #    #output,err = self.call_zk('stat',path)
    #    #if err is None or re.match('^Node does not exist:\s*%s' % path,err) is None:
    #    #    return True
    #    #else:
    #    #    return False

    def check_hdfs(self):
        return [nagiosplugin.Metric('ActiveNN lock', self.zk.exists('/hadoop-ha/' + self.hdfs_cluster_name + '/ActiveStandbyElectorLock') is not None,context='true')]

    def check_hbase(self):
        return [ nagiosplugin.Metric('Hbase Master', self.zk.exists('/hbase/master') is not None, context='true'),
            nagiosplugin.Metric('Failover', self.zk.exists('/hbase/backup-masters') is not None, context='true'),
            nagiosplugin.Metric('Metaregion server', self.zk.exists('/hbase/meta-region-server') is not None, context='true'),
            nagiosplugin.Metric('Unassigned', self.zk.exists('/hbase/unassigned') is None, context='false')]

    def check_kafka(self):
        metrics=[]
        self.topics,self.kafka_servers=self.get_kafka_state()
        metrics.append(nagiosplugin.Metric('Total topics',len(self.topics.keys()),context='total_topics'))
        metrics.append(nagiosplugin.Metric('Hosts',len(self.kafka_servers.keys()),context='hosts'))
        if self.check_topics:
            for k_topic,v_topic in self.topics.iteritems():
                for k_partition,v_partition in v_topic['partitions'].iteritems():
                    all_sync=True
                    for host in v_partition:
                        all_sync = False if not host in v_topic['isr'][k_partition] else all_sync
                    metrics.append(nagiosplugin.Metric('Topic %s partition %s in sync' % (k_topic, k_partition),all_sync,context='true'))
        return metrics

    def get_kafka_state(self):
        ids_parsed=dict()
        topics_parsed=dict()
        #output,err=self.call_zk('ls','/brokers/ids')
        #output,err=self.call_zk('ls','/')
        #if err is None or err is "":
        #    ids=ast.literal_eval(output)
        ids = self.zk.get_children('/brokers/ids')
        for host in ids:
            ids_parsed[str(host)]=ast.literal_eval(self.zk.get('/brokers/ids/%d' % int(host))[0])
        if self.zk.exists('/brokers/topics') is not None:
            if self.topic is not None:
                topics=[self.topic]
            else:
                topics=self.zk.get_children('/brokers/topics')
            for topic in topics:
                topics_parsed[str(topic)]=dict()
                if self.check_topics:
                    aux=self.zk.get('/brokers/topics/%s' % str(topic))[0]
                    topic_stat=ast.literal_eval(aux)
                    topics_parsed[str(topic)]['partitions']=topic_stat['partitions']
                    topics_parsed[str(topic)]['isr']=dict()
                    for partition in topics_parsed[topic]['partitions'].keys():
                        output=ast.literal_eval(self.zk.get('/brokers/topics/%s/partitions/%s/state' % (topic,partition))[0])
                        topics_parsed[str(topic)]['isr'][partition]=output['isr']
        return topics_parsed,ids_parsed
        
@nagiosplugin.guarded
def main():
    timeout=10 # default
    args = parser()
    check = nagiosplugin.Check(ZookeeperZnode(args),
        stringContext.StringContext('true',True),
        stringContext.StringContext('false',False),
        nagiosplugin.ScalarContext('total_topics',args.warn_topics,args.crit_topics),
        nagiosplugin.ScalarContext('hosts',args.warn_hosts,args.crit_hosts))
    if args.test == "kafka":
        timeout=0
    check.main(timeout=timeout)

if __name__ == '__main__':
    main()
