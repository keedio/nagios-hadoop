#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80
import stormStatus

class StormArgs:
    nimbus_serv='jcr1.jcfernandez.cediant.es'
    nimbus_port=6627
    topology=None

storm=stormStatus.StormStatus(StormArgs()).topologies

def feeder(name):
    topology,component,bolt=name.split('-',2)
    for aux in storm[topology]['components'][component]['bolts']:
        if aux['id']==bolt:
            return aux['stats']['load']['600']*100

def metric_init(params):
    global descriptors
    descriptors=[]
    for topology_key,topology_val in storm.iteritems():
        for component_key,component_val in topology_val['components'].iteritems():
            for bolt in component_val['bolts']:
                name = topology_key+"-"+str(component_key)+"-"+str(bolt['id'])
            
                d = {'name': name,
                'call_back': feeder,
                'time_max': 90,
                'value_type': 'float',
                'units': '%',
                'slope': 'both',
                'format': '%f',
                'description': 'Bolt load 10m',
                'groups': 'Storm'}

                descriptors.append(d)

    return descriptors

def metric_cleanup():
    '''Clean up the metric module.'''
    pass

#This code is for debugging and unit testing
if __name__ == '__main__':
    metric_init(None)
    for d in descriptors:
        v = d['call_back'](d['name'])
        print 'value for %s is %f' % (d['name'],  v)
