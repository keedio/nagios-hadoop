nagios-hadoop
=============

This repository contains a collection of nagios plugins to monitor Hadoop ecosystem.

PRE-REQUISITES
==============
    yum install python-krbV
    yum install python-requests
    yum install python-requests-kerberos
    yum install python-argparse
    yum install python-storm
    yum install python-pip
    easy_install thrift
    eay_install nagios-plugin-elasticsearch

To check storm is required python clases from sources

    wget https://bitbucket.org/pypa/setuptools/raw/0.7.8/ez_setup.py -O - | python
    wget https://pypi.python.org/packages/source/n/nagiosplugin/nagiosplugin-1.2.1.tar.gz#md5=d81c724525e8e8b290d17046109e71d2
    python bootstrap.py
    bin/buildout
