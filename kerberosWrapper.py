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

import krbV
import os

class krb_wrapper():
    def __init__(self,principal,keytab,ccache_file=None):
        self.context = krbV.default_context()
        self.principal = krbV.Principal(name=principal, context=self.context)
        self.keytab = krbV.Keytab(name=keytab, context=self.context)
        self.ccache_file = ccache_file
        if ccache_file:
            self.ccache_file = ccache_file
            self.ccache = krbV.CCache(name="FILE:" + self.ccache_file, context=self.context, primary_principal=self.principal)
        else:
            self.ccache = self.context.default_ccache(primary_principal=self.principal)
        self.ccache.init(self.principal)
        self.ccache.init_creds_keytab(keytab=self.keytab,principal=self.principal)

    def destroy(self):
        if self.ccache_file:
            os.system('kdestroy -c %s 2>/dev/null' % self.ccache_file)
        else:
            os.system('kdestroy 2>/dev/null')

    def reload(self):
        self.ccache.init(self.principal)
        self.ccache.init_creds_keytab(keytab=self.keytab,principal=self.principal)
