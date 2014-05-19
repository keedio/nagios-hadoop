#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80
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
