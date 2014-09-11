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

import nagiosplugin

class StringContext(nagiosplugin.Context):
    def __init__(self, name,value,level="critical",fmt_metric=None, result_cls=nagiosplugin.Result):
        super(StringContext, self).__init__(name, fmt_metric, result_cls)
        self.value=value
        self.level=level

    def evaluate(self, metric, resource):
        if self.value == metric.value:
            return self.result_cls(nagiosplugin.Ok,hint=metric.description,metric=metric)
        else:
            if self.level == "critical":
                return self.result_cls(nagiosplugin.Critical,hint=metric.description,metric=metric)
            else:
                return self.result_cls(nagiosplugin.Warn,hint=metric.description,metric=metric)
