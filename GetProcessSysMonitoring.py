# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import re
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope

### NLP
class GetProcessSysMonitoring(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-SNAPSHOT'
        dependencies = ['psutil']
        description = """ Cross-platform process and system monitoring """
        tags = ["psutil", "process", "system", "system monitoring", "process monitoring", "server", "system utilization"]

    def __init__(self, **kwargs):
        super().__init__()


    def transform(self, context, flowfile):
        import psutil
        import json

        usage = psutil.disk_usage("/")
        users = json.dumps(psutil.users())
        pids = json.dumps(psutil.pids())
        netio = json.dumps(psutil.net_io_counters(pernic=True))
        netaddr = json.dumps(psutil.net_if_addrs())
        diskusage = str("{:.1f} MB".format(float(usage.free) / 1024 / 1024))
        cpu = str(psutil.cpu_percent(interval=1))
        swapmemory = str(psutil.swap_memory())
        memory = str(psutil.virtual_memory().percent)

        attributes = {"memory": memory, 
            "diskusage": diskusage,
            "cpu": cpu,
            "swapmemory": swapmemory,
            "users": users,
            "pids": pids,
            "netio": netio,
            "netaddr": netaddr
        }

        return FlowFileTransformResult(relationship = "success", contents=flowfile, attributes=attributes)
