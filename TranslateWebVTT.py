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
import datetime

class TranslateWebVTT(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-M2'
        dependencies = ['webvtt-py']
        description = """Parse text from Web VTT """
        tags = ["vtt", "webvtt", "python",  "parsing", "text", "text extract", "video"]

    def __init__(self, **kwargs):
        super().__init__()

    def transform(self, context, flowfile):
        import webvtt
        import requests
        import io
        from io import StringIO

        vttFile = flowfile.getContentsAsBytes().decode()

        buffer = StringIO(str(vttFile))

        outputBuffer = io.StringIO()

        for caption in webvtt.read_buffer(buffer):
            # print(caption.start)
            # print(caption.end)
            outputBuffer.write(caption.text)

        result = outputBuffer.getvalue()

        return FlowFileTransformResult(relationship = "success", contents=result)
