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

# https://github.com/tspannhw/FLaNK-Halifax
# https://github.com/MobilityData/gtfs-realtime-bindings/blob/master/python/README.md

class GetGTFSFeed(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-M1'
        dependencies = ['gtfs-realtime-bindings']
        description = """Get a GTFS Feed as JSON """
        tags = ["gtfs", "url", "protobuf",  "protobuf", "json", "transit"]

    GTFS_URL = PropertyDescriptor(
        name="URL for GTFS Feed",
        description="Read GTFS from Feed and Convert to JSON",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [
        GTFS_URL
    ]

    def __init__(self, **kwargs):
        super().__init__()
        self.property_descriptors.append(self.GTFS_URL)

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):
        from google.transit import gtfs_realtime_pb2
        import urllib
        import urllib.request
        from google.transit import gtfs_realtime_pb2
        from google.protobuf.json_format import MessageToDict
        from google.protobuf.json_format import MessageToJson
		# from collections import OrderedDict

		# https://gtfs.halifax.ca/realtime/Vehicle/VehiclePositions.pb
        gtfsurl = context.getProperty(self.GTFS_URL).evaluateAttributeExpressions(flowfile).getValue()

        attributes = {"gtfsurl": gtfsurl}

        if (gtfsurl != None):
            feed = gtfs_realtime_pb2.FeedMessage()
            response = urllib.request.urlopen(gtfsurl)
            feed.ParseFromString(response.read())
            json_obj = MessageToJson(feed)

        return FlowFileTransformResult(relationship = "success", contents=json_obj, attributes=attributes)
