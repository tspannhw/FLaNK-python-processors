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
import datetime
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope

# sometimes feeds like MTA Subway contain all 3 types of GTFS data: alerts, vehicle positions and trip updates

TRIP_UPDATE = "trip_update"
VEHICLE = "vehicle" 
ALERT = "alert"

#### Class

class GetGTFSCompoundFeed(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-M2'
        dependencies = ['gtfs-realtime-bindings']
        description = """Get a GTFS Feed as JSON with 1 or more types """
        tags = ["gtfs", "url", "protobuf",  "protobuf", "json", "transit"]

    GTFS_URL = PropertyDescriptor(
        name="URL for GTFS Feed",
        description="Read GTFS from Feed and Convert to JSON from one or more types",
        required=True,
        sensitive=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    HEADER_NAME = PropertyDescriptor(
        name="API Key for header name ex: (MTA)",
        description="API Key for header name",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    API_KEY = PropertyDescriptor(
        name="API Key for header (MTA)",
        description="API Key for header",
        required=False,
        sensitive=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    GTFS_TYPE = PropertyDescriptor(
        name="Type for GTFS Feed",
        description="Read GTFS from Feed and Convert to JSON from TripUpdate, Vehicle or Alert",
        allowable_values=[TRIP_UPDATE, VEHICLE, ALERT],
        required=True,
        default_value=TRIP_UPDATE
    )

    property_descriptors = [
        GTFS_URL,
        API_KEY,
        HEADER_NAME,
        GTFS_TYPE
    ]

    def __init__(self, **kwargs):
        super().__init__()
        self.property_descriptors.append(self.GTFS_URL)
        self.property_descriptors.append(self.API_KEY)
        self.property_descriptors.append(self.HEADER_NAME)
        self.property_descriptors.append(self.GTFS_TYPE)

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):
        from google.transit import gtfs_realtime_pb2
        import urllib
        import urllib.request
        from google.transit import gtfs_realtime_pb2
        from google.protobuf.json_format import MessageToDict
        from google.protobuf.json_format import MessageToJson

		# https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr
        gtfsurl = context.getProperty(self.GTFS_URL).evaluateAttributeExpressions(flowfile).getValue()
        apikey = context.getProperty(self.API_KEY).evaluateAttributeExpressions(flowfile).getValue()
        headername = context.getProperty(self.HEADER_NAME).evaluateAttributeExpressions(flowfile).getValue()

        attributes = {"gtfsurl": gtfsurl}
        apiheaderpresent = False
        json_obj = ""

        if (apikey != None and headername != None):
            apiheaderpresent = True

        if (gtfsurl != None):
            feed = gtfs_realtime_pb2.FeedMessage()
            headers = {} 
            if ( apiheaderpresent ):
                headers = {headername: apikey}

            gtfs_type = context.getProperty(self.GTFS_TYPE).evaluateAttributeExpressions(flowfile).getValue()
        
            try:
                transitrequest = urllib.request.Request(url=gtfsurl, headers=headers)
                response = urllib.request.urlopen(transitrequest)
                feed.ParseFromString(response.read())

                if gtfs_type == TRIP_UPDATE:
                    trip_updates = gtfs_realtime_pb2.FeedMessage()

                if gtfs_type == VEHICLE:
                    vehicles = gtfs_realtime_pb2.FeedMessage()

                if gtfs_type == ALERT:
                    alerts = gtfs_realtime_pb2.FeedMessage()
                
                json_obj = MessageToJson(feed)

                if gtfs_type == TRIP_UPDATE:
                    trip_updates.header.CopyFrom(feed.header)

                if gtfs_type == VEHICLE:
                    vehicles.header.CopyFrom(feed.header)

                if gtfs_type == ALERT:
                    alerts.header.CopyFrom(feed.header)

                for feedentity in feed.entity:
                    if feedentity.HasField('is_deleted') and feedentity.is_deleted == True:
                        if gtfs_type == TRIP_UPDATE:
                            e = trip_updates.entity.add()
                            e.CopyFrom(feedentity)
                        if gtfs_type == VEHICLE:                        
                            e = vehicles.entity.add()
                            e.CopyFrom(feedentity)
                        if gtfs_type == ALERT:
                            e = alerts.entity.add()
                            e.CopyFrom(feedentity)

                    elif feedentity.HasField('trip_update'):
                        if gtfs_type == TRIP_UPDATE:
                            e = trip_updates.entity.add()
                            e.CopyFrom(feedentity)

                    elif feedentity.HasField('vehicle'):
                        if gtfs_type == VEHICLE:   
                            e = vehicles.entity.add()
                            e.CopyFrom(feedentity)

                    elif feedentity.HasField('alert'):
                        if gtfs_type == ALERT:
                            e = alerts.entity.add()
                            e.CopyFrom(feedentity)

                if gtfs_type == TRIP_UPDATE:
                    json_obj = MessageToJson(trip_updates)

                if gtfs_type == VEHICLE:
                    json_obj = MessageToJson(vehicles)

                if gtfs_type == ALERT:
                    json_obj = MessageToJson(alerts)
                #json_obj = str( jsontu + "\n" + jsonvehicles + "\n" + jsonalerts + "\n")

            except Exception as ex:
                print(ex)

        return FlowFileTransformResult(relationship = "success", contents=json_obj, attributes={"gtfsurl": gtfsurl,"gtfstype": gtfs_type})
