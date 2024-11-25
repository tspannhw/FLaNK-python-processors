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

### https://openstreetmap.org/copyright
### “OpenStreetMap” a link to openstreetmap.org/copyright, 
### which has information about OpenStreetMap’s data sources 
### (which OpenStreetMap needs to credit)
### as well as the ODbL.
### https://medium.com/@gopesh3652/geocoding-with-python-using-nominatim-a-beginners-guide-220b250ca48d
### pip3 install geopy
### Address to Latitude and Longitude
class AddressToLatLong(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-M2'
        dependencies = ['geopy' ]
        description = """OpenStreamMaps ODBL openstreetmap.org/copyright Nominatim for parsing addresses and getting lat/long"""
        tags = ["locations","osm",  "latitude", "longitude", "NLP", "Nominatim", "addresses", "lat/long", "ai", "OpenStreamMaps", "geopy", "text"]

    PARSE_TEXT = PropertyDescriptor(
        name="Parse Text",
        description="Specifies the text to extract latitude and longitude for addresse",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [
        PARSE_TEXT
    ]

    def __init__(self, **kwargs):
        super().__init__()
        self.property_descriptors.append(self.PARSE_TEXT)

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):
        from geopy.geocoders import Nominatim

        # Instantiate a new Nominatim client
        app = Nominatim(user_agent="nifi-AddressToLatLong-nominatim")

        parse_text = context.getProperty(self.PARSE_TEXT).evaluateAttributeExpressions(flowfile).getValue()

        location = app.geocode(str(parse_text)).raw

        latitude = ""
        longitude = ""
        license = ""
        osm_id = ""
        place_id = ""
        osm_class = ""
        osm_type = ""
        place_rank = ""
        addresstype = ""
        osm_name = ""
        display_name = ""
        boundingbox = ""
        osm_importance = ""

        try:
            latitude = str(location['lat'])
            longitude = str(location['lon'])
            license = str(location['licence'])
            osm_id = str(location['osm_id'])
            place_id = str(location['place_id'])
            osm_class = str(location['class'])
            osm_type = str(location['type'])
            place_rank = str(location['place_rank'])
            osm_importance = str(location['importance'])
            addresstype = str(location['addresstype'])
            osm_name = str(location['name'])
            display_name = str(location['display_name'])
            boundingbox = str(location['boundingbox'])
        except Exception as ex:
            print(ex)

        attributes = { "latitude": latitude, "longitude": longitude,
                       "osmlicense": license,  "osmid": osm_id,
                       "place_id": place_id, "osmclass": osm_class,
                       "osmtype": osm_type, "placerank": place_rank,
                       "osmimportance": osm_importance, "addresstype": addresstype,
                       "osmname": osm_name, "displayname": display_name, "boundingbox": boundingbox }
        return FlowFileTransformResult(relationship = "success", contents=str(location), attributes=attributes)        
