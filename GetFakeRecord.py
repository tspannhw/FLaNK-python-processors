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
import math
import base64
import time
import datetime
from time import gmtime, strftime
import random, string
import uuid
import logging
from time import sleep
import sys
import os
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope

### Fake Records
class GetFakeRecord(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-M1'
        dependencies = ['faker']
        description = """ Generate Faker data, synthetic data, test data, fake data """
        tags = ["generate", "testdata", "faker", "fake data"]

    USER_ID = PropertyDescriptor(
        name="Include UUID",
        description="User ID",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    CREATED_DT = PropertyDescriptor(
        name="Include CREATED_DT",
        description="CREATED Date",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    EMAIL = PropertyDescriptor(
        name="Include EMAIL",
        description="Email",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    IP = PropertyDescriptor(
        name="Include IP V4",
        description="IP Address",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    USER_NAME = PropertyDescriptor(
        name="Include USER_NAME",
        description="User Name",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    CLUSTER_NAME = PropertyDescriptor(
        name="Include CLUSTER_NAME",
        description="Cluster Name",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    CITY = PropertyDescriptor(
        name="Include CITY",
        description="City",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    COUNTRY = PropertyDescriptor(
        name="Include COUNTRY",
        description="Country",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    POSTCODE = PropertyDescriptor(
        name="Include POSTCODE",
        description="Postal Code / Zip Code",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    STREET_ADDRESS = PropertyDescriptor(
        name="Include STREET_ADDRESS",
        description="Street Address",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    LICENSE_PLATE = PropertyDescriptor(
        name="Include LICENSE_PLATE",
        description="License Plate",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    EAN13 = PropertyDescriptor(
        name="Include EAN13",
        description="EAN13",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    CATCH_PHRASE = PropertyDescriptor(
        name="Include CATCH_PHRASE",
        description="CATCH_PHRASE",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    COMMENT = PropertyDescriptor(
        name="Include COMMENT",
        description="COMMENT",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )


    COMPANY = PropertyDescriptor(
        name="Include COMPANY",
        description="Company Name",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    LATITUDE = PropertyDescriptor(
        name="Include LATITUDE",
        description="Latitude",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    LONGITUDE = PropertyDescriptor(
        name="Include LONGITUDE",
        description="Longitude",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    JOB = PropertyDescriptor(
        name="Include JOB",
        description="Job",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    MD5 = PropertyDescriptor(
        name="Include MD5",
        description="MD5",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    PASSWORD = PropertyDescriptor(
        name="Include PASSWORD",
        description="Password",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    FIRST_NAME = PropertyDescriptor(
        name="Include FIRST_NAME",
        description="First Name",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    LAST_NAME = PropertyDescriptor(
        name="Include LAST_NAME",
        description="Last Name",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )        

    PHONE_NUMBER = PropertyDescriptor(
        name="Include PHONE_NUMBER",
        description="Phone Number",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    USER_AGENT = PropertyDescriptor(
        name="Include USER_AGENT",
        description="User Agent",
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    property_descriptors = [ 
        USER_ID, 
        CREATED_DT, 
        EMAIL, 
        IP, 
        USER_NAME,
        CLUSTER_NAME,
        CITY,
        COUNTRY,
        POSTCODE, 
        STREET_ADDRESS,
        LICENSE_PLATE,
        EAN13,
        CATCH_PHRASE,
        COMMENT,
        COMPANY,
        LATITUDE, 
        LONGITUDE,
        JOB,
        MD5,
        PASSWORD,
        FIRST_NAME,
        LAST_NAME,
        PHONE_NUMBER,
        USER_AGENT 
    ]

    def __init__(self, **kwargs):
        super().__init__()
        self.property_descriptors.append(self.USER_ID)
        self.property_descriptors.append(self.CREATED_DT)
        self.property_descriptors.append(self.EMAIL)
        self.property_descriptors.append(self.IP)
        self.property_descriptors.append(self.USER_NAME)
        self.property_descriptors.append(self.CLUSTER_NAME)
        self.property_descriptors.append(self.CITY)
        self.property_descriptors.append(self.COUNTRY)
        self.property_descriptors.append(self.POSTCODE) 
        self.property_descriptors.append(self.STREET_ADDRESS)
        self.property_descriptors.append(self.LICENSE_PLATE)
        self.property_descriptors.append(self.EAN13)
        self.property_descriptors.append(self.CATCH_PHRASE)
        self.property_descriptors.append(self.COMMENT)
        self.property_descriptors.append(self.COMPANY)
        self.property_descriptors.append(self.LATITUDE) 
        self.property_descriptors.append(self.LONGITUDE)
        self.property_descriptors.append(self.JOB)
        self.property_descriptors.append(self.MD5)
        self.property_descriptors.append(self.PASSWORD)
        self.property_descriptors.append(self.FIRST_NAME)
        self.property_descriptors.append(self.LAST_NAME)
        self.property_descriptors.append(self.PHONE_NUMBER)
        self.property_descriptors.append(self.USER_AGENT)

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):
        import time
        import datetime
        from time import gmtime, strftime
        import random, string
        import uuid

        from faker import Faker
        from faker.providers import internet, address, automotive, barcode, company, date_time, geo, job, misc, person
        from faker.providers import phone_number, user_agent
        import json

        fake = Faker()
        fake.add_provider(internet)
        fake.add_provider(address)
        fake.add_provider(automotive)
        fake.add_provider(barcode)
        fake.add_provider(company)
        fake.add_provider(date_time)
        fake.add_provider(geo)
        fake.add_provider(job)
        fake.add_provider(misc)
        fake.add_provider(person)
        fake.add_provider(phone_number)
        fake.add_provider(user_agent)

        attributes = dict()

        if ( context.getProperty(self.USER_ID).asBoolean() ): 
            uuid_key = str( '{0}_{1}'.format(strftime("%Y%m%d%H%M%S",gmtime()),uuid.uuid4() ) )
            attributes["uuidkey"] = uuid_key

        if ( context.getProperty(self.CREATED_DT).asBoolean() ): 
            attributes["createddt"] = str(fake.date() )

        if ( context.getProperty(self.EMAIL).asBoolean() ): 
            attributes["email"] = fake.ascii_email() 

        if ( context.getProperty(self.IP).asBoolean() ): 
            attributes["ipv4public"] = str(fake.ipv4_public() )

        if ( context.getProperty(self.USER_NAME).asBoolean() ): 
            attributes["username"] = fake.user_name() 

        if ( context.getProperty(self.CLUSTER_NAME).asBoolean() ): 
            attributes["clustername"] = fake.slug() 

        if ( context.getProperty(self.CITY).asBoolean() ): 
            attributes["city"] = fake.city() 

        if ( context.getProperty(self.COUNTRY).asBoolean() ): 
            attributes["country"] = fake.country() 

        if ( context.getProperty(self.POSTCODE).asBoolean() ): 
            attributes["postcode"] = str(fake.postcode())

        if ( context.getProperty(self.STREET_ADDRESS).asBoolean() ): 
            attributes["streetaddress"] = fake.street_address() 

        if ( context.getProperty(self.LICENSE_PLATE).asBoolean() ): 
            attributes["licenseplate"] = str(fake.license_plate())

        if ( context.getProperty(self.EAN13).asBoolean() ): 
            attributes["ean13"] = str(fake.ean13() )

        if ( context.getProperty(self.CATCH_PHRASE).asBoolean() ): 
            attributes["catchphrase"] = fake.catch_phrase() 

        if ( context.getProperty(self.COMMENT).asBoolean() ): 
            attributes["comment"] = fake.bs() 

        if ( context.getProperty(self.COMPANY).asBoolean() ): 
            attributes["company"] = fake.company() 

        if ( context.getProperty(self.LATITUDE).asBoolean() ): 
            attributes["latitude"] = str(fake.latitude() )

        if ( context.getProperty(self.LONGITUDE).asBoolean() ): 
            attributes["longitude"] = str(fake.longitude() )

        if ( context.getProperty(self.JOB).asBoolean() ): 
            attributes["job"] = fake.job() 

        if ( context.getProperty(self.MD5).asBoolean() ): 
            attributes["md5"] = str(fake.md5() )

        if ( context.getProperty(self.PASSWORD).asBoolean() ): 
            attributes["password"] = fake.password() 

        if ( context.getProperty(self.FIRST_NAME).asBoolean() ): 
            attributes["firstname"] = fake.first_name() 

        if ( context.getProperty(self.LAST_NAME).asBoolean() ): 
            attributes["lastname"] = fake.last_name() 

        if ( context.getProperty(self.PHONE_NUMBER).asBoolean() ): 
            attributes["phonenumber"] = str(fake.phone_number() )
  
        if ( context.getProperty(self.USER_AGENT).asBoolean() ): 
            attributes["useragent"] = fake.user_agent()  

        return FlowFileTransformResult(relationship = "success", contents=flowfile, attributes=attributes)
