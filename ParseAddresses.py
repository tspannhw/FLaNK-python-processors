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

### Parse Addresses
class ParseAddresses(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-M2'
        dependencies = ['pyap' ]
        description = """Extract NLP Addresses """
        tags = ["locations", "NLP", "pyap", "extract addresses", "addresses", "ai", "artificial intelligence", "ml", "machine learning", "text", "LLM"]

    PARSE_TEXT = PropertyDescriptor(
        name="Parse Text",
        description="Specifies the text to parse for addresses",
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
        import pyap

        parse_text = context.getProperty(self.PARSE_TEXT).evaluateAttributeExpressions(flowfile).getValue()

        addresses = pyap.parse(parse_text, country='US')

        primaryaddress = ""
        json_string = ""
        
        try:
            for address in addresses:
                # shows found address
                if ( str(address) != "" )
                        primaryaddress = str(address)
                        json_string = json.dumps(address.as_dict())
        except Exception as ex:
            print(ex)

        attributes = { "primaryaddress": primaryaddress }
        return FlowFileTransformResult(relationship = "success", contents=json_string, attributes=attributes)        
