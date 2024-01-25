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

### Wiki Data Retrieve
### wikipedia-api
### https://wikipedia-api.readthedocs.io/en/latest/README.html
### pip3 install wikipedia-api
class GetWikiData(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-M1'
        dependencies = ['wikipedia-api']
        description = """Get a Wiki Article """
        tags = ["wikipedia", "article", "wiki",  "text"]

    FORMAT = PropertyDescriptor(
        name="Plain Text Wiki format or HTML",
        description="Format",
        default_value="text",
        allowable_values=["text", "html"],
        required=True
    )

    WIKIPAGE = PropertyDescriptor(
        name="Wiki Page",
        description="Specifies which wiki page",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [
        FORMAT,
        WIKIPAGE
    ]

    def __init__(self, **kwargs):
        super().__init__()
        self.property_descriptors.append(self.FORMAT)
        self.property_descriptors.append(self.WIKIPAGE)

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):
        import wikipediaapi
        
        wikipage = context.getProperty(self.WIKIPAGE).evaluateAttributeExpressions(flowfile).getValue()
        whichone = context.getProperty(self.FORMAT).evaluateAttributeExpressions(flowfile).getValue()

        attributes = {"format": whichone, "wikipage": wikipage}

        if (whichone != None):
            if (whichone == "text"):
            	wiki_wiki = wikipediaapi.Wikipedia(user_agent='NiFi tspann@cloudera',language='en',extract_format=wikipediaapi.ExtractFormat.WIKI)
            else: 
            	wiki_wiki = wikipediaapi.Wikipedia(user_agent='NiFi tspann@cloudera',language='en',extract_format=wikipediaapi.ExtractFormat.HTML)

        if (wikipage != None):
        	results = wiki_wiki.page(wikipage)
       		attributes["results"] = str(results.text)

        return FlowFileTransformResult(relationship = "success", contents=None, attributes=attributes)
