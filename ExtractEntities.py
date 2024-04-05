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
### https://spacy.io/usage/spacy-101
### https://www.newscatcherapi.com/blog/named-entity-recognition-with-spacy
class ExtractEntities(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-M2'
        dependencies = ['spacy' ]
        description = """Extract NLP Entities """
        tags = ["locations", "NLP", "extract text", "SPacY", "ai", "artificial intelligence", "ml", "machine learning", "text", "LLM"]

    PARSE_TEXT = PropertyDescriptor(
        name="Parse Text",
        description="Specifies the text to parse for company names",
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
        import spacy
        
        parse_text = context.getProperty(self.PARSE_TEXT).evaluateAttributeExpressions(flowfile).getValue()

        nlp = spacy.load('en_core_web_sm')
        doc = nlp(parse_text)

        orgs=[]
        dates=[]
        persons=[]
        locs=[]
        moneys=[]
        times=[]
        products=[]
        quantities=[]
        events=[]
        facs=[]
        gpes=[]

        for entity in doc.ents:
         if entity.label_ == 'GPE':
            gpes.append(entity.text)
         if entity.label_ == 'ORG':
            orgs.append(entity.text)
         if entity.label_ == 'DATE':
            dates.append(entity.text)
         if entity.label_ == 'PERSON':
            persons.append(entity.text)
         if entity.label_ == 'LOC':
            locs.append(entity.text)
         if entity.label_ == 'MONEY':
            moneys.append(entity.text)
         if entity.label_ == 'TIME':
            times.append(entity.text)                                                
         if entity.label_ == 'PRODUCT':
            products.append(entity.text)
         if entity.label_ == 'QUANTITY':
            quantities.append(entity.text)
         if entity.label_ == 'EVENT':
            events.append(entity.text)      
         if entity.label_ == 'FAC':
            facs.append(entity.text)

        # ORG - 
        # DATE - dates
        # PERSON - people
        # LOC - non gpe locations 
        # MONEY
        # TIME
        # PRODUCT - food, objects
        # QUANTITY
        # EVENT
        # FAC - buildings airports highways bridges
        # GPE - countries cities states

        orgstr = ""
        datestr = ""
        personstr = ""
        locstr = ""
        moneystr = ""
        timestr = ""
        productstr = ""
        quantitiestr = ""
        eventstr = ""
        facstr = ""
        gpestr = ""
        
        try:
            orgstr = ', '.join(orgs)
            datestr = ', '.join(dates)
            personstr = ', '.join(persons)
            locstr = ', '.join(locs)
            moneystr = ', '.join(moneys)
            timestr = ', '.join(times)
            productstr = ', '.join(products)
            quantitiestr = ', '.join(quantities)
            eventstr = ', '.join(events)
            facstr = ', '.join(facs)
            gpestr = ', '.join(gpes)
        except Exception as ex:
            print(ex)

        attributes = {"orgs": orgstr, "dates": datestr, "persons": personstr, "locs": locstr,
                      "moneys": moneystr, "times": timestr, "products": productstr, "quantities": quantitiestr,
                      "events": eventstr, "facs": facstr, "gpes": gpestr }
        return FlowFileTransformResult(relationship = "success", contents=flowfile, attributes=attributes)
