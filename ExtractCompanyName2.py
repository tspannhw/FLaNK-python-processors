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
class ExtractCompanyName(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-SNAPSHOT'
        dependencies = ['transformers', 'spacy', 'torch', 'torchvision', 'torchaudio' ]
        description = """Extract Company Name """
        tags = ["company name", "xlm-roberta-large-finetuned-conll03-english", "NLP", "extract text", "SPacY", "ai", "artificial intelligence", "ml", "machine learning", "text", "LLM"]

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
        from transformers import pipeline
        import spacy
        from spacy.matcher import Matcher
        
        parse_text = context.getProperty(self.PARSE_TEXT).evaluateAttributeExpressions(flowfile).getValue()

        # nlp = spacy.load('en_core_web_sm')
        model_checkpoint = "xlm-roberta-large-finetuned-conll03-english"
        token_classifier = pipeline(
            "token-classification", model=model_checkpoint, aggregation_strategy="simple"
        )

        classifier = token_classifier(parse_text)
        values = [item for item in classifier if item["entity_group"] == "ORG"]
        res = [sub['word'] for sub in values]
        final1 = list(set(res))  # Remove duplicates
        final = list(filter(None, final1)) # Remove empty strings

        companyList = ''
        companyName = ''

        if (final != None):
            companyList = json.dumps(final)
            companyName = final[0]

        attributes = {"companylist": companyList, "parsedcompany": companyName}

        if (final != None):
            for i, val in enumerate(final):
                attributes["company" + str(i)] =val

        return FlowFileTransformResult(relationship = "success", contents=None, attributes=attributes)
