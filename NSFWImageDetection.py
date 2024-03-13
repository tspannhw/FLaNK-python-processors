# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
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

### NSFW image detection
class NSFWImageDetection(FlowFileTransform):
	class Java:
		implements = ['org.apache.nifi.python.processor.FlowFileTransform']

	class ProcessorDetails:
		version = '2.0.0-M2'
		dependencies = ['transformers' ]
		description = """Detect NSFW"""
		tags = ["image detection", "nsfw", "find", "FlowFileTransformResult", "huggingface", "ai", "artificial intelligence", "ml", "machine learning", "images", "LLM"]

	HF_OPTION = PropertyDescriptor(
		name="HuggingFace Options",
		description="Future use as huggingface options",
		required=False,
		validators=[StandardValidators.NON_EMPTY_VALIDATOR],
		expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
	)

	property_descriptors = [
		HF_OPTION
	]

	def __init__(self, **kwargs):
		super().__init__()
		self.property_descriptors.append(self.HF_OPTION)

	def getPropertyDescriptors(self):
		return self.property_descriptors

	def transform(self, context, flowfile):
		import requests
		from PIL import Image
		import sys
		import io
		from transformers import pipeline

		attributes = dict()
		classifier = pipeline("image-classification", model="Falconsai/nsfw_image_detection")

		caption_option = context.getProperty(self.HF_OPTION).evaluateAttributeExpressions(flowfile).getValue()

		# Read the FlowFile content as "image".
		imagebinary = flowfile.getContentsAsBytes() # .decode()
		raw_image = Image.open(io.BytesIO(imagebinary))

		outputstr = classifier(raw_image)

		for result in outputstr:
			if str(result['label']) == 'normal':
				attributes["normal"] = str(result['score'])
			else: 
				attributes["nsfw"] = str(result['score'])

		attributes["captionoption"] = str(caption_option)

		return FlowFileTransformResult(relationship = "success", contents=flowfile, attributes=attributes)
