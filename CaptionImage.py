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

### BLIP for image captioning
class CaptionImage(FlowFileTransform):
	class Java:
		implements = ['org.apache.nifi.python.processor.FlowFileTransform']

	class ProcessorDetails:
		version = '2.0.0-M2'
		dependencies = ['transformers' ]
		description = """Caption an image"""
		tags = ["caption", "blip-image-captioning-large", "coco", "model", "huggingface", "ai", "artificial intelligence", "ml", "machine learning", "images", "LLM"]

	CAPTION_OPTION = PropertyDescriptor(
		name="Caption Options",
		description="Future use as caption options",
		required=False,
		validators=[StandardValidators.NON_EMPTY_VALIDATOR],
		expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
	)

	property_descriptors = [
		CAPTION_OPTION
	]

	def __init__(self, **kwargs):
		super().__init__()
		self.property_descriptors.append(self.CAPTION_OPTION)

	def getPropertyDescriptors(self):
		return self.property_descriptors

	def transform(self, context, flowfile):
		import requests
		from PIL import Image
		from transformers import BlipProcessor, BlipForConditionalGeneration
		import sys
		import io

		caption_option = context.getProperty(self.CAPTION_OPTION).evaluateAttributeExpressions(flowfile).getValue()

		processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-large")
		model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-large")
		
		# Read the FlowFile content as "image".
		imagebinary = flowfile.getContentsAsBytes() # .decode()
		raw_image = Image.open(io.BytesIO(imagebinary))

		inputs = processor(raw_image, return_tensors="pt")
		out = model.generate(**inputs)
		caption = (processor.decode(out[0], skip_special_tokens=True))
		attributes = {"caption": caption, "captionoption": caption_option}

		return FlowFileTransformResult(relationship = "success", contents=flowfile, attributes=attributes)
