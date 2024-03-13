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

# https://huggingface.co/microsoft/resnet-50

### RES Net image classification
class RESNetImageClassification(FlowFileTransform):
	class Java:
		implements = ['org.apache.nifi.python.processor.FlowFileTransform']

	class ProcessorDetails:
		version = '2.0.0-M2'
		dependencies = ['transformers', 'datasets', 'torch' ]
		description = """Classify images"""
		tags = ["image classification", "resnet", "microsoft", "models", "huggingface", "ai", "artificial intelligence", "ml", "machine learning", "images", "LLM"]

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
		from transformers import AutoImageProcessor, ResNetForImageClassification
		import torch
		from datasets import load_dataset
		import sys
		import io
		import requests
		from PIL import Image
		model_name = "microsoft/resnet-50"

		attributes = dict()
		image_processor = AutoImageProcessor.from_pretrained(model_name)
		model = ResNetForImageClassification.from_pretrained(model_name)

		hf_option = context.getProperty(self.HF_OPTION).evaluateAttributeExpressions(flowfile).getValue()

		# Read the FlowFile content as "image".
		imagebinary = flowfile.getContentsAsBytes() 
		raw_image = Image.open(io.BytesIO(imagebinary))

		try:
			inputs = image_processor(raw_image, return_tensors="pt")

			with torch.no_grad():
			    logits = model(**inputs).logits

			predicted_label = logits.argmax(-1).item()
			attributes["classificationlabel"] = str(model.config.id2label[predicted_label])
		except Exception as ex:
			print(ex)

		attributes["hfoption"] = str(hf_option)
		attributes["modelused"] = str(model_name)

		return FlowFileTransformResult(relationship = "success", contents=flowfile, attributes=attributes)
