import boto3
import json
from utils.connectors.api_connector import ApiConnector


class SNSApiConnector(ApiConnector):

	def __init__(self, profile_name, region_name, dry_run=False, file_location=''):
		super().__init__(file_location)
		self.rt_session = boto3.session.Session(profile_name=profile_name)
		self.sns_client = self.rt_session.client("sns", region_name=region_name)
		self.dry_run = dry_run


	def create_topic(self, topic_name):
		if self.dry_run:
			return
		topic = self.sns_client.create_topic(Name=topic_name)
		return topic



	def send_message(self, topic_arn, message_title, message_body):
		if self.dry_run:
			return
		message = {
			"version": 1.0,
			"source": "custom",
			"content": {
				"textType": "client-markdown",
				"title": f":warning: {message_title}",
				"description": message_body
			}
		}
		response = self.sns_client.publish(TopicArn=topic_arn, Message=json.dumps(message))
		return response
