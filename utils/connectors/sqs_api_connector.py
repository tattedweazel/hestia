import boto3
import json
from time import sleep
from utils.connectors.api_connector import ApiConnector


class SQSApiConnector(ApiConnector):

	def __init__(self, profile_name, queue_name, region_name, dry_run=False, file_location=''):
		super().__init__(file_location)
		self.rt_session = boto3.session.Session(profile_name=profile_name)
		self.sqs_client = self.rt_session.client("sqs", region_name=region_name)
		self.queue_url = f"https://sqs.{region_name}.amazonaws.com/{self.creds['AWS_RT_ACCOUNT_ID']}/{queue_name}"
		self.dry_run = dry_run


	def send_message(self, message_body, **kwargs):
		if self.dry_run:
			return
		message_response = self.sqs_client.send_message(QueueUrl=self.queue_url, MessageBody=message_body, **kwargs)
		return message_response


	def receive_message(self, wait_time_seconds=20, **kwargs):
		message_response = self.sqs_client.receive_message(QueueUrl=self.queue_url, WaitTimeSeconds=wait_time_seconds, **kwargs)
		if 'Messages' in message_response:
			return {'status': 200, 'response': message_response}
		else:
			return {'status': 204}


	def delete_message(self, receipt_handle=None):
		if self.dry_run:
			return

		if receipt_handle is None:
			message_response = self.sqs_client.receive_message(QueueUrl=self.queue_url, WaitTimeSeconds=20, MaxNumberOfMessages=1)
			if 'Messages' in message_response and 'ReceiptHandle' in message_response['Messages'][0]:
				receipt_handle = message_response['Messages'][0]['ReceiptHandle']
			else:
				return {'status': 204}

		self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
		return {'status': 200}


	def receive_message_and_delete(self, wait_time_seconds=20):
		message_response = self.sqs_client.receive_message(QueueUrl=self.queue_url, WaitTimeSeconds=wait_time_seconds)
		if 'Messages' in message_response:
			if self.dry_run:
				pass
			else:
				receipt_handle = message_response['Messages'][0]['ReceiptHandle']
				self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
			return {'status': 200, 'response': message_response}
		else:
			return {'status': 204}


	def listen_to_queue(self, wait_time_seconds=20, listen_interval_seconds=120, **kwargs):
		"""
		:param wait_time_seconds: the wait time during an sqs receive_message call
		:param listen_interval_seconds: the time between each sqs receive_message call
		:return:
		"""
		while True:
			response = self.receive_message(wait_time_seconds=wait_time_seconds, **kwargs)
			if response['status'] == 200:
				return response
			else:
				print('sleeping')
				sleep(listen_interval_seconds)


	def extract_s3_key(self, message):
		message_response = message['response']
		if 'Messages' in message_response:
			outer_body = json.loads(message_response['Messages'][0]['Body'])
			inner_body = json.loads(outer_body['Message'])
			if 'Records' in inner_body and 's3' in inner_body['Records'][0]:
				s3_key = inner_body['Records'][0]['s3']['object']['key']
				return s3_key
