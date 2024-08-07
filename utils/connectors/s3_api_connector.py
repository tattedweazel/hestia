import boto3
from base.exceptions import S3ContentsException
from utils.connectors.api_connector import ApiConnector


class S3ApiConnector(ApiConnector):

	def __init__(self, file_location, bucket, profile_name, dry_run=False):
		super().__init__(file_location)
		self.rt_session = boto3.session.Session(profile_name=profile_name)
		self.s3 = self.rt_session.client('s3')
		self.bucket = bucket
		self.dry_run = dry_run


	def get_object_list_from_bucket(self, prefix):
		response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
		bucket_objects = []
		if 'Contents' in response:
			for content in response['Contents']:
				bucket_objects.append(content.get('Key'))
			return bucket_objects
		else:
			raise S3ContentsException()


	def download_files_from_object(self, key, filename):
		self.s3.download_file(Bucket=self.bucket, Key=key, Filename=filename)


	def put_object(self, file_object, key):
		if self.dry_run is True:
			return
		else:
			response = self.s3.put_object(Body=file_object, Bucket=self.bucket, Key=key)
			return response


	def upload_file(self, file_name, key):
		if self.dry_run is True:
			return
		else:
			response = self.s3.upload_file(Filename=file_name, Bucket=self.bucket, Key=key)
			return response


	def delete_objects_from_bucket(self, keys):
		if self.dry_run is True:
			return
		else:
			delete = {'Objects': []}
			for key in keys:
				delete['Objects'].append({'Key': key})

			self.s3.delete_objects(Bucket=self.bucket, Delete=delete)
