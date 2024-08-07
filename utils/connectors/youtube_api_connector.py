import json
import httplib2
from oauth2client.file import Storage
from googleapiclient.discovery import build
from utils.connectors.api_connector import ApiConnector


class YouTubeApiConnector(ApiConnector):

	def __init__(self, file_location, service):
		super().__init__(file_location)
		"""
		OAuth resources: https://auth0.com/docs/authenticate/protocols/oauth
		"""
		self.service = service
		self.oauth_credentials_file = file_location + 'utils/auth/' + 'stored_auth.json'
		self.services = {
			'reporting': {'api_service_name': 'youtubereporting', 'api_version': 'v1'},
			'videos': {'api_service_name': 'youtube', 'api_version': 'v3'}
		}
		self.api_service_name = self.services[self.service]['api_service_name']
		self.api_version = self.services[self.service]['api_version']
		self.content_ids = {
			"RT Affiliate": "R6gVO4AWL-aCIaJPB2mreQ",
			"RT": "Sdm9nUQSFGpzVpn9zehLQQ"
		}
		self.credentials = None
		self.storage = None



	def get_credentials(self):
		"""
		Using oauth_credentials_file, creates a OAuth credentials object for authorization and resource owners for authentication.
		These credentials usually access resources on behalf of a resource owner (i.e. YouTube, Google).
		https://oauth2client.readthedocs.io/en/latest/source/oauth2client.file.html
		:return: Credentials object used to get resource object
		"""
		self.storage = Storage(self.oauth_credentials_file)
		return self.storage.get()


	def get_service_api(self):
		"""
		Uses credentials object on httplib2.Http client t to authorize and sign each request from each scope with the OAuth 2.0 access token.
		List of scopes are from oauth_credentials_file.
		https://oauth2client.readthedocs.io/en/latest/source/oauth2client.client.html
		:return: A Resource object for interacting with an API service (i.e. YouTube Reporting API)
		"""
		self.credentials = self.get_credentials()
		if self.service == 'reporting':
			return build(self.api_service_name, self.api_version, http=self.credentials.authorize(httplib2.Http()))
		elif self.service == 'videos':
			return build(self.api_service_name, self.api_version, developerKey=self.creds['YOUTUBE_API_KEY'])
		else:
			raise ValueError(f'Service param must be the following keys: {self.services.keys()}')



	def create_reporting_job(self, reporting_api, content_id, report_id):
		"""
		Note this method only needs to be called once ever for any YouTube Reporting API (bulk reports). It does not need to be called daily.
		:param reporting_api: The output of self.get_service_api
		:param content_id: The values in self.content_ids
		:param report_id: The id of the report that you want created
		:return: Object with meta data on the report being created
		"""
		reporting_job = reporting_api.jobs().create(
			onBehalfOfContentOwner=content_id,
			body=dict(reportTypeId=report_id, name=report_id)
		).execute()
		return reporting_job


	def get_report_types(self, reporting_api, content_id):
		"""
		:param reporting_api: The output of self.get_service_api
		:param content_id: The values in self.content_ids
		:return: A list of report types that can be created or downloaded
		"""
		results = reporting_api.reportTypes().list(onBehalfOfContentOwner=content_id).execute()
		return results['reportTypes']


	def test_access_token(self):
		"""
		Tests if the current access token is valid and what scope(s) are requested.
		:return: Dict
		"""
		stash = None
		with open(self.oauth_credentials_file, 'r') as raw_stash:
			stash = json.load(raw_stash)

		return f"https://www.googleapis.com/oauth2/v3/tokeninfo?access_token={stash['access_token']}"
