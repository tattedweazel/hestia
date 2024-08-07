import os
import string
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from utils.connectors.api_connector import ApiConnector


class GoogleCloudApiConnector(ApiConnector):

	def __init__(self, file_location, service):
		super().__init__(file_location)
		self.service = service
		self.services = {
			'docs': {'api_service_name': 'docs', 'api_version': 'v1'},
			'drive': {'api_service_name': 'drive', 'api_version': 'v3'},
			'sheets': {'api_service_name': 'sheets', 'api_version': 'v4'}
		}
		self.api_service_name = self.services[self.service]['api_service_name']
		self.api_version = self.services[self.service]['api_version']
		self.SCOPES = [
			"https://www.googleapis.com/auth/documents.readonly",
			"https://www.googleapis.com/auth/documents",
			"https://www.googleapis.com/auth/drive",
			"https://www.googleapis.com/auth/drive.file",
			"https://www.googleapis.com/auth/drive.appdata",
			"https://www.googleapis.com/auth/drive.metadata",
			"https://www.googleapis.com/auth/drive.scripts",
			"https://www.googleapis.com/auth/spreadsheets.readonly",
			'https://www.googleapis.com/auth/spreadsheets'
		]
		self.creds = None
		self.dir = file_location + 'utils/auth/'
		self.token_file = 'google_cloud_token.json'
		self.auth_file = 'google_cloud_stored_auth.json'
		self.service = None


	def get_credentials(self):
		if os.path.exists(self.dir + self.token_file):
			self.creds = Credentials.from_authorized_user_file(self.dir + self.token_file, self.SCOPES)

		if not self.creds or not self.creds.valid:
			if self.creds and self.creds.expired and self.creds.refresh_token:
				self.creds.refresh(Request())
			else:
				flow = InstalledAppFlow.from_client_secrets_file(self.dir + self.auth_file, self.SCOPES)
				self.creds = flow.run_local_server(port=0)

			with open(self.dir + self.token_file, "w") as token:
				token.write(self.creds.to_json())


	def get_service_api(self):
		self.get_credentials()
		self.service = build(self.api_service_name, self.api_version, credentials=self.creds)
		return self.service


	def create_doc(self, title):
		return self.service.documents().create(body={'title': title}).execute()


	def write_to_doc(self, doc_id, text, index=1):
		requests = [{'insertText': {'location': {'index': index}, 'text': text}}]
		result = self.service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()
		return result


	def create_sheet(self, title):
		spreadsheet = {"properties": {"title": title}}
		result = self.service.spreadsheets().create(body=spreadsheet, fields="spreadsheetId").execute()
		return result


	def write_to_sheet(self, sheet_id, df, value_input_option='RAW', major_dimension='ROWS'):
		# TODO: IF needed, create logic for dynamic starting range (depending on business use case)
		# Create Output
		columns = df.columns.tolist()
		output = df.values.tolist()
		output.insert(0, columns)
		# Create Range
		ending_range = self.identify_ending_range(df)
		output_range = f'A1:{ending_range}'

		self.service.spreadsheets().values().update(
			spreadsheetId=sheet_id,
			valueInputOption=value_input_option,
			range=output_range,
			body=dict(
				majorDimension=major_dimension,
				values=output
			)
		).execute()


	def move_file(self, file_id, folder_id):
		"""
		:param file_id: The file that is being moved
		:param folder_id: The folder in which the file is being moved to
		:return: None
		"""
		file = self.service.files().get(fileId=file_id, fields='parents').execute()
		previous_parents = ','.join(file.get('parents'))
		self.service.files().update(
			fileId=file_id,
			addParents=folder_id,
			removeParents=previous_parents,
			fields='id, parents'
		).execute()


	@staticmethod
	def identify_ending_range(df):
		# Create Letter Index
		letters = list(string.ascii_uppercase)
		double_letters = [i * 2 for i in letters]
		letter_index = letters + double_letters
		ending_letter = letter_index[len(list(df.columns)) - 1]
		ending_number = len(df) + 1
		return f'{ending_letter}{ending_number}'
