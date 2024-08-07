from pyairtable import Table
from pyairtable.formulas import match
from utils.connectors.api_connector import ApiConnector


class AirtableApiConnector(ApiConnector):

	def __init__(self, file_location, airtable_base, airtable_name):
		super().__init__(file_location)
		self.airtable_bases = {
			'rooster_teeth_hq': 'appDzrFX32ZtJqWhp',
			'cancel_survey': 'app4Vle694LyFPHH0'
		}
		self.airtable_endpoints = {
			'funhaus': 'tblRQ7DhOBpTU1BpV',
			'team_members': 'tbl1OEjVo3D1iPy7H',
			'submissions': 'tblT6etR7pbNtOcXn'
		}
		self.airtable_tokens = {
			'rooster_teeth_hq': self.creds['AIRTABLE_RT_TOKEN'],
			'cancel_survey': self.creds['AIRTABLE_SURVEY_TOKEN']
		}
		self.table = Table(
			api_key=self.airtable_tokens[airtable_base],
			base_id=self.airtable_bases[airtable_base],
			table_name=self.airtable_endpoints[airtable_name]
		)


	def get_single_record(self, fields):
		return self.table.first(fields=fields)


	def get_all_records(self, fields=[], formula={}):
		match_formula = match(formula)
		return self.table.all(fields=fields, formula=match_formula)
