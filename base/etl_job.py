from abc import ABC, abstractmethod
from utils.components.logger import Logger


class EtlJob(ABC):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, table_name = None):
		self.target_date = target_date
		self.db_connector = db_connector
		self.api_connector = api_connector
		self.table_name = table_name
		self.logger = Logger()
		super().__init__()

	@abstractmethod
	def execute(self):
		pass
