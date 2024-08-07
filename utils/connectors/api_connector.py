from base.connector import Connector


class ApiConnector(Connector):

	def __init__(self, file_location):
		super().__init__(file_location)
