from abc import ABC
from utils.auth.secret_squirrel import SecretSquirrel


class Connector(ABC):

	def __init__(self, file_location):
		self.creds = SecretSquirrel(file_location).stash
		super().__init__()