from abc import ABC, abstractmethod


class ServerProcessHandler(ABC):

	def __init__(self):
		self.local_files = None
		super().__init__()

	@abstractmethod
	def run_jobs(self):
		pass