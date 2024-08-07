from datetime import datetime

class Logger:

	def __init__(self):
		self.enabled = True

	def log(self, msg, inline=False):
			"""
			like print, but better because timestamps and inline option
			"""
			if self.enabled:
				if inline:
					print(f"{datetime.now()}: {msg}{' '*10}", end="\r")
				else:
					print(f"{datetime.now()}: {msg}")
