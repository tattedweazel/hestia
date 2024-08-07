from datetime import datetime
import os


class Push:

	def __init__(self, user, server, parts, dry_run):
		self.date = datetime.now()
		self.user = user
		self.server = server
		self.parts = parts
		self.dry_run = dry_run
		self.commands = self.build_commands()

	def build_commands(self):
		commands = []
		for item in self.parts.items:
			command = "scp "
			user_string = self.user.connection_string
			source = item[0]
			server_string = self.server.connection_string
			destination = server_string + item[1]
			commands.append(f"{command}{user_string} {source} {destination}")
		return commands


	def send(self):
		for command in self.commands:
			print(command)
			if not self.dry_run:
				os.system(command)
			

class Server:

	def __init__(self, name):
		self.name = name.lower()
		self.ip = self.get_ip()
		self.user = self.get_user()
		self.connection_string = self.get_connection_string()


	def get_ip(self):
		if self.name == 'eds':
			return '34.219.155.118'
		elif self.name == 'dsp':
			return '172.20.14.103'
		elif self.name == 'o':
			return '52.88.208.144'


	def get_user(self):
		if self.name == 'eds':
			return 'centos'
		elif self.name in ['dsp', 'o']:
			return 'ubuntu'


	def get_connection_string(self):
		return f"{self.user}@{self.ip}:/home/{self.user}/processes/rt-data-hestia/"


class User:

	def __init__(self, initials, server):
		self.server = server
		self.connection_string = self.get_connection_string(initials)
		self.items = []
		

	def get_connection_string(self, initials):
		user = initials.lower()
		connection_map = {
			"dp": {
				"eds": "",
				"dsp": "-i ~/keys/rt-data.pem",
				"o": "-i ~/keys/rt-data-prod.cer"
			},
			"ds": {
				"eds": "-i ~/.ssh/id_rsa main.py",
				"dsp": "-i ~/keys/rt-data.pem",
				"o": "-i ~/keys/rt-data-prod.cer"
			}
		}
		return connection_map[user][self.server.name]


class PartsContainer:

	def __init__(self, parts):
		self.location_map = {
			"base"													: ("base/*.py", "base"),
			"jobs"													: ("jobs/*.py", "jobs"),
			"main"													: ("main.py", ""),
			"config"												: ("config.py", ""),
			"process_handlers"										: ("process_handlers/*.py", "process_handlers/"),
			"secrets"												: ("secrets.json", ""),
			"utils/auth"											: ("utils/auth/*.py", "utils/auth/"),
			"utils/classes"											: ("utils/classes/*.py", "utils/classes/"),
			"utils/components"										: ("utils/components/*.py", "utils/components/"),
			"utils/connectors"										: ("utils/connectors/*.py", "utils/connectors/"),
		}
		self.items = []
		self.get_items(parts.lower().split(','))


	def get_items(self, parts):
		if parts[0] == 'all':
			for name, values in self.location_map.items():
				self.items.append(values)
		else:
			for part in parts:
				location = self.get_location(part)
				if location:
					self.items.append(location)


	def get_location(self, part):
		return self.location_map[part]



