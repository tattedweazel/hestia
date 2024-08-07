from collections import OrderedDict


class SignupJourney:

	def __init__(self, user_id, records, valid_date):
		self.ANON_CODE = '00_anon'
		self.user_id = user_id
		self.records = sorted(records, key=lambda d: d['event_timestamp'])
		self.user_key = None
		self.valid_date = valid_date
		self.valid = False
		self.keys = []
		self.tiers = []
		self.window = []
		self.steps = []
		self.sequence = self.parse_sequence()
		self.step_timing = self.parse_timing()
		self.result = None
		self.set_user_key()
		self.audit()
		self.set_result()


	def parse_sequence(self):
		sequence = []
		keys = set()
		tiers = set()
		window = set()
		steps = set()
		for record in self.records:
			keys.add(record['user_id'] if record['user_id'] is not None else self.ANON_CODE ) 
			tiers.add(record['user_tier'])
			window.add(record['event_timestamp'].strftime('%Y-%m-%d'))
			steps.add(record['step'])
			if record['state'] == 'entered':
				mod = 'N'
			elif record['state'] == 'exited':
				mod = 'X'
			else:
				mod = 'U'
			sequence.append(f"{mod}{record['step']}")
		self.keys = sorted(list(keys))
		self.tiers = sorted(list(tiers))
		self.window = sorted(window)
		self.steps = sorted(list(steps))
		self.min_step = self.steps[0]
		self.max_step = self.steps[-1]
		return sequence


	def get_sequence(self):
		return '-'.join(self.sequence)


	def parse_timing(self):
		time_map = OrderedDict()
		timings = []
		previous_time = None
		for record in self.records:
			current_time = record['event_timestamp']
			if not previous_time:
				timings.append(0.0)
			else:
				delta = current_time - previous_time
				timings.append(delta.total_seconds())
			previous_time = current_time
		idx = 0
		for step in self.sequence:
			time_map[step] = timings[idx]
			idx += 1
		return time_map


	def set_user_key(self):
		for key in self.keys:
			if key != self.ANON_CODE:
				self.user_key = key


	def set_result(self):
		"""
		" Determines the result of a particular Journey
		" Valid options include:
		" - anon_to_free
		" - anon_to_premium
		" - free_to_premium
		" - non_counting_premium
		" - premium_login
		" - anon_bounce
		" - free_bounce
		"""

		if len(self.tiers) > 1: # Have Upgraded or Logged in
			# If they started as anon
			if self.tiers[0] == 'anon':
				# and their last tier was free
				if self.tiers[-1] == 'free':
					self.result = 'anon_to_free'
				# and their last tier was premium
				elif self.tiers[-1] == 'premium':
					if 3 not in self.steps:
						self.result = 'premium_login'
					else:
						self.result = 'anon_to_premium'
			# If they started as free
			elif self.tiers[0] == 'free':
				if self.tiers[-1] == 'premium':
					self.result = 'free_to_premium'


		else: # Have Bounced
			if self.tiers[0] == 'premium': # They started as premium
				self.result = 'non_counting_premium'
			elif self.tiers[0] == 'anon':
				self.result = 'anon_bounce'
			elif self.tiers[0] == 'free':
				self.result = 'free_bounce'


	def audit(self):
		# Check if events start on target day, if they don't start until the next day, we're not looking for this record
		if self.window[0] == self.valid_date:
			self.valid = True

