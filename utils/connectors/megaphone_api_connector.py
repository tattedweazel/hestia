import json
import requests
from time import sleep
from utils.connectors.api_connector import ApiConnector


class MegaphoneApiConnector(ApiConnector):

	def __init__(self, file_location, logger=None):
		super().__init__(file_location)
		self.requests = []
		self.SLEEP_FOR_429 = 0.25
		self.retries = 15
		self.loggerv3 = logger


	def get_all_podcasts(self, channel):
		""" Gets a list of all podcasts for a given channel """

		params = {
			"channel": channel,
			"resource": "podcasts"
		}
		return self.fetch_data(params)


	def get_podcast(self, channel, podcast_id):
		""" Gets an individual Podcast for a given channel
			required: channel, podcast_id """

		params = {
			"channel": channel,
			"resource": "podcasts",
			"resource_id": podcast_id
		}
		return self.fetch_data(params)


	def get_all_episodes(self, channel, podcast_id):
		""" Gets all episodes for an individual Podcast for a given channel
			required: channel, podcast_id """

		params = {
			"channel": channel,
			"resource": "podcasts",
			"resource_id": podcast_id,
			"sub_resource": "episodes"
		}
		return self.fetch_data(params)


	def get_episode(self, channel, podcast_id, episode_id):
		""" Gets an individual episode for an individual Podcast for a given channel
			required: channel, podcast_id """

		params = {
			"channel": channel,
			"resource": "podcasts",
			"resource_id": podcast_id,
			"sub_resource": "episodes",
			"sub_resource_id": episode_id
		}
		return self.fetch_data(params)


	def fetch_data(self, params):
		""" Handles calling the API and converting the response into the requested data
			required: params """

		# Make the call as is to retrieve data
		headers, data = self.call(params)

		# Make sure that the data we got back is ALL the data
		if 'X-Total' not in headers or len(data) == int(headers['X-Total']):
			return data

		# If it's not, let's paginate until it is
		page = 1
		while len(data) < int(headers['X-Total']):
			page += 1
			params['page'] = page
			headers, new_data = self.call(params)
			for item in new_data:
				data.append(item)

		return data


	def call(self, params):
		""" Makes a call to the Megaphone API
			valid params:
			channel: one of - rooster_teeth, the_axes_files, the_roost
			resource: one of - podcasts, podcast, episodes, episode
			resource_id: ID of resource to be called (for podcast and episode resources)
		"""

		# Build out or params from what was passed
		call_params = {
			"channel": None,
			"resource": None,
			"resource_id": None,
			"sub_resource": None,
			"sub_resource_id": None,
			"page": "1",
			"per_page": "500"
		}
		# Update params dict with what was passed
		for param in params:
			call_params[param] = params[param]

		# Build out the full URI based on passed params
		full_uri = self.endpoint_uri(call_params['channel']) + call_params['resource']

		if call_params['resource_id'] is not None:
			full_uri += "/" + call_params['resource_id']

		if call_params['sub_resource'] is not None:
			full_uri += "/" + call_params['sub_resource']

		if call_params['sub_resource_id'] is not None:
			full_uri += "/" + call_params['sub_resource_id']

		full_uri += f"?page={call_params['page']}&per_page={call_params['per_page']}"

		# Actually make the call
		rq_headers = {"Authorization": f"Token token=\"{self.creds['MEGAPHONE_TOKEN']}\""}

		self.requests.append({
			'headers': rq_headers,
			'uri': full_uri
		})


		retries = 0
		while retries < self.retries:
			sleep(self.SLEEP_FOR_429 * retries)
			response = requests.get(full_uri, headers=rq_headers)
			headers = response.headers
			retries += 1
			if response.status_code >= 200 and response.status_code <= 299:
				data = json.loads(response.text)
				return (headers, data)
			elif retries == self.retries:
				data = []
				self.loggerv3.info(f"Failed to get data on call with params: {params}")
				return (headers, data)


	def endpoint_uri(self, channel):
		""" Passes back endpoint based on requested channel
			valid channels: one of - rooster_teeth, the_axes_files, the_roost
		"""

		return {
			'rooster_teeth': "https://cms.megaphone.fm/api/networks/ccbaeb96-d0fd-11e9-a861-238b9f17d424/",
			'the_roost': "https://cms.megaphone.fm/api/networks/d82fece0-7280-11ea-9592-672c6747f374/",
			'rooster_teeth_premium': 'https://cms.megaphone.fm/api/networks/fe848546-0c16-11ec-a63f-c71506a7206a/',
			'blind_nil_audio': 'https://cms.megaphone.fm/api/networks/48b5f5da-53f2-11ee-bc67-f76b729db41e/',
			'the_axe_files': 'https://cms.megaphone.fm/api/networks/2ebf08f4-3332-11ea-94fd-cb3f1945bf36/'
		}[channel]
