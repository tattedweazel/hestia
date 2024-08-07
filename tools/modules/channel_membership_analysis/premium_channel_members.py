import config
import pandas as pd
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class PremiumChannelMembers:

	def __init__(self):
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.output_directory = config.file_location + 'tools/output'
		self.output_filename = 'all_channel_counts.csv'
		self.viewers = {}
		self.channel_counts = {}
		self.final_dataframe = None


	def get_viewers(self):
		self.loggerv3.info('Getting viewers by channel')
		query = f"""
		SELECT vv.user_key, dse.channel_title
		FROM warehouse.vod_viewership vv
				 LEFT JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
		WHERE vv.user_tier in ('trial', 'premium')
		  AND vv.start_timestamp >= current_date - 30
		 AND dse.channel_title IS NOT NULL 
		GROUP BY 1, 2
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			user_key = result[0]
			channel = result[1]
			if user_key not in self.viewers:
				self.viewers[user_key] = [channel]
			else:
				self.viewers[user_key].append(channel)


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		for user_key, channels in self.viewers.items():
			channels.sort()
			channels_str = ','.join(channels)
			if channels_str not in self.channel_counts:
				self.channel_counts[channels_str] = 1
			else:
				self.channel_counts[channels_str] += 1



	def output(self):
		self.loggerv3.info('Outputting final viewers list')
		self.final_dataframe = pd.DataFrame([self.channel_counts])
		self.final_dataframe = self.final_dataframe.tranpose()
		self.final_dataframe = self.final_dataframe.reset_index()
		full_file_path = '/'.join([self.output_directory, self.output_filename])
		self.final_dataframe.to_csv(full_file_path, index=False)


	def execute(self):
		self.get_viewers()
		self.build_final_dataframe()
		self.output()
		self.loggerv3.success("All Processing Complete!")



