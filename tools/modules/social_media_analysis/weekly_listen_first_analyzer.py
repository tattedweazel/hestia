import config
import os
import numpy as np
import pandas as pd
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class WeeklyListenFirstAnalyser:

	def __init__(self, author, output_file_name, text_match=None, by='week'):
		"""Assuming Sun - Sat is a week and Sun is the start of the week"""
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.author = author
		self.text_match = text_match
		self.by = by  # can be 'week' or 'total'
		self.input_directory = config.file_location + 'tools/input'
		self.output_directory = config.file_location + 'tools/output'
		self.input_df = None
		self.filtered_df = None
		self.output_df = None
		self.file_name = '_'.join([self.author, output_file_name])  # 'listen_first.csv'


	def author_mapper(self, author):
		if author == 'funhaus':
			return ['funhausteam']
		elif author == 'roosterteeth':
			return ['roosterteeth']
		elif author == 'achievementhunter':
			return ['achievementhunter', 'achievementhunt']
		elif author == 'squadteamforce':
			return ['squadteamforce']
		elif author == 'rwby':
			return ['officialrwby', 'rt.rwby']
		elif author == 'stinkydragon':
			return ['stinkydragonpod']
		elif author == 'redweb':
			return ['theredweb']
		elif author == 'deathbattle':
			return ['deathbattle', 'officialdeathbattle']
		elif author == 'fuckface':
			return ['fuckfacepod']
		elif author == 'facejam':
			return ['facejampod']
		elif author == 'blackboxdown':
			return ['blackboxdownpod']


	def read_files(self):
		self.loggerv3.info('Reading files')
		for filename in os.listdir(self.input_directory):
			if '.csv' in filename:
				f = os.path.join(self.input_directory, filename)
				try:
					df = pd.read_csv(
						f,
						skiprows=lambda x: x in [1, 2],
						usecols=['Date', 'Channel', 'Author', 'Text', 'Engagements', 'Video Views'],
						encoding='utf-8',
						thousands=','
					)
				except:
					raise UnicodeError(f)

				if self.input_df is None:
					self.input_df = df
				else:
					self.input_df = pd.concat([self.input_df, df])


	def clean_dataframe(self):
		self.loggerv3.info('Clean dataframe')
		self.input_df['Date'] = pd.to_datetime(self.input_df['Date'], format='%m/%d/%Y')
		self.input_df['Channel'] = self.input_df['Channel'].str.lower()
		self.input_df['Author'] = self.input_df['Author'].str.lower()
		self.input_df['Text'] = self.input_df['Text'].str.lower()
		self.input_df['Engagements'] = pd.to_numeric(self.input_df['Engagements'], errors='coerce')
		self.input_df['Video Views'] = pd.to_numeric(self.input_df['Video Views'], errors='coerce')
		self.input_df['Week Start'] = self.input_df['Date'].dt.to_period('W-SAT').dt.start_time


	def analyze_data(self):
		self.loggerv3.info('Analyzing data')
		authors = self.author_mapper(self.author)
		self.filtered_df = self.input_df[self.input_df['Author'].isin(authors)]
		if self.text_match is not None:
			self.filtered_df = self.filtered_df[self.input_df['Text'].isin([self.text_match])]

		if self.by == 'week':
			self.output_df = self.filtered_df.groupby(['Week Start', 'Channel']).agg(
				Mean=('Video Views', np.mean),
				Sum=('Video Views', np.sum),
				Count=('Video Views', np.count_nonzero)
			)
			self.output_df = self.output_df.astype(int, errors='ignore')
			self.output_df.reset_index(inplace=True)
			self.output_df = self.output_df.sort_values(by=['Week Start'], ascending=False)
			self.output_df = self.output_df[['Week Start', 'Channel', 'Count', 'Sum', 'Mean']]
		elif self.by == 'total':
			self.output_df = self.filtered_df.groupby(['Channel']).agg(
				Mean=('Video Views', np.mean),
				Sum=('Video Views', np.sum),
				Count=('Video Views', np.count_nonzero)
			)
			self.output_df = self.output_df.astype(int, errors='ignore')
			self.output_df.reset_index(inplace=True)
			self.output_df = self.output_df[['Channel', 'Count', 'Sum', 'Mean']]


	def output(self):
		self.loggerv3.info('Outputting data')
		full_file_path = '/'.join([self.output_directory, self.file_name])
		self.output_df.to_csv(full_file_path, index=False)
		pd.set_option('display.max_columns', None)
		print(self.output_df)


	# def clean_up(self):
	# 	self.loggerv3.info('Cleaning up files in local dir')
	# 	os.system(f'rm {self.input_directory}/*.*')


	def execute(self):
		self.read_files()
		self.clean_dataframe()
		self.analyze_data()
		self.output()
		self.loggerv3.success("All Processing Complete!")



