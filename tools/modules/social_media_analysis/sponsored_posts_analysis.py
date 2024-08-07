import config
import os
import numpy as np
import pandas as pd
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class SponsoredPostsAnalysis:

	def __init__(self, authors, include_paid=True):
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.input_directory = config.file_location + 'tools/input'
		self.output_directory = config.file_location + 'tools/output'
		self.input_file = 'sponsored_input.csv'
		self.output_file = 'sponsored_posts.xlsx'
		self.authors = authors
		self.include_paid = include_paid
		self.input_df = None
		self.sponsored_df = None
		self.organic_df = None
		self.writer = None


	def read_files(self):
		self.loggerv3.info('Reading files')
		f = os.path.join(self.input_directory, self.input_file)
		try:
			self.input_df = pd.read_csv(
				f,
				skiprows=lambda x: x in [1, 2],
				usecols=['Channel', 'Author', 'Sponsor Name', 'Paid', 'Impressions', 'Paid Impressions', 'Reach', 'Engagements'],
				encoding='utf-8',
				thousands=','
			)
		except:
			raise UnicodeError(f)


	def clean_dataframe(self):
		self.loggerv3.info('Clean dataframe')
		# Lowercase strings
		for col in ['Channel', 'Author']:
			self.input_df[col] = self.input_df[col].str.lower()

		# Convert AH author name
		self.input_df.loc[(self.input_df['Author'].isin(['achievementhunter', 'achievementhunt'])), 'Author'] = 'achievementhunter'

		# Filter to only Authors we are requesting
		self.input_df = self.input_df[self.input_df['Author'].isin(self.authors)]

		# Make numeric fields numeric (just in case)
		for col in ['Impressions', 'Paid Impressions', 'Reach', 'Engagements']:
			self.input_df[col].fillna(0, downcast='infer', inplace=True)
			self.input_df[col] = pd.to_numeric(self.input_df[col], errors='coerce')

		# Set Impressions = Reach, if Impressions is 0
		self.input_df['Impressions'] = np.where((self.input_df['Impressions'] == 0) & (self.input_df['Reach'] != 0), self.input_df['Reach'], self.input_df['Impressions'])

		if self.include_paid:
			# Subtract Paid Impressions
			self.input_df['Impressions'] = self.input_df['Impressions'] - self.input_df['Paid Impressions']

		# Only keep posts with Impressions
		self.input_df = self.input_df[self.input_df['Impressions'] != 0]

		# Identify Organic posts
		self.input_df['Sponsor Name'] = np.where((self.input_df['Sponsor Name'].isnull()) & (self.input_df['Paid'].isnull()), 'Organic', self.input_df['Sponsor Name'])

		# Keep only Sponsored or Organic
		self.input_df = self.input_df[self.input_df['Sponsor Name'].notnull()]


	def initiate_output(self):
		self.loggerv3.info('Initiating output')
		f = os.path.join(self.output_directory, self.output_file)
		self.writer = pd.ExcelWriter(f, engine='xlsxwriter')


	def analyze_overall(self):
		self.loggerv3.info('Analyzing overall data')

		output = self.input_df.groupby(['Sponsor Name']).agg(
			avg_impressions=('Impressions', np.mean),
			avg_engagements=('Engagements', np.mean),
			posts=('Impressions', np.count_nonzero)
		)
		convert = {'avg_impressions': int, 'avg_engagements': int}
		output = output.astype(convert)
		output.to_excel(self.writer, sheet_name='overall')


	def analyze_by_channel(self):
		self.loggerv3.info('Analyzing data by channel')

		output = self.input_df.groupby(['Sponsor Name', 'Channel']).agg(
			avg_impressions=('Impressions', np.mean),
			avg_engagements=('Engagements', np.mean),
			posts=('Impressions', np.count_nonzero)
		)
		convert = {'avg_impressions': int, 'avg_engagements': int}
		output = output.astype(convert)
		output.to_excel(self.writer, sheet_name='sponsored_channel')


	def analyze_by_author(self):
		self.loggerv3.info('Analyzing data by author')

		output = self.input_df.groupby(['Sponsor Name', 'Author']).agg(
			avg_impressions=('Impressions', np.mean),
			avg_engagements=('Engagements', np.mean),
			posts=('Impressions', np.count_nonzero)
		)
		convert = {'avg_impressions': int, 'avg_engagements': int}
		output = output.astype(convert)
		output.to_excel(self.writer, sheet_name='sponsored_author')


	def analyze_by_channel_and_author(self):
		self.loggerv3.info('Analyzing data by channel and author')

		output = self.input_df.groupby(['Sponsor Name', 'Channel', 'Author']).agg(
			avg_impressions=('Impressions', np.mean),
			avg_engagements=('Engagements', np.mean),
			posts=('Impressions', np.count_nonzero)
		)
		convert = {'avg_impressions': int, 'avg_engagements': int}
		output = output.astype(convert)
		output.to_excel(self.writer, sheet_name='sponsored_channel_author')


	def save_output(self):
		self.loggerv3.info('Outputting data')
		self.writer.save()


	# def clean_up(self):
	# 	self.loggerv3.info('Cleaning up files in local dir')
	# 	os.system(f'rm {self.input_directory}/*.*')


	def execute(self):
		self.read_files()
		self.clean_dataframe()
		self.initiate_output()
		self.analyze_overall()
		self.analyze_by_channel()
		self.analyze_by_author()
		self.analyze_by_channel_and_author()
		self.save_output()
		self.loggerv3.success("All Processing Complete!")



