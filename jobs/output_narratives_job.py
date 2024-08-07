import os
from base.etl_jobv3 import EtlJobV3
from time import sleep
from utils.connectors.google_cloud_api_connector import GoogleCloudApiConnector
from utils.connectors.database_connector import DatabaseConnector  # TODO: REMOVE AFTER TESTING


class OutputNarrativesJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = '', local_mode=True)
		self.target_date = target_date
		self.youtube_dir = 'downloads/narratives/youtube/'
		self.site_dir = 'downloads/narratives/site/'
		self.audio_dir = 'downloads/narratives/audio/'
		self.output_dir = 'downloads/narratives/outputs/'
		self.folder_id = '1u39m0g5GvGeWdghczd6RPrcQzSs8EV7H'
		self.brand_narratives = {}
		self.docs_api_connector = GoogleCloudApiConnector(file_location=file_location, service='docs')
		self.docs_api = None
		self.drive_api_connector = GoogleCloudApiConnector(file_location=file_location, service='drive')
		self.drive_api = None


		self.db_connector = DatabaseConnector('')  # TODO: REMOVE AFTER TESTING


	def read_youtube_narratives(self):
		self.loggerv3.info('Reading YouTube narratives')
		for file in os.listdir(self.youtube_dir):
			brand = file.replace('.txt', '')
			file = open(self.youtube_dir + file, 'r')
			if brand not in self.brand_narratives:
				self.brand_narratives[brand] = {'youtube': file.read()}
			else:
				self.brand_narratives[brand]['youtube'] = file.read()
			file.close()


	def read_site_narratives(self):
		self.loggerv3.info('Reading site narratives')
		for file in os.listdir(self.site_dir):
			brand = file.replace('.txt', '')
			file = open(self.site_dir + file, 'r')
			if brand not in self.brand_narratives:
				self.brand_narratives[brand] = {'site': file.read()}
			else:
				self.brand_narratives[brand]['site'] = file.read()
			file.close()


	def read_audio_narratives(self):
		self.loggerv3.info('Reading audio narratives')
		for file in os.listdir(self.audio_dir):
			brand = file.replace('.txt', '')
			file = open(self.audio_dir + file, 'r')
			if brand not in self.brand_narratives:
				self.brand_narratives[brand] = {'audio': file.read()}
			else:
				self.brand_narratives[brand]['audio'] = file.read()
			file.close()


	def instatiate_apis(self):
		self.loggerv3.info('Instatiating APIs')
		self.docs_api = self.docs_api_connector.get_service_api()
		self.drive_api = self.drive_api_connector.get_service_api()


	def merge_files(self):
		self.loggerv3.info('Merge Files')
		for brand, narrative_dict in self.brand_narratives.items():
			self.loggerv3.info(f'Building for {brand}')
			narrative = ""
			if brand in ['letsplay', "let's play"]:
				output_brand = 'Lets Play'
			elif brand in ['rooster teeth animation', 'camp camp', 'rwby', 'red vs. blue']:
				output_brand = 'RT Anim'
			elif brand in ['anma podcast', 'anma']:
				output_brand = 'ANMA'
			elif brand in ['death battle!', 'death battle', 'death battle cast']:
				output_brand = 'Death Battle'
			elif brand in ['so... alright', 'so...alright']:
				output_brand = 'So...Alright'
			elif brand in ['tales from the stinky dragon']:
				output_brand = 'Stinky Dragon'
			elif brand in ['rooster teeth podcast']:
				output_brand = 'RTP'
			else:
				output_brand = brand.title()

			for platform, narrative_txt in narrative_dict.items():
				narrative += narrative_txt

			with open(f'{self.output_dir}{output_brand}.txt', 'a+') as f:
				f.write(f'{narrative}')


	def write_to_docs(self):
		self.loggerv3.info('Writng to docs')
		for file in os.listdir(self.output_dir):
			brand = file.replace('.txt', '')
			file = open(self.output_dir + file, 'r')
			narrative = file.read()
			# Create Title
			title = f'{self.target_date} - {brand}'
			# Create Doc
			doc = self.docs_api_connector.create_doc(title=title)
			sleep(2)
			# Get Doc ID
			doc_id = doc.get('documentId')
			# Write narrative to Doc
			self.docs_api_connector.write_to_doc(doc_id=doc_id, text=narrative)
			# Move Doc to Sub Folder
			self.drive_api_connector.move_file(file_id=doc_id, folder_id=self.folder_id)
			file.close()


	def clean_up_dirs(self):
		self.loggerv3.info('Cleaning up directories')
		if len(os.listdir(f'{self.youtube_dir}/')) > 1:
			os.system(f'rm {self.youtube_dir}/*.*')

		if len(os.listdir(f'{self.site_dir}/')) > 1:
			os.system(f'rm {self.site_dir}/*.*')

		if len(os.listdir(f'{self.audio_dir}/')) > 1:
			os.system(f'rm {self.audio_dir}/*.*')

		if len(os.listdir(f'{self.output_dir}/')) > 1:
			os.system(f'rm {self.output_dir}/*.*')


	def execute(self):
		self.loggerv3.info(f"Running Output Narratives Job")
		self.read_youtube_narratives()
		self.read_site_narratives()
		self.read_audio_narratives()
		self.instatiate_apis()
		self.merge_files()
		self.write_to_docs()
		self.clean_up_dirs()
		self.loggerv3.success("All Processing Complete!")
