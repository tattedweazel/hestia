from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd
from utils.connectors.airtable_api_connector import AirtableApiConnector



class AirtableFunhausJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='funhaus_productions', local_mode=True)
        """ Retrieves data from Airtable and creates a episodes-like table for funhaus. Also creates dim table for members and their role in a given production. """

        self.schema = 'airtable'
        self.airtable_base = 'rooster_teeth_hq'
        self.airtable_name = 'funhaus'
        self.dim_table_name = 'dim_production_members'
        self.airtable = AirtableApiConnector(file_location=self.file_location, airtable_base=self.airtable_base, airtable_name=self.airtable_name)
        self.fields = ['Production', 'Producer', 'Editor', 'Status', 'FH Series', 'FH YT Launch', 'RT ONLY Launch', 'FH Stream Date', 'Social Programming Date',
                       'Cast', 'RT Launch', 'Ad Read?',  'Live Stream?', 'Wave 1 Title', 'Wave 2 Packaging Date', 'Wave 2 Title', 'Guests', 'Uploaded to RT?',
                       'Uploaded to YT?', 'Post Type', 'Delivery Date [Automation]', 'Event Programming', 'Show Name', 'Game Name']
        self.formula = {'Status': 'Live (Archive)'}
        self.results = None
        self.productions = []
        self.members = []
        self.productions_df = None
        self.members_df = None


    def get_records(self):
        self.loggerv3.info('Getting records from Airtable')
        self.results = self.airtable.get_all_records(self.fields, self.formula)


    def build_productions(self):
        self.loggerv3.info(f'Building {self.table_name} data structure')
        for result in self.results:
            result_dict = {'production_id': result['id']}
            for field, data in result['fields'].items():
                key = field.strip().lower().replace(' ','_').replace('-', '_').replace('[', '').replace(']', '').replace('?', '').replace('"', '')
                result_dict[key] = set(data) if isinstance(data, list) else data

            self.productions.append(result_dict)


    def build_members(self):
        self.loggerv3.info(f'Building {self.dim_table_name} data structure')
        for production in self.productions:
            self.normalize_data('producer', production)
            self.normalize_data('editor', production)
            self.normalize_data('cast', production)
            self.normalize_data('guests', production)


    def normalize_data(self, key, production):
        if key in production:
            for item in production[key]:
                data = {
                    'production_id': production['production_id'],
                    'member_id': item,
                    'role': key
                }
                self.members.append(data)


    def build_dataframes(self):
        self.loggerv3.info('Building dataframes')
        self.productions_df = pd.DataFrame(self.productions)
        self.members_df = pd.DataFrame(self.members)

        self.productions_df.drop('producer', inplace=True, axis=1)
        self.productions_df.drop('editor', inplace=True, axis=1)
        self.productions_df.drop('cast', inplace=True, axis=1)
        self.productions_df.drop('guests', inplace=True, axis=1)


    def truncate_tables(self):
        self.loggerv3.info('Truncating tables')
        self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.table_name};")
        self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.dim_table_name};")


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Redshift")
        self.db_connector.write_to_sql(self.productions_df, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)

        self.db_connector.write_to_sql(self.members_df, self.dim_table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.dim_table_name, schema=self.schema)


    def execute(self):
        self.loggerv3.start(f'Running Airtable Funhaus Job')
        self.get_records()
        self.build_productions()
        self.build_members()
        self.build_dataframes()
        self.truncate_tables()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
