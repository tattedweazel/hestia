from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd
from utils.connectors.airtable_api_connector import AirtableApiConnector



class AirtableTeamMembersJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='dim_team_members')
        """ Retrieves data from Airtable and creates a dim table of all member ids and names. """

        self.schema = 'airtable'
        self.airtable_base = 'rooster_teeth_hq'
        self.airtable_name = 'team_members'
        self.airtable = AirtableApiConnector(file_location=self.file_location, airtable_base=self.airtable_base, airtable_name=self.airtable_name)
        self.fields = ['Name']
        # self.formula = {'Department': 'Funhaus'}
        self.formula = {}
        self.results = None
        self.records = []
        self.final_dataframe = None


    def get_records(self):
        self.loggerv3.info('Getting records from Airtable')
        self.results = self.airtable.get_all_records(self.fields, self.formula)


    def build_dataframe(self):
        self.loggerv3.info('Building dataframe')
        for result in self.results:
            result_dict = {'member_id': result['id']}
            for field, data in result['fields'].items():
                key = field.strip().lower().replace(' ','_').replace('[', '').replace(']', '').replace('?', '')
                result_dict[key] = data
            self.records.append(result_dict)

        self.final_dataframe = pd.DataFrame(self.records)


    def truncate_tables(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.table_name};")


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')


    def execute(self):
        self.loggerv3.start(f'Running Airtable Team Members Job')
        self.get_records()
        self.build_dataframe()
        self.truncate_tables()
        self.write_results_to_redshift()
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)
        self.loggerv3.success("All Processing Complete!")
