from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd
from utils.connectors.airtable_api_connector import AirtableApiConnector



class AirtableCancelSurveyJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='cancel_survey_responses', local_mode=True)
        self.staging_schema = 'staging'
        self.prod_schema = 'airtable'
        self.airtable_base = 'cancel_survey'
        self.airtable_name = 'submissions'
        self.airtable = AirtableApiConnector(file_location=self.file_location, airtable_base=self.airtable_base, airtable_name=self.airtable_name)
        self.fields = ['UUID', 'Plan', 'In Trial', 'Offer Displayed', 'Outcome', 'Reason', 'Response', 'Date']
        self.records = []
        self.cleaned_records = []
        self.final_dataframe = None


    def get_records(self):
        self.loggerv3.info('Getting records from Airtable')
        self.records = self.airtable.get_all_records(self.fields)


    def clean_records(self):
        self.loggerv3.info('Cleaning records')
        for result in self.records:
            result_dict = {}
            for field, data in result['fields'].items():
                key = field.strip().lower().replace(' ','_').replace('-', '_').replace('[', '').replace(']', '').replace('?', '').replace('"', '')
                result_dict[key] = set(data) if isinstance(data, list) else data

            self.cleaned_records.append(result_dict)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.cleaned_records)
        self.final_dataframe['date'] = pd.to_datetime(self.final_dataframe['date'])
        self.final_dataframe['response'] = self.final_dataframe['response'].str.replace('\n', ' ')
        self.final_dataframe['response'] = self.final_dataframe['response'].str[:256]
        self.final_dataframe = self.final_dataframe[['uuid', 'plan', 'in_trial', 'offer_displayed', 'outcome', 'reason', 'response', 'date']]


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(
            self.final_dataframe, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append'
        )
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
               BEGIN TRANSACTION;

                   UPDATE {self.prod_schema}.{self.table_name}
                   SET
                        plan = staging.plan,
                        in_trial = staging.in_trial,
                        offer_displayed = staging.offer_displayed,
                        outcome = staging.outcome,
                        reason = staging.reason, 
                        response = staging.response,
                        date = staging.date
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.uuid = prod.uuid;

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                        prod.uuid = stage_{self.table_name}.uuid;

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)



    def execute(self):
        self.loggerv3.start(f'Running Airtable Cancel Survey Job')
        self.get_records()
        self.clean_records()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
