import pandas as pd
from base.exceptions import DuplicateSupportingCastEpisodeException
from base.etl_jobv3 import EtlJobV3
from io import StringIO
from utils.connectors.supporting_cast_api_connector import SupportingCastApiConnector
from utils.components.sql_helper import SqlHelper


class SupportingCastEpisodesJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector=None, file_location=''):
        super().__init__(target_date = target_date, jobname =  __name__, db_connector = db_connector, table_name = 'supporting_cast_episodes')
        self.supporting_cast_api_connector = SupportingCastApiConnector(file_location=self.file_location)
        self.sql_helper = SqlHelper()
        self.export_endpoint = 'episode_downloads'
        self.records_df = None
        self.rt_users = None
        self.sc_users = None
        self.rt_users_df = None
        self.sc_users_df = None
        self.final_dataframe = None
        self.min_air_dates_df = None


    def request_data_from_url(self):
        self.loggerv3.info('Requesting data from Supporting Cast')
        response = self.supporting_cast_api_connector.make_single_day_request(endpoint=self.export_endpoint, target_date=self.target_date)
        self.records_df = pd.read_csv(StringIO(response['data']), sep=",")


    def clean_record_attributes(self):
        self.loggerv3.info('Cleaning record attributes')
        self.records_df.drop('Ip', axis=1, inplace=True)
        self.records_df.drop('External Id', axis=1, inplace=True)
        self.records_df.rename(columns={'Date': 'download_datetime',
                                        'Podcast Name': 'podcast',
                                        'Podcast Id': 'podcast_id',
                                        'Feed Id': 'feed_id',
                                        'Episode Id': 'episode_id',
                                        'Episode Title': 'episode_title',
                                        'Email': 'email',
                                        'User Agent': 'user_agent'
                                        },
                               inplace=True)
        self.records_df['email'] = self.records_df['email'].str.lower()
        self.records_df['episode_title'] = self.records_df['episode_title'].str.strip()
        self.records_df['podcast_clean'] = self.records_df['podcast'].str.replace('\(FIRST Member Early Access\)', '').str.strip()


    def check_for_duplicates(self):
        self.loggerv3.info('Checking for duplicates')
        grouped_records_df = self.records_df.groupby(['email', 'download_datetime', 'episode_id']).size().reset_index(name='count')
        duplicate_records_df = grouped_records_df[grouped_records_df['count'] > 1]
        if len(duplicate_records_df.index) > 0:
            raise DuplicateSupportingCastEpisodeException()


    def get_sc_users(self):
        self.loggerv3.info('Getting Supporting Cast users')
        self.sc_users = []
        results = self.db_connector.read_redshift("""SELECT sc_id, rt_uuid
                                                      FROM warehouse.supporting_cast_members
                                                      WHERE rt_uuid is not null;
                                                   """)
        for result in results:
            self.sc_users.append({
                'sc_id': result[0],
                'rt_uuid': result[1],
            })


    def get_rt_users(self):
        self.loggerv3.info('Getting RT users')
        self.rt_users = []
        results = self.db_connector.query_v2_db(f"""SELECT u.uuid, u.email, u.id
                                                    FROM production.users u;
                                                """)
        for result in results:
            self.rt_users.append({
                'rt_uuid': result[0],
                'email': result[1].lower(),
                'rt_id': result[2]
            })


    def merge_records_to_rt_users(self):
        self.loggerv3.info('Merging records to RT users')
        self.rt_users_df = pd.DataFrame(self.rt_users, index=None)
        self.final_dataframe = self.records_df.merge(self.rt_users_df, on='email', how='left')


    def merge_records_to_sc_users(self):
        self.loggerv3.info('Merging records to Supporting Cast users')
        self.sc_users_df = pd.DataFrame(self.sc_users, index=None)
        self.final_dataframe = self.final_dataframe.merge(self.sc_users_df, on='rt_uuid', how='left')


    def get_min_air_date(self):
        self.loggerv3.info('Getting min air date')
        min_air_dates = []
        results = self.db_connector.read_redshift("""SELECT 
                                                         title, 
                                                         clean_title,
                                                         cast(min(pub_date) as timestamp) as air_date
                                                      FROM warehouse.dim_megaphone_episode
                                                      GROUP BY 1, 2;
                                                   """)
        for result in results:
            min_air_dates.append({
                'episode_title': result[0].strip() if result[0] is not None else None,
                'clean_title': result[1].strip() if result[1] is not None else None,
                'air_date': result[2]
            })
        self.min_air_dates_df = pd.DataFrame(min_air_dates)


    def merge_min_air_date(self):
        self.loggerv3.info('Merging records to min air date')
        self.final_dataframe = self.final_dataframe.merge(self.min_air_dates_df[['episode_title', 'air_date']], on='episode_title', how='left')
        self.final_dataframe = self.final_dataframe.merge(self.min_air_dates_df[['clean_title', 'air_date']], left_on='episode_title', right_on='clean_title', how='left')


    def coalesce_air_date(self):
        self.loggerv3.info('Coalescing air dates')
        self.final_dataframe['air_date'] = self.final_dataframe['air_date_x'].mask(pd.isnull, self.final_dataframe['air_date_y'])
        self.final_dataframe.drop('air_date_x', axis=1, inplace=True)
        self.final_dataframe.drop('air_date_y', axis=1, inplace=True)
        self.final_dataframe.drop('clean_title', axis=1, inplace=True)


    def clean_final_dataframe(self):
        self.loggerv3.info('Cleaning final dataframe')
        self.final_dataframe['sc_id'] = self.final_dataframe['sc_id'].astype(pd.Int32Dtype())
        self.final_dataframe['rt_id'] = self.final_dataframe['rt_id'].astype(pd.Int32Dtype())
        self.final_dataframe['target_date'] = self.target_date
        self.final_dataframe.drop('email', axis=1, inplace=True)
        self.final_dataframe = self.final_dataframe.rename(columns={'Subscription Plan': 'subscription_plan'})


    def write_all_results_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Supporting Cast Episodes Job {self.target_date}")
        self.request_data_from_url()
        self.clean_record_attributes()
        self.check_for_duplicates()
        self.get_sc_users()
        self.get_rt_users()
        self.merge_records_to_rt_users()
        self.merge_records_to_sc_users()
        self.get_min_air_date()
        self.merge_min_air_date()
        self.coalesce_air_date()
        self.clean_final_dataframe()
        self.write_all_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
