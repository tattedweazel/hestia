from base.etl_jobv3 import EtlJobV3
from rapidfuzz import fuzz, process
from pandas import pandas as pd
import numpy as np


class AirtableFunhausCastMapperJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='funhaus_cast_map')
        self.schema = 'warehouse'
        self.EXTRACT_LIMIT = 1
        self.MATCH_THRESHOLD = 95
        self.dim_segment_episodes = []
        self.dim_segment_episodes_df = None
        self.funhaus_productions_df = None
        self.joined_episodes_productions_df = None
        self.dim_segment_episodes_df_filtered = None
        self.cast_members_df = None
        self.final_dataframe = None


    def get_dim_segment_episodes(self):
        self.loggerv3.info('Getting dim episodes')
        self.dim_segment_episodes = []
        query = """
        SELECT
            episode_key,
            episode_title,
            season_title,
            series_title
        FROM warehouse.dim_segment_episode
        WHERE 
            channel_title = 'Funhaus' AND 
            air_date >= '2021-02-01' AND 
            series_title not in ('Wrestling With The Week', 'Inside Gaming Roundup') AND 
            episode_title not like '%%(UNFILTERED)%%' AND
            episode_title not like '%%(UNCUT)%%';
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.dim_segment_episodes.append({
                'episode_key': result[0],
                'episode_title': result[1],
                'season_title': result[2],
                'series_title': result[3]
            })

        self.dim_segment_episodes_df = pd.DataFrame(self.dim_segment_episodes)


    def get_funhaus_productions(self):
        self.loggerv3.info('Getting funhaus productions')
        funhaus_productions = []
        query = """
        SELECT
            production_id,
            coalesce(wave_1_title, production) as production_title
        FROM airtable.funhaus_productions
        WHERE 
            rt_launch is NOT NULL AND
            production_id != 'reciBnUbBibj3ZP9M';
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            funhaus_productions.append({
                'production_id': result[0],
                'production_title': result[1]
            })

        self.funhaus_productions_df = pd.DataFrame(funhaus_productions)


    def fuzzy_map_episodes(self):
        self.loggerv3.info('Fuzzy mapping episodes by title')
        funhaus_titles = self.funhaus_productions_df['production_title'].tolist()
        # Creates a list of tuples with (title, accuracy 1-100)
        self.dim_segment_episodes_df['matches1'] = self.dim_segment_episodes_df['episode_title'].apply(lambda x: process.extract(x, funhaus_titles, limit=self.EXTRACT_LIMIT))
        self.dim_segment_episodes_df['matches2'] = self.dim_segment_episodes_df['episode_title'].apply(lambda x: process.extract(x, funhaus_titles, limit=self.EXTRACT_LIMIT, scorer=fuzz.token_set_ratio))
        # Filters tuple list to most accurate
        self.dim_segment_episodes_df['results1'] = self.dim_segment_episodes_df['matches1'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= self.MATCH_THRESHOLD]))
        self.dim_segment_episodes_df['results2'] = self.dim_segment_episodes_df['matches2'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= self.MATCH_THRESHOLD]))
        # Coalesce to Single Match
        self.dim_segment_episodes_df['final_match'] = np.where(self.dim_segment_episodes_df['results1'] == "",
                                                                 self.dim_segment_episodes_df['results2'],
                                                                 self.dim_segment_episodes_df['results1']
                                                                 )
        # Filter any episodes without matches
        self.dim_segment_episodes_df_filtered = self.dim_segment_episodes_df[self.dim_segment_episodes_df['final_match'] != ""]
        # Join with funhaus productions
        self.joined_episodes_productions_df = self.dim_segment_episodes_df_filtered.merge(self.funhaus_productions_df, left_on='final_match', right_on='production_title', how='left')
        self.joined_episodes_productions_df = self.joined_episodes_productions_df[['episode_key', 'episode_title', 'season_title', 'series_title', 'production_id']]


    def get_cast_members(self):
        self.loggerv3.info('Getting cast members')
        cast_members = []
        query = """
        SELECT 
            dpm.production_id,
            dpm.member_id,
            dtm.name,
            dpm.role
        FROM airtable.dim_production_members dpm
        LEFT JOIN airtable.dim_team_members dtm on dtm.member_id = dpm.member_id
        WHERE dtm.name is NOT NULL;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            cast_members.append({
                'production_id': result[0],
                'member_id': result[1],
                'member': result[2],
                'role': result[3]
            })

        self.cast_members_df = pd.DataFrame(cast_members)


    def map_cast_members(self):
        self.loggerv3.info('Mapping cast members')
        self.final_dataframe = self.cast_members_df.merge(self.joined_episodes_productions_df, on='production_id', how='left')
        self.final_dataframe = self.final_dataframe[self.final_dataframe['episode_key'].notnull()]


    def truncate_table(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.table_name};")


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


    def execute(self):
        self.loggerv3.start('Running Airtable Funhaus Cast Mapper Job')
        self.get_dim_segment_episodes()
        self.get_funhaus_productions()
        self.fuzzy_map_episodes()
        self.get_cast_members()
        self.map_cast_members()
        self.truncate_table()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
