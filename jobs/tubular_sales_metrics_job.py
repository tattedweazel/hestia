import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime
from utils.components.sql_helper import SqlHelper



class TubularSalesMetricsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_sales_metrics', local_mode=True)
        self.staging_schema = 'staging'
        self.prod_schema = 'warehouse'
        self.run_date = target_date
        self.sql_helper = SqlHelper()
        self.LOOKBACK = 120
        self.tubular_podcasts = []
        self.final_dataframe = None


    def get_tubular_data(self):
        self.loggerv3.info('Getting tubular data')

        query = f"""
        WITH cte as (
            SELECT
                (CASE
                    WHEN lower(dim.upload_creator) = 'hypothetical nonsense' THEN 'hypothetical nonsense'
                    WHEN (lower(dim.upload_creator) = 'rooster teeth trailers' OR lower(dim.upload_creator) = 'rooster teeth podcast') AND lower(dim.video_title) LIKE '%%| rooster teeth podcast' THEN 'rooster teeth podcast'
                    WHEN lower(dim.upload_creator) = 'all good no worries' AND lower(dim.video_title) LIKE '%%always open%%' THEN 'always open'
                    WHEN lower(dim.upload_creator) = 'all good no worries' AND lower(dim.video_title) LIKE '%%please be nice to me%%' THEN 'please be nice to me'
                    WHEN lower(dim.upload_creator) = 'tales from the stinky dragon' AND date_part('dow', cast(dim.date_of_upload as date)) = 2 THEN 'tales from the stinky dragon'
                    WHEN lower(dim.upload_creator) = 'letsplay' AND lower(dim.video_title) LIKE '%%regulation gameplay%%' THEN 'f**kface regulation gameplay'
                    WHEN lower(dim.upload_creator) = 'f**kface' AND dim.video_title LIKE '%%//%%' AND lower(dim.video_title) NOT LIKE '%%f**kface breaks shit%%' THEN 'f**kface'
                    WHEN lower(dim.upload_creator) = 'f**kface' AND lower(dim.video_title) LIKE '%%does it do%%' THEN 'does it do'
                    WHEN lower(dim.upload_creator) = 'black box down' AND lower(dim.video_title) NOT LIKE '%%black box down explained%%' THEN 'black box down'
                    WHEN lower(dim.upload_creator) = 'achievement hunter' AND (lower(dim.video_title) LIKE '%%let''s roll%%' OR lower(dim.video_title) LIKE '%%let’s roll%%' OR date_part('dow', cast(dim.date_of_upload as date)) = 5) THEN 'lets roll'
                    WHEN lower(dim.upload_creator) = 'red web' AND lower(dim.video_title) LIKE '%%|%%' AND lower(dim.video_title) NOT LIKE '%%red web case files%%' THEN 'red web'
                    WHEN lower(dim.upload_creator) = 'face jam' AND (lower(dim.video_title) LIKE '%%| face jam' OR lower(dim.video_title) LIKE '%%spittin silly%%') AND lower(dim.video_title) NOT LIKE '%%face jam shorts%%' AND lower(dim.video_title) NOT LIKE '%%face jam live shopping event%%' AND lower(dim.video_title) NOT LIKE '%%truck’d up!%%' THEN 'face jam' 
                    WHEN lower(dim.upload_creator) = 'achievement hunter' AND lower(dim.video_title) LIKE '%%- off topic -%%' THEN 'off topic'
                    WHEN lower(dim.upload_creator) = 'annual pass' AND lower(dim.video_title) LIKE '%%annual pass podcast%%' THEN 'annual pass'
                    WHEN lower(dim.upload_creator) = 'funhaus' AND lower(dim.video_title) NOT LIKE '%%is live%%' AND lower(dim.video_title) NOT LIKE '%%must be dice%%' AND date_part('dow', cast(dim.date_of_upload as date)) = 1  THEN 'funhaus mondays' 
                    WHEN lower(dim.upload_creator) = 'funhaus' AND date_part('dow', cast(dim.date_of_upload as date)) IN (0, 3, 5) AND dim.duration_seconds < 6000 AND lower(dim.video_title) NOT LIKE '%%board as hell%%' THEN 'funhaus sundays or fridays'
                    WHEN lower(dim.video_title) LIKE '%%funhaus podcast%%' THEN 'funhaus podcast'
                    WHEN (lower(dim.video_title) LIKE '%%must be dice%%' OR lower(dim.video_title) LIKE '%%paradise path rpg%%' OR lower(dim.video_title) LIKE '%%super princess rescue quest%%') AND lower(dim.video_title) NOT LIKE '%%community haus party%%' THEN 'must be dice'
                    WHEN lower(dim.upload_creator) = 'funhaus' AND (lower(dim.video_title) LIKE '%%board game%%' OR lower(dim.video_title) LIKE '%%board as hell%%') THEN 'board as hell'
                    WHEN lower(dim.upload_creator) = 'inside gaming' AND (date_part('dow', cast(dim.date_of_upload as date)) = 3 OR date_part('dow', cast(dim.date_of_upload as date)) = 5) THEN 'inside gaming roundup'
                    WHEN lower(dim.upload_creator) = 'death battle!' AND lower(dim.video_title) LIKE '%%death battle cast%%' THEN 'death battle cast'
                    WHEN lower(dim.upload_creator) = 'death battle!' AND date_part('dow', cast(date_of_upload as date)) IN (1,3) AND video_title NOT LIKE '%%| DEATH BATTLE!%%' AND lower(dim.video_title) NOT LIKE '%%bloopers%%' THEN 'death battle preview'
                    WHEN lower(dim.upload_creator) = 'death battle!' AND lower(dim.video_title) LIKE '%%| death battle!%%' AND lower(dim.video_title) NOT LIKE '%%bloopers%%' THEN 'death battle'
                    WHEN lower(dim.upload_creator) = 'sugar pine 7' AND lower(dim.video_title) LIKE '%%beyond the pine%%' THEN 'beyond the pine'
                    WHEN lower(dim.upload_creator) = 'comicstorian' AND lower(dim.video_title) LIKE '%%complete story%%' THEN 'complete story'
                    WHEN lower(dim.upload_creator) = 'cox n crendor Podcast' THEN 'cox n crendor'
                    WHEN lower(dim.upload_creator) = 'dead meat' AND lower(dim.video_title) LIKE '%%dead meat podcast%%' THEN 'dead meat podcast'
                    WHEN lower(dim.upload_creator) = 'double toasted' AND lower(dim.video_title) NOT LIKE '%%we''re live!%%' THEN 'double toasted podcast'
                    WHEN lower(dim.upload_creator) = 'easy allies' AND lower(dim.video_title) LIKE '%%easy allies podcast%%' AND lower(dim.video_title) NOT LIKE '%%ad free%%' AND lower(dim.video_title) NOT LIKE '%%ad-free%%' THEN 'easy allies'
                    WHEN dim.upload_creator = 'HECZ' AND lower(dim.video_title) LIKE '%%eavesdrop%%' THEN 'eavesdrop podcast'
                    WHEN lower(dim.upload_creator) = 'foundflix' AND lower(dim.video_title) LIKE '%%ending explained%%' THEN 'ending explained found flix'
                    WHEN lower(dim.upload_creator) = 'h3 podcast' AND lower(dim.video_title) LIKE '%%off the rails%%' THEN 'h3 podcast off the rails'
                    WHEN lower(dim.upload_creator) = 'h3 podcast' AND lower(dim.video_title) LIKE '%%after dark%%' THEN 'h3 podcast after dark'
                    WHEN lower(dim.upload_creator) = 'h3 podcast' AND lower(dim.video_title) LIKE '%%h3tv%%' THEN 'h3 podcast h3tv'
                    WHEN lower(dim.upload_creator) = 'h3 podcast' AND lower(dim.video_title) LIKE '%%leftovers%%' THEN 'h3 podcast leftovers'
                    WHEN lower(dim.upload_creator) = 'howie mandel does stuff' THEN 'howie mandel does stuff'
                    WHEN lower(dim.upload_creator) = 'anthonypadilla' AND lower(dim.video_title) LIKE '%%i spent a day with%%' THEN 'i spent a day with'
                    WHEN lower(dim.upload_creator) = 'internet today' AND lower(dim.video_title) NOT LIKE '%%news dump%%' AND lower(dim.video_title) NOT LIKE '%%technewsday%%' AND lower(dim.video_title) NOT LIKE '%%weekly weird news%%' THEN 'internet today daily'
                    WHEN lower(dim.upload_creator) = 'supercarlinbrothers' AND (lower(dim.video_title) LIKE '%%j vs ben%%' OR lower(dim.video_title) LIKE '%%j vs. ben%%') THEN 'j vs ben'
                    WHEN lower(dim.upload_creator) = 'jeff fm' AND lower(dim.video_title) LIKE '%%jeff fm%%' THEN 'jeff fm'
                    WHEN lower(dim.upload_creator) = 'kinda funny' AND lower(dim.video_title) NOT LIKE '%%ad-free%%' AND lower(dim.video_title) NOT LIKE '%%ad free%%' THEN 'kinda funny'
                    WHEN lower(dim.upload_creator) = 'kinda funny games' AND lower(dim.video_title) NOT LIKE '%%ad-free%%' AND lower(dim.video_title) NOT LIKE '%%ad free%%' THEN 'kinda funny'
                    WHEN lower(dim.upload_creator) = 'neebs gaming' AND lower(dim.video_title) LIKE '%%neebscast%%' THEN 'neebscast'
                    WHEN lower(dim.upload_creator) = 'internet today' AND lower(dim.video_title) LIKE '%%news dump%%' THEN 'internet today news dump'
                    WHEN lower(dim.upload_creator) = 'channel awesome' AND lower(dim.video_title) LIKE '%%nostalgia critic%%' THEN 'nostalgia critic'
                    WHEN lower(dim.upload_creator) = 'justkiddingnews' AND lower(dim.video_title) LIKE '%%off the record:%%' THEN 'just kidding news off the record'
                    WHEN lower(dim.upload_creator) = 'niki and gabi' THEN 'opposite twins'
                    WHEN lower(dim.upload_creator) = 'optic audio network' AND lower(dim.video_title) LIKE '%%optic podcast%%' THEN 'optic podcast'
                    WHEN lower(dim.upload_creator) = 'peer to peer podcast' THEN 'peer to peer'
                    WHEN lower(dim.upload_creator) = 'pine park' THEN 'pine park after dark'
                    WHEN lower(dim.upload_creator) = 'podcast but outside' THEN 'podcast but outside'
                    WHEN lower(dim.upload_creator) = 'primm''s hood cinema' AND lower(dim.video_title) LIKE '%%primm''s hood cinema%%' THEN 'primms hood cinema'
                    WHEN lower(dim.upload_creator) = 'recreyo' THEN 'recreyo'
                    WHEN lower(dim.upload_creator) = 'supercarlinbrothers' AND lower(dim.video_title) NOT LIKE '%%j vs ben%%' AND lower(dim.video_title) NOT LIKE '%%j vs. ben%%' THEN 'super carlin bros theories'
                    WHEN lower(dim.upload_creator) = 'internet today' AND lower(dim.video_title) LIKE '%%technewsday%%' THEN 'internet today tech newsday'
                    WHEN lower(dim.upload_creator) = 'salem tovar' THEN 'salem tovar podcast'
                    WHEN lower(dim.upload_creator) = 'the sip with ryland adams and lizze gordon' THEN 'the sip'
                    WHEN lower(dim.upload_creator) = 'the take' THEN 'the take'
                    WHEN lower(dim.upload_creator) = 'the yard' AND lower(dim.video_title) LIKE '%%the yard%%' THEN 'the yard'
                    WHEN lower(dim.upload_creator) = 'this might get' AND lower(dim.video_title) LIKE '%%tmgw%%' THEN 'this might get weird'
                    WHEN lower(dim.upload_creator) = 'triforce!' THEN 'triforce'
                    WHEN lower(dim.upload_creator) = 'the valleyfolk' THEN 'valley cast'
                    WHEN lower(dim.upload_creator) = 'internet today' AND lower(dim.video_title) LIKE '%%weekly weird news%%' THEN 'weekly weird news'
                    WHEN lower(dim.upload_creator) = 'what''s good games' AND (lower(dim.video_title) LIKE '%%ep.%%' OR lower(dim.video_title) LIKE '%%episode%%') THEN 'whats good games'
                    WHEN lower(dim.upload_creator) = 'theo von' AND lower(dim.video_title) LIKE '%%this past weekend%%' AND lower(dim.video_title) LIKE '%%theo von%%' THEN 'this past weekend'
                    WHEN lower(dim.upload_creator) = 'what''s good podcast' AND dim.duration_seconds >= 3600 THEN 'whats good podcast'
                    WHEN lower(dim.upload_creator) = 'smosh pit' AND lower(dim.video_title) LIKE '%%reading reddit stories%%' THEN 'smosh pit reading reddit stories'
                    WHEN lower(dim.upload_creator) = 'suburb talks' THEN 'suburb talks'
                    ELSE 'none'
                END) as series_title,
                video_title as episode_title,
                dim.video_id as episode_id,
                dim.duration_seconds,
                cast(date_add('days', m.date_value, cast(dim.date_of_upload as date)) as varchar(10)) as viewership_date,
                cast(dim.date_of_upload as varchar(10)) as air_date,
                m.views
            FROM warehouse.daily_tubular_metrics_v2 m
            LEFT JOIN warehouse.dim_tubular_videos dim on dim.video_id = m.video_id
            WHERE 
                 m.agg_type = 'daily'
                 AND dim.video_title NOT LIKE '%%!PODCAST!%%'
                 AND dim.video_title NOT LIKE '%%!LIVE GAMEPLAY!%%'
                 AND dim.duration_seconds > 120
                 AND viewership_date >= current_date - {self.LOOKBACK}

        )
        SELECT
            episode_id,
            series_title,
            episode_title,
            viewership_date,
            air_date,
            sum(views) as views
        FROM cte
        WHERE
            series_title NOT IN ('none', 'death battle', 'death battle preview')
            AND episode_title NOT in (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes) 
            
        GROUP BY 1, 2, 3, 4, 5;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            days_since_pub = (datetime.strptime(result[3], '%Y-%m-%d') - datetime.strptime(result[4], '%Y-%m-%d')).days
            self.tubular_podcasts.append({
                'episode_id': result[0],
                'series_title': result[1],
                'episode_title': result[2],
                'viewership_date': result[3],
                'air_date': result[4],
                'platform': 'youtube',
                'days_since_pub': days_since_pub,
                'views': result[5]
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.tubular_podcasts)
        self.final_dataframe['run_date'] = self.run_date


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(self.final_dataframe, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
               BEGIN TRANSACTION;

                   UPDATE {self.prod_schema}.{self.table_name}
                   SET 
                       views = staging.views,
                       run_date = staging.run_date,
                       episode_title = staging.episode_title
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.episode_id = prod.episode_id
                       AND staging.days_since_pub = prod.days_since_pub
                       AND staging.views != prod.views
                    WHERE prod.platform = 'youtube';

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                       prod.episode_id = stage_{self.table_name}.episode_id
                       AND prod.days_since_pub = stage_{self.table_name}.days_since_pub
                       AND prod.platform = stage_{self.table_name}.platform;

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)



    def execute(self):
        self.loggerv3.start(f"Running Tubular Sales Metrics Job on {self.run_date}")
        self.get_tubular_data()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
