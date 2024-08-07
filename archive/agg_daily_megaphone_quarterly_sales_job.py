import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime



class AggDailyMegaphoneQuarterlySalesJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_quarterly_sales_metrics', local_mode=True)
        self.schema = 'warehouse'
        self.run_date = target_date
        self.megaphone_podcasts = []
        self.final_dataframe = None


    def get_megaphone_data(self):
        self.loggerv3.info('Getting megaphone data')
        query = f"""
        WITH cte as (
           SELECT
                (CASE
                    WHEN lower(mp.clean_title) = 'always open' THEN 'always open'
                    WHEN lower(mp.clean_title) = 'f**kface' THEN 'f**kface' 
                    WHEN lower(mp.clean_title) = 'red web' THEN 'red web'
                    WHEN lower(mp.clean_title) = 'face jam' THEN 'face jam'
                    WHEN lower(mp.clean_title) = 'off topic' THEN 'off topic'
                    WHEN lower(mp.clean_title) = 'annual pass' THEN 'annual pass'
                    WHEN lower(mp.clean_title) = 'rooster teeth podcast' THEN 'rooster teeth podcast'
                    WHEN lower(mp.clean_title) = 'black box down' THEN 'black box down'
                    WHEN lower(mp.clean_title) = 'tales from the stinky dragon' THEN 'tales from the stinky dragon'
                    WHEN lower(mp.clean_title) = 'anma' THEN 'anma'
                    WHEN lower(mp.clean_title) = 'funhaus podcast' THEN 'funhaus podcast'
                    WHEN lower(mp.clean_title) = '30 morbid minutes' THEN '30 morbid minutes'
                    WHEN lower(mp.clean_title) = 'ship hits the fan' THEN 'ship hits the fan'
                    WHEN lower(mp.clean_title) = 'must be dice' THEN 'must be dice' 
                    WHEN lower(mp.clean_title) = 'death battle cast' THEN 'death battle cast'
                    WHEN lower(mp.clean_title) = 'beyondthepine' THEN 'beyond the pine'
                    WHEN lower(mp.clean_title) = 'comics experiment' THEN 'comics experiment'
                    WHEN lower(mp.clean_title) = 'iilluminaughtii' AND lower(me.title) LIKE '%%corporate casket%%' THEN 'iilluminaughtii corporate casket'
                    WHEN lower(mp.clean_title) = 'iilluminaughtii' AND lower(me.title) LIKE '%%dark dives%%' THEN 'iilluminaughtii dark dives'
                    WHEN lower(mp.clean_title) = 'dead meat podcast' THEN 'dead meat podcast' 
                    WHEN lower(mp.clean_title) = 'double toasted podcast' THEN 'double toasted podcast'
                    WHEN lower(mp.clean_title) = 'the easy allies podcast' THEN 'easy allies'
                    WHEN lower(mp.clean_title) = 'the eavesdrop podcast' THEN 'eavesdrop podcast'
                    WHEN lower(mp.clean_title) = 'foundflix' THEN 'ending explained found flix'
                    WHEN lower(mp.clean_title) = 'h3 podcast' AND lower(me.title) LIKE '%%off the rails%%' THEN 'h3 podcast off the rails'
                    WHEN lower(mp.clean_title) = 'h3 podcast' AND lower(me.title) LIKE '%%after dark%%' THEN 'h3 podcast after dark'
                    WHEN lower(mp.clean_title) = 'h3 podcast' AND lower(me.title) LIKE '%%h3tv%%' THEN 'h3 podcast h3tv'
                    WHEN lower(mp.clean_title) = 'h3 podcast' AND lower(me.title) LIKE '%%leftovers%%' THEN 'h3 podcast leftovers'
                    WHEN lower(mp.clean_title) = 'howie mandel does stuff podcast' THEN 'howie mandel does stuff'
                    WHEN lower(mp.clean_title) = 'i spent a day with...' THEN 'i spent a day with'
                    WHEN lower(mp.clean_title) = 'internet today' AND lower(me.title) LIKE '%%itdaily%%' THEN 'internet today daily'
                    WHEN lower(mp.clean_title) = 'super carlin brothers' AND (lower(me.title) LIKE '%%j vs ben%%' OR lower(me.title) LIKE '%%j vs. ben%%') THEN 'j vs ben'
                    WHEN lower(mp.clean_title) = 'super carlin brothers' AND lower(me.title) NOT LIKE '%%j vs ben%%' AND lower(me.title) NOT LIKE '%%j vs. ben%%' THEN 'super carlin bros theories'
                    WHEN lower(mp.clean_title) = 'jeff fm' THEN 'jeff fm'
                    WHEN lower(mp.clean_title) = 'jon solo''s messed up origins™ podcast' THEN 'jon solo messed up origins'
                    WHEN lower(mp.clean_title) = 'in review: movies ranked, reviewed, & recapped – a kinda funny film & tv podcast'THEN 'kinda funny'
                    WHEN lower(mp.clean_title) = 'kinda funny gamescast: video game podcast' THEN 'kinda funny'
                    WHEN lower(mp.clean_title) = 'kinda funny games daily: video games news podcast' THEN 'kinda funny'
                    WHEN lower(mp.clean_title) = 'kinda funny xcast: xbox podcast' THEN 'kinda funny'
                    WHEN lower(mp.clean_title) = 'ps i love you xoxo: playstation podcast by kinda funny' THEN 'kinda funny'
                    WHEN lower(mp.clean_title) = 'the kinda funny podcast' THEN 'kinda funny'
                    WHEN lower(mp.clean_title) = 'lew later' THEN 'lew later'
                    WHEN lower(mp.clean_title) = 'neebscast' THEN 'neebscast'
                    WHEN lower(mp.clean_title) = 'nostalgia critic' THEN 'nostalgia critic'
                    WHEN lower(mp.clean_title) = 'iilluminaughtii' AND lower(me.title) LIKE '%%multi level mondays%%' THEN 'iilluminaughtii multi level mondays'
                    WHEN lower(mp.clean_title) = 'internet today' AND lower(me.title) LIKE '%%news dump%%' THEN 'internet today news dump'
                    WHEN lower(mp.clean_title) = 'justkiddingnews-offtherecord' THEN 'just kidding news off the record'
                    WHEN lower(mp.clean_title) = 'the opposite twins' THEN 'opposite twins'
                    WHEN lower(mp.clean_title) = 'optic podcast' THEN 'optic podcast'
                    WHEN lower(mp.clean_title) = 'peer to peer' THEN 'peer to peer'
                    WHEN lower(mp.clean_title) = 'pine park after dark' THEN 'pine park after dark'
                    WHEN lower(mp.clean_title) = 'primm''s hood cinema' THEN 'primms hood cinema'
                    WHEN lower(mp.clean_title) = 'podcast but outside' THEN 'podcast but outside'
                    WHEN lower(mp.clean_title) = 'internet today' AND lower(me.title) LIKE '%%technewsday%%' THEN 'internet today tech newsday'
                    WHEN lower(mp.clean_title) = 'recreyo' THEN 'recreyo'
                    WHEN lower(mp.clean_title) = 'the salem tovar podcast' THEN 'salem tovar podcast'
                    WHEN lower(mp.clean_title) = 'the take' THEN 'the take'
                    WHEN lower(mp.clean_title) = 'the yard' AND lower(me.title) LIKE '%%ep.%%' THEN 'the yard'
                    WHEN lower(mp.clean_title) = 'the valleycast' THEN 'valley cast'
                    WHEN lower(mp.clean_title) = 'this might get weird' THEN 'this might get weird' 
                    WHEN lower(mp.clean_title) = 'weekly weird news' THEN 'weekly weird news'
                    WHEN lower(mp.clean_title) = 'what''s good games: a video game podcast' AND (lower(me.title) LIKE '%%ep.%%' OR lower(me.title) LIKE '%%episode%%') THEN 'whats good games'
                    WHEN lower(mp.clean_title) = 'this past weekend' THEN 'this past weekend'
                    ELSE 'none'
                END) as series_title,
                mp.clean_title,
                me.title as episode_title,
                min(cast(me.pub_date as timestamp)) as air_date
            FROM warehouse.megaphone_metrics mm
            LEFT JOIN warehouse.dim_megaphone_podcast mp ON mm.podcast_id = mp.id
            LEFT JOIN warehouse.dim_megaphone_episode me ON mm.episode_id = me.id
            WHERE me.episode_type != 'trailer'
                 AND me.podcast_title NOT LIKE '%%(FIRST Member Early Access)%%'
            GROUP BY 1, 2, 3
        )
        SELECT
            lower(cte.series_title) as series_title,
            me.title as episode_title,
            (CASE
                WHEN mm.created_at < cte.air_date THEN cast(cte.air_date as varchar(10))
                ELSE cast(mm.created_at as varchar(10))
            END) as viewership_date,
            cast(cte.air_date as varchar(10)) as air_date,
            count(*) as views
        FROM warehouse.megaphone_metrics mm
        LEFT JOIN warehouse.dim_megaphone_podcast mp ON mm.podcast_id = mp.id
        LEFT JOIN warehouse.dim_megaphone_episode me ON mm.episode_id = me.id
        LEFT JOIN cte ON cte.episode_title = me.title AND cte.clean_title = mp.clean_title
        WHERE
            cte.series_title != 'none'
            AND mm.seconds_downloaded >= 30
            AND cte.air_date IS NOT NULL
            AND me.podcast_title IS NOT NULL
            AND me.title NOT IN (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes)    
            AND me.podcast_title NOT LIKE '%%(FIRST Member Early Access)%%'
        GROUP BY 1, 2, 3, 4;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            days_since_pub = (datetime.strptime(result[2], '%Y-%m-%d') - datetime.strptime(result[3], '%Y-%m-%d')).days
            if days_since_pub <= 45:
                self.megaphone_podcasts.append({
                    'series_title': result[0],
                    'episode_title': result[1],
                    'viewership_date': result[2],
                    'air_date': result[3],
                    'platform': 'megaphone',
                    'days_since_pub': days_since_pub,
                    'views': result[4]
                })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.megaphone_podcasts)
        self.final_dataframe['run_date'] = self.run_date


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def execute(self):
        self.loggerv3.start(f"Running Agg Daily Megaphone Quarterly Sales Job for Run Date {self.run_date}")
        self.get_megaphone_data()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
