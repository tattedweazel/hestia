import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime
from utils.components.sql_helper import SqlHelper



class AggDailyTubularQuarterlySalesJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_quarterly_sales_metrics', local_mode=True)
        self.schema = 'warehouse'
        self.run_date = target_date
        self.sql_helper = SqlHelper()
        self.tubular_podcasts = []
        self.final_dataframe = None


    def get_tubular_data(self):
        self.loggerv3.info('Getting tubular data')
        add_fh_pod_episodes = ["What''s Got Us So Pissed Off Now?! - Funhaus Fan Q & A",
                               'Tales From the Funhaus Pool Party!',
                               'Are You Ready for More Funhaus Fantasy Roleplaying Games?']
        add_fh_pod_episodes_str = self.sql_helper.array_to_sql_list(add_fh_pod_episodes)
        query = f"""
        WITH cte as (
            SELECT
                (CASE
                    WHEN dim.upload_creator = 'All Good No Worries' AND lower(dim.video_title) LIKE '%%always open%%' THEN 'always open'
                    WHEN dim.upload_creator = 'All Good No Worries' AND lower(dim.video_title) LIKE '%%please be nice to me%%' THEN 'please be nice to me'
                    WHEN dim.upload_creator = 'LetsPlay' AND lower(dim.video_title) LIKE '%%gmod%%' THEN 'ah gmod gameplay'
                    WHEN dim.upload_creator = 'LetsPlay' AND date_part('dow', cast(dim.date_of_upload as date)) = 1 AND dim.video_title LIKE '%%VR%%' THEN 'ah vr mondays'
                    WHEN dim.upload_creator = 'LetsPlay' AND date_part('dow', cast(dim.date_of_upload as date)) = 0 THEN 'ah fan favorites sundays'
                    WHEN dim.upload_creator = 'LetsPlay' AND lower(dim.video_title) LIKE '%%challenge accepted%%' THEN 'challenge accepted'
                    WHEN dim.upload_creator = 'F**KFACE' AND dim.video_title LIKE '%%//%%' AND lower(dim.video_title) NOT LIKE '%%f**kface breaks shit%%' THEN 'f**kface'
                    WHEN dim.upload_creator = 'F**KFACE' AND lower(dim.video_title) LIKE '%%does it do%%' THEN 'does it do'
                    WHEN dim.upload_creator = 'Achievement Hunter' AND (lower(dim.video_title) LIKE '%%let''s roll%%' OR lower(dim.video_title) LIKE '%%let’s roll%%' OR date_part('dow', cast(dim.date_of_upload as date)) = 5) THEN 'lets roll'
                    WHEN dim.upload_creator = 'Red Web' AND lower(dim.video_title) LIKE '%%|%%' AND lower(dim.video_title) NOT LIKE '%%red web case files%%' THEN 'red web'
                    WHEN dim.upload_creator = 'Face Jam' AND lower(dim.video_title) NOT LIKE '%%face jam shorts%%' AND lower(dim.video_title) NOT LIKE '%%face jam live shopping event%%' AND lower(dim.video_title) NOT LIKE '%%truck’d up!%%' THEN 'face jam' 
                    WHEN dim.upload_creator = 'Achievement Hunter' AND lower(dim.video_title) LIKE '%%- off topic -%%' THEN 'off topic'
                    WHEN dim.upload_creator = 'Annual Pass' AND lower(dim.video_title) LIKE '%%annual pass podcast%%' THEN 'annual pass'
                    WHEN dim.upload_creator = 'Rooster Teeth' AND lower(dim.video_title) LIKE '%%rt podcast%%' THEN 'rooster teeth podcast'
                    WHEN dim.upload_creator = 'Black Box Down' AND lower(dim.video_title) NOT LIKE '%%black box down explained%%' THEN 'black box down'
                    WHEN dim.upload_creator = 'Funhaus' AND lower(dim.video_title) NOT LIKE '%%is live%%' AND lower(dim.video_title) NOT LIKE '%%must be dice%%' AND date_part('dow', cast(dim.date_of_upload as date)) = 1  THEN 'funhaus mondays' 
                    WHEN dim.upload_creator = 'Funhaus' AND date_part('dow', cast(dim.date_of_upload as date)) IN (0, 5) AND dim.duration_seconds < 6000 THEN 'funhaus sundays or fridays'
                    WHEN (lower(dim.video_title) LIKE '%%funhaus podcast%%' OR dim.video_title in ({add_fh_pod_episodes_str})) THEN 'funhaus podcast'
                    WHEN ((lower(dim.video_title) LIKE '%%paradise path rpg%%' OR lower(dim.video_title) LIKE '%%super princess rescue quest%%') AND lower(dim.video_title) NOT LIKE '%%community haus party%%') THEN 'must be dice'
                    WHEN dim.upload_creator = 'Inside Gaming' AND date_part('dow', cast(dim.date_of_upload as date)) = 6 THEN 'inside gaming roundup'
                    WHEN dim.upload_creator = 'DEATH BATTLE!' AND lower(dim.video_title) LIKE '%%death battle cast%%' THEN 'death battle cast'
                    WHEN dim.upload_creator = 'DEATH BATTLE!' AND date_part('dow', cast(date_of_upload as date)) IN (1,3) AND video_title NOT LIKE '%%| DEATH BATTLE!%%' THEN 'death battle preview'
                    WHEN dim.upload_creator = 'DEATH BATTLE!' AND lower(dim.video_title) LIKE '%%| death battle!%%' THEN 'death battle'
                    WHEN dim.upload_creator = 'Sugar Pine 7' AND lower(dim.video_title) LIKE '%%beyond the pine%%' THEN 'beyond the pine'
                    WHEN dim.upload_creator = 'Comicstorian' AND lower(dim.video_title) LIKE '%%comics experiment%%' THEN 'comics experiment'
                    WHEN dim.upload_creator = 'Comicstorian' AND lower(dim.video_title) LIKE '%%complete story%%' THEN 'complete story'
                    WHEN dim.upload_creator = 'iilluminaughtii' THEN 'iilluminaughtii corporate casket'
                    WHEN dim.upload_creator = 'Cox n Crendor Podcast' THEN 'cox n crendor'
                    WHEN dim.upload_creator = 'Dead Meat' AND lower(dim.video_title) LIKE '%%dead meat podcast%%' THEN 'dead meat podcast'
                    WHEN dim.upload_creator = 'Double Toasted' AND lower(dim.video_title) NOT LIKE '%%we''re live!%%' THEN 'double toasted podcast'
                    WHEN dim.upload_creator = 'Easy Allies' AND lower(dim.video_title) LIKE '%%easy allies podcast%%' AND lower(dim.video_title) NOT LIKE '%%ad free%%' AND lower(dim.video_title) NOT LIKE '%%ad-free%%' THEN 'easy allies'
                    WHEN dim.upload_creator = 'HECZ' AND lower(dim.video_title) LIKE '%%eavesdrop%%' THEN 'eavesdrop podcast'
                    WHEN dim.upload_creator = 'FoundFlix' AND lower(dim.video_title) LIKE '%%ending explained%%' THEN 'ending explained found flix'
                    WHEN dim.upload_creator = 'H3 Podcast' AND lower(dim.video_title) LIKE '%%off the rails%%' THEN 'h3 podcast off the rails'
                    WHEN dim.upload_creator = 'H3 Podcast' AND lower(dim.video_title) LIKE '%%after dark%%' THEN 'h3 podcast after dark'
                    WHEN dim.upload_creator = 'H3 Podcast' AND lower(dim.video_title) LIKE '%%h3tv%%' THEN 'h3 podcast h3tv'
                    WHEN dim.upload_creator = 'H3 Podcast' AND lower(dim.video_title) LIKE '%%leftovers%%' THEN 'h3 podcast leftovers'
                    WHEN dim.upload_creator = 'Howie Mandel Does Stuff' THEN 'howie mandel does stuff'
                    WHEN dim.upload_creator = 'AnthonyPadilla' AND lower(dim.video_title) LIKE '%%i spent a day with%%' THEN 'i spent a day with'
                    WHEN dim.upload_creator = 'Internet Today' AND lower(dim.video_title) NOT LIKE '%%news dump%%' AND lower(dim.video_title) NOT LIKE '%%technewsday%%' AND lower(dim.video_title) NOT LIKE '%%weekly weird news%%' THEN 'internet today daily'
                    WHEN dim.upload_creator = 'SuperCarlinBrothers' AND (lower(dim.video_title) LIKE '%%j vs ben%%' OR lower(dim.video_title) LIKE '%%j vs. ben%%') THEN 'j vs ben'
                    WHEN dim.upload_creator = 'JEFF FM' AND lower(dim.video_title) LIKE '%%jeff fm%%' THEN 'jeff fm'
                    WHEN dim.upload_creator = 'Jon Solo' AND lower(dim.video_title) LIKE '%%messed up origins%%' THEN 'jon solo messed up origins'
                    WHEN dim.upload_creator = 'Kinda Funny' AND lower(dim.video_title) NOT LIKE '%%ad-free%%' AND lower(dim.video_title) NOT LIKE '%%ad free%%' THEN 'kinda funny'
                    WHEN dim.upload_creator = 'Kinda Funny Games' AND lower(dim.video_title) NOT LIKE '%%ad-free%%' AND lower(dim.video_title) NOT LIKE '%%ad free%%' THEN 'kinda funny'
                    WHEN dim.upload_creator = 'Lew Later' THEN 'lew later'
                    WHEN dim.upload_creator = 'Neebs Gaming' AND lower(dim.video_title) LIKE '%%neebscast%%' THEN 'neebscast'
                    WHEN dim.upload_creator = 'Internet Today' AND lower(dim.video_title) LIKE '%%news dump%%' THEN 'internet today news dump'
                    WHEN dim.upload_creator = 'Channel Awesome' AND lower(dim.video_title) LIKE '%%nostalgia critic%%' THEN 'nostalgia critic'
                    WHEN dim.upload_creator = 'JustKiddingNews' AND lower(dim.video_title) LIKE '%%off the record:%%' THEN 'just kidding news off the record'
                    WHEN dim.upload_creator = 'Niki and Gabi' THEN 'opposite twins'
                    WHEN dim.upload_creator = 'OpTic Audio Network' AND lower(dim.video_title) LIKE '%%optic podcast%%' THEN 'optic podcast'
                    WHEN dim.upload_creator = 'Peer To Peer Podcast' THEN 'peer to peer'
                    WHEN dim.upload_creator = 'Pine Park' AND lower(dim.video_title) LIKE '%%pine park after dark%%' THEN 'pine park after dark'
                    WHEN dim.upload_creator = 'Podcast But Outside' THEN 'podcast but outside'
                    WHEN dim.upload_creator = 'Primm''s Hood Cinema' AND lower(dim.video_title) LIKE '%%primm''s hood cinema%%' THEN 'primms hood cinema'
                    WHEN dim.upload_creator = 'Recreyo' THEN 'recreyo'
                    WHEN dim.upload_creator = 'SuperCarlinBrothers' AND lower(dim.video_title) NOT LIKE '%%j vs ben%%' AND lower(dim.video_title) NOT LIKE '%%j vs. ben%%' THEN 'super carlin bros theories'
                    WHEN dim.upload_creator = 'Internet Today' AND lower(dim.video_title) LIKE '%%technewsday%%' THEN 'internet today tech newsday'
                    WHEN dim.upload_creator = 'Salem Tovar' THEN 'salem tovar podcast'
                    WHEN dim.upload_creator = 'The Sip with Ryland Adams and Lizze Gordon' THEN 'the sip'
                    WHEN dim.upload_creator = 'The Take' THEN 'the take'
                    WHEN dim.upload_creator = 'The Yard' AND lower(dim.video_title) LIKE '%%the yard%%' THEN 'the yard'
                    WHEN dim.upload_creator = 'This Might Get' AND lower(dim.video_title) LIKE '%%tmgw%%' THEN 'this might get weird'
                    WHEN dim.upload_creator = 'Triforce!' THEN 'triforce'
                    WHEN dim.upload_creator = 'The Valleyfolk' THEN 'valley cast'
                    WHEN dim.upload_creator = 'Internet Today' AND lower(dim.video_title) LIKE '%%weekly weird news%%' THEN 'weekly weird news'
                    WHEN dim.upload_creator = 'What''s Good Games' AND (lower(dim.video_title) LIKE '%%ep.%%' OR lower(dim.video_title) LIKE '%%episode%%') THEN 'whats good games'
                    WHEN dim.upload_creator = 'Theo Von' AND lower(dim.video_title) LIKE '%%this past weekend%%' AND lower(dim.video_title) LIKE '%%theo von%%' THEN 'this past weekend'
                    ELSE 'none'
                END) as series_title,
                video_title as episode_title,
                dim.duration_seconds,
                cast(date_add('days', m.date_value, cast(dim.date_of_upload as date)) as varchar(10)) as viewership_date,
                cast(dim.date_of_upload as varchar(10)) as air_date,
                m.views
            FROM warehouse.quarterly_sales_tubular_metrics m
            LEFT JOIN warehouse.dim_tubular_videos dim on dim.video_id = m.video_id
            WHERE 
                 m.agg_type = 'daily'
                 AND dim.video_title NOT LIKE '%%!PODCAST!%%'
                 AND dim.video_title NOT LIKE '%%!LIVE GAMEPLAY!%%'
                 AND dim.duration_seconds > 120

        )
        SELECT
            series_title,
            episode_title,
            viewership_date,
            air_date,
            sum(views) as views
        FROM cte
        WHERE
            series_title != 'none'
            AND episode_title NOT in (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes) 
        GROUP BY 1, 2, 3, 4;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            days_since_pub = (datetime.strptime(result[2], '%Y-%m-%d') - datetime.strptime(result[3], '%Y-%m-%d')).days
            if days_since_pub <= 45:
                self.tubular_podcasts.append({
                    'series_title': result[0],
                    'episode_title': result[1],
                    'viewership_date': result[2],
                    'air_date': result[3],
                    'platform': 'youtube',
                    'days_since_pub': days_since_pub,
                    'views': result[4]
                })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.tubular_podcasts)
        self.final_dataframe['run_date'] = self.run_date


    def truncate_table(self):
        self.loggerv3.info('Truncation table')
        query = f"""TRUNCATE {self.schema}.{self.table_name};"""
        self.db_connector.write_redshift(query)


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def execute(self):
        self.loggerv3.start(f"Running Agg Daily Tubular Quarterly Sales Job for Run Date {self.run_date}")
        self.get_tubular_data()
        self.build_final_dataframe()
        self.truncate_table()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
