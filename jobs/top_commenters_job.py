import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.connectors.google_cloud_api_connector import GoogleCloudApiConnector
from utils.components.batcher import Batcher
from utils.components.dater import Dater
from utils.connectors.database_connector import DatabaseConnector
from utils.components.byte_helper import byte_helper


class TopCommentersJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, local_mode=True)
        """
        To Run Locally
        from jobs.top_commenters_job import TopCommentersJob
        tp = TopCommentersJob()
        tp.execute()
        """
        self.dater = Dater()
        self.target_date = self.today = self.dater.format_date(self.dater.get_today())
        self.batcher = Batcher()
        self.batches = None
        self.sheets_api_connector = GoogleCloudApiConnector(file_location=file_location, service='sheets')
        self.sheets_service = self.sheets_api_connector.get_service_api()
        self.drive_api_connector = GoogleCloudApiConnector(file_location=file_location, service='drive')
        self.drive_service = self.drive_api_connector.get_service_api()
        self.folder_id = '1l_Dsd_ZVK2YpS7JhMrz-DDl76yFgQn69'
        self.top_episode_commenters = []
        self.top_community_commenters = []
        self.top_community_posters = []
        self.users = []
        self.top_community_commenters_df = None
        self.top_episode_commenters_df = None
        self.top_community_posters_df = None
        self.users_df = None
        self.DAY_THRESHOLD = 7
        self.TOP_COMMENTS_THRESHOLD = 20
        self.db_connector = DatabaseConnector('')


    def query_comments(self, topic_types):
        query = f"""
        SELECT
            owner_uuid,
            count(*) as comments
        FROM comments
        WHERE created_at > DATE_SUB(curdate(), INTERVAL {self.DAY_THRESHOLD} DAY)
          AND created_by_staff = 0
          AND deleted_at is NULL
          AND spam_auto_detected is NULL
          AND flags_count = 0
          AND owner_uuid NOT IN (SELECT user_uuid FROM shadowbans)
          AND topic_type IN ({topic_types})
        GROUP BY 1
        ORDER BY 2 desc
        LIMIT {self.TOP_COMMENTS_THRESHOLD};
        """
        return self.db_connector.query_comments_db_connection(query)


    def load_top_episode_commenters(self):
        self.loggerv3.info("Load Top Episode Commenters")
        results = self.query_comments(topic_types='0, 2')
        for result in results:
            self.top_episode_commenters.append({
                'user_id': result[0].decode(),
                'comments': result[1]
            })


    def load_top_community_commenters(self):
        self.loggerv3.info("Load Top Community Commenters")
        results = self.query_comments(topic_types='1')
        for result in results:
            self.top_community_commenters.append({
                'user_id': result[0].decode(),
                'comments': result[1]
            })


    def load_top_community_posters(self):
        self.loggerv3.info("Load Top Community Posters")
        query = f""" 
        SELECT author_id,
               count(*)
        FROM posts
        WHERE owner_type = 'User'
            AND flags_count = 0
            AND deleted_at is NULL
            AND auto_removed is NULL
            AND created_at >= DATE_SUB(curdate(), INTERVAL {self.DAY_THRESHOLD} DAY)
        GROUP BY 1
        ORDER BY 2 desc
        LIMIT {self.TOP_COMMENTS_THRESHOLD};
        """
        results = self.db_connector.query_community_db_connection(query)
        for result in results:
            record = {
                'user_id': byte_helper(result[0]),
                'posts': byte_helper(result[1])
            }
            self.top_community_posters.append(record)


    def load_users(self):
        self.loggerv3.info("Loading Users")
        target_uuids = []
        for user in self.top_episode_commenters:
            target_uuids.append(f"'{user['user_id']}'")
        for user in self.top_community_commenters:
            target_uuids.append(f"'{user['user_id']}'")
        for user in self.top_community_posters:
            target_uuids.append(f"'{user['user_id']}'")

        self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=target_uuids)
        for batch in self.batches:
            users = ','.join(batch)
            query = f"""
                SELECT uuid, username 
                FROM users
                WHERE uuid in ({users});
            """
            results = self.db_connector.query_v2_db(query)
            for result in results:
                self.users.append({
                    'user_id': result[0],
                    'username': result[1]
                })


    def merge_users(self):
        self.loggerv3.info('Merging users')
        self.users_df = pd.DataFrame(self.users)

        # Episode Commenters
        self.top_episode_commenters_df = pd.DataFrame(self.top_episode_commenters)
        self.top_episode_commenters_df = self.top_episode_commenters_df.merge(self.users_df, how='inner', on='user_id')

        # Community Commenters
        self.top_community_commenters_df = pd.DataFrame(self.top_community_commenters)
        self.top_community_commenters_df = self.top_community_commenters_df.merge(self.users_df, how='inner', on='user_id')

        # Community Posters
        self.top_community_posters_df = pd.DataFrame(self.top_community_posters)
        self.top_community_posters_df = self.top_community_posters_df.merge(self.users_df, how='inner', on='user_id')


    def write_to_google_drive(self, df, title):
        # Create Episode Commenters Sheet
        sheet = self.sheets_api_connector.create_sheet(title=f'Top {title} - {self.target_date}')

        # Write to Episode Commenters Sheet
        sheet_id = sheet['spreadsheetId']
        # Note output could also be created like this self.top_episode_commenters_df.T.reset_index().T.values.tolist()
        self.sheets_api_connector.write_to_sheet(
            sheet_id=sheet_id,
            df=df
        )

        # Move Episode Commenters Sheet
        self.drive_api_connector.move_file(file_id=sheet_id, folder_id=self.folder_id)


    def write_episode_commenters(self):
        self.loggerv3.info('Write Episode Commenters to Google Drive')
        self.write_to_google_drive(df=self.top_episode_commenters_df, title='Episode Commenters')


    def write_community_commenters(self):
        self.loggerv3.info('Write Community Commenters to Google Drive')
        self.write_to_google_drive(df=self.top_community_commenters_df, title='Community Commenters')


    def write_community_posters(self):
        self.loggerv3.info('Write Community Posters to Google Drive')
        self.write_to_google_drive(df=self.top_community_posters_df, title='Community Posters')


    def execute(self):
        self.loggerv3.start(f"Running Top Commenters Job")
        self.load_top_episode_commenters()
        self.load_top_community_commenters()
        self.load_top_community_posters()
        self.load_users()
        self.merge_users()
        self.write_episode_commenters()
        self.write_community_commenters()
        self.write_community_posters()
        self.loggerv3.success("All Processing Complete!")
