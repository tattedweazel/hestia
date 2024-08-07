import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class EngagementsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'site_engagements')

		self.target_dto = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.next_dto = self.target_dto + timedelta(1)
		self.next_date = datetime.strftime(self.next_dto, '%Y-%m-%d')
		
		self.final_df = None


	def load_comment_data(self):
		self.loggerv3.info("Loading Comment Data...")
		# topic_type mapping:
		# 0 - Episode
		# 1 - Community
		# 2 - Bonus Feature
		# 3 - Movie (this is currently never used)

		results = self.db_connector.query_comments_db_connection(f""" SELECT
			'{self.target_date}' as run_date,
			(SELECT
			    count(uuid)
			FROM
			    comments
			WHERE
			    parent_uuid is Null AND
			    deleted_at is Null AND
			    topic_type != 1 AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as video_comments_created,

			(/* Video Replies Created */
			SELECT
			    count(uuid)
			FROM
			    comments
			WHERE
			    parent_uuid is NOT Null AND
			    deleted_at is Null AND
			    topic_type != 1 AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as video_replies_created,

			(/* Video Commentors */
			SELECT
			    count(distinct owner_uuid)
			FROM
			    comments
			WHERE
			    parent_uuid is Null AND
			    deleted_at is Null AND
			    topic_type != 1 AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as video_commentors,

			(/* Video Repliers */
			SELECT
			    count(distinct owner_uuid)
			FROM
			    comments
			WHERE
			    parent_uuid is Not Null AND
			    deleted_at is Null AND
			    topic_type != 1 AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as video_repliers,

			(/* Video Likes Created */
			SELECT
			    count(id)
			FROM
			    likes
			WHERE
			    comment_id in (
			        SELECT
			            id
			        FROM
			            comments
			        WHERE
			            deleted_at is Null AND
			            topic_type != 1
			    ) AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as video_likes_created,

			(/* Community Comments Created */
			SELECT
			    count(uuid)
			FROM
			    comments
			WHERE
			    parent_uuid is Null AND
			    deleted_at is Null AND
			    topic_type = 1 AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as community_comments_created,

			(/* Community Replies Created */
			SELECT
			    count(uuid)
			FROM
			    comments
			WHERE
			    parent_uuid is NOT Null AND
			    deleted_at is Null AND
			    topic_type = 1 AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as community_replies_created,

			(/* Community Commentors */
			SELECT
			    count(distinct owner_uuid)
			FROM
			    comments
			WHERE
			    parent_uuid is Null AND
			    deleted_at is Null AND
			    topic_type = 1 AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as community_commentors,

			(/* Community Repliers */
			SELECT
			    count(distinct owner_uuid)
			FROM
			    comments
			WHERE
			    parent_uuid is NOT Null AND
			    deleted_at is Null AND
			    topic_type = 1 AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as community_repliers,

			(/* Community Likes Created */
			SELECT
			    count(id)
			FROM
			    likes
			WHERE
			    comment_id in (
			        SELECT
			            id
			        FROM
			            comments
			        WHERE
			            deleted_at is Null AND
			            topic_type = 1
			    ) AND
			    created_at >= '{self.target_date}' AND
			    created_at < '{self.next_date}') as community_likes_created;
		""")
		for result in results:
			record = {
				"run_date": result[0],
				"total_engagements": result[1] + result[2] + result[5] + result[6] + result[7] + result[10],
				"video_comments_created": result[1],
				"video_replies_created": result[2],
				"video_commentors": result[3],
				"video_repliers": result[4],
				"video_likes_created": result[5],
				"community_comments_created": result[6],
				"community_replies_created": result[7],
				"community_commentors": result[8],
				"community_repliers": result[9],
				"community_likes_created": result[10]
			}
		self.final_df = pd.DataFrame([record])


	def write_to_red_shift(self):
		self.loggerv3.info("Writing to Red Shift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Engagements Job")
		self.load_comment_data()
		self.write_to_red_shift()
		self.loggerv3.success("All Processing Complete!")
