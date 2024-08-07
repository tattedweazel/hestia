import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class WeeklyAudienceFanCommunityJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'weekly_audience_fan_community')

		self.days_examined = 7
		self.seconds_threshold = 30

		self.target_dto = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.next_dto = self.target_dto + timedelta(1)
		self.start_dto = self.target_dto - timedelta(self.days_examined - 1)
		self.next_date = datetime.strftime(self.next_dto, '%Y-%m-%d')
		self.start_date = datetime.strftime(self.start_dto, '%Y-%m-%d')
		
		self.audience = []
		self.fans = []
		self.community = []

		self.final_df = None

		self.schema = 'warehouse'


	def load_audience(self):
		self.loggerv3.info("Loading Audience")
		# Grab all visitors
		query = f"""
		SELECT
		    case 
		    	when user_key is Null then anonymous_id 
		    	else cast(user_key as varchar) 
	    	end as key
		FROM warehouse.fact_visits
		WHERE
		    timestamp >= '{self.start_date}' AND
		    timestamp < '{self.next_date}'
		GROUP BY 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.audience.append(result[0])


	def load_fans(self):
		self.loggerv3.info("Loading Fans")
		query = f"""
			WITH known_vod_viewers as (
			    SELECT
			        du.user_id as key,
			        sum(active_seconds) as seconds
			    FROM warehouse.vod_viewership vv
			    INNER JOIN warehouse.dim_user du
			    ON du.user_key = vv.user_key
			    WHERE start_timestamp >= '{self.start_date}'
			      AND start_timestamp < '{self.next_date}'
			      AND vv.user_key is not null
			    GROUP BY 1
			), live_viewers as (
			    SELECT
			        user_id as key,
			        sum(active_seconds) as seconds
			    FROM warehouse.livestream_viewership
			    WHERE start_timestamp >= '{self.start_date}'
			      AND start_timestamp < '{self.next_date}'
			    GROUP BY 1
			), combined as (
			    SELECT kvv.key as key, kvv.seconds as seconds
			    FROM known_vod_viewers kvv
			    UNION ALL
			    SELECT lv.key as key, lv.seconds as seconds
			    FROM live_viewers lv
			), de_duped as (
			    SELECT
			        key,
			        sum(seconds) as seconds
			    FROM combined
			    GROUP BY 1
			)
			SELECT
			    key
			FROM de_duped
			WHERE seconds > {self.days_examined * self.seconds_threshold};
		"""

		results = self.db_connector.read_redshift(query)
		for result in results:
			self.fans.append(result[0])


	def load_community(self):
		self.loggerv3.info("Loading Community")

		# Use a set to de-dupe across sets to be pulled in
		community = set()
		
		# Chats/Votes
		query = f"""
			SELECT
			    user_id
			FROM warehouse.chat_event
			WHERE
			    action in ('sent_message', 'voted') AND
			    event_timestamp >= '{self.start_date}' AND
			    event_timestamp < '{self.next_date}'
			GROUP BY 1;
		"""
		self.loggerv3.info("Loading Chat/Votes")
		results = self.db_connector.read_redshift(query)
		for result in results:
			community.add(result[0])

		# Commentors
		query = f"""
			SELECT
			    owner_uuid as uuid
			FROM comments
			WHERE
			    created_at >= '{self.start_date}' AND
			    created_at < '{self.next_date}' AND
			    created_by_staff != 1 AND
			    spam_auto_detected is Null
		    GROUP BY 1
		"""
		self.loggerv3.info("Loading Commentors")
		results = self.db_connector.query_comments_db_connection(query)
		for result in results:
			community.add(result[0].decode('utf-8'))

		# Likers
		query = f"""
			SELECT
			    user_uuid as uuid
			FROM likes
			WHERE
			    created_at >= '{self.start_date}' AND
			    created_at < '{self.next_date}'
			GROUP BY 1;
		"""
		self.loggerv3.info("Loading Likes")
		results = self.db_connector.query_comments_db_connection(query)
		for result in results:
			community.add(result[0])

		# Shoppers
		query = f"""
		SELECT
		    dsr.rt_id
		FROM warehouse.shopify_orders so
		INNER JOIN warehouse.dim_shopify_rt dsr
		ON so.shopify_id = dsr.shopify_id
		WHERE
		    created_at >= '{self.start_date}' AND
		    created_at < '{self.next_date}'
		GROUP BY 1;
		"""
		# self.loggerv3.info("Loading Shoppers")
		# results = self.db_connector.read_redshift(query)
		# for result in results:
		# 	community.add(result[0])

		# Turn set into list and set self.community
		self.community = list(community)


	def dedupe_lists(self):
		self.loggerv3.info("De-duping lists")
		deduped_audience = []
		deduped_fans = []

		self.loggerv3.info("De-duping Audience")
		for key in self.audience:
			if key not in self.fans and key not in self.community:
				deduped_audience.append(key)

		self.loggerv3.info("De-duping Fans")
		for key in self.fans:
			if key not in self.community:
				deduped_fans.append(key)

		self.audience = deduped_audience
		self.fans = deduped_fans


	def build_final_dataframe(self):
		self.loggerv3.info("Creating Dataframe")
		record = {
			'week_start': self.start_date,
			'week_end': self.target_date,
			'total_population': len(self.audience) + len(self.fans) + len(self.community),
			'audience': len(self.audience),
			'fans': len(self.fans),
			'community': len(self.community)
		}
		self.final_df = pd.DataFrame([record])
		

	def process(self):
		self.load_audience()
		self.load_fans()
		self.load_community()
		self.dedupe_lists()
		self.build_final_dataframe()


	def write_to_redshift(self):
		self.loggerv3.info("Writing to Redshift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Weekly Audience/Fan/Community Job: {self.start_date} - {self.target_date}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
