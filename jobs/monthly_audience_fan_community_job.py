import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class MonthlyAudienceFanCommunityJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'monthly_audience_fan_community')

		self.Dater = DateHandler()
		self.start_day = self.Dater.get_start_of_month(self.target_date.replace('-',''))
		self.end_day = self.Dater.get_end_of_month(self.start_day)
		self.next_day = self.Dater.find_next_day(self.end_day)
		self.formatted_dates = self.get_formatted_dates()
		self.days_examined = self.get_days_examined()
		self.seconds_threshold = 300
		self.audience = []
		self.fans = []
		self.community = []
		self.final_df = None


	def get_formatted_dates(self):
		return {
			"start": self.Dater.format_date(self.start_day),
			"end": self.Dater.format_date(self.end_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def get_days_examined(self):
		month = self.target_date.split('-')[1]
		year = self.target_date.split('-')[0]
		if month in ['01', '03', '05', '07', '08', '10', '12']:
			return 31
		if month == '02':
			if int(year) % 4 == 0:
				return 29
			else:
				return 28
		return 30


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
		    timestamp >= '{self.formatted_dates['start']}' AND
		    timestamp < '{self.formatted_dates['next']}'
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
			    WHERE start_timestamp >= '{self.formatted_dates['start']}'
			      AND start_timestamp < '{self.formatted_dates['next']}'
			      AND vv.user_key is not null
			    GROUP BY 1
			), live_viewers as (
			    SELECT
			        user_id as key,
			        sum(active_seconds) as seconds
			    FROM warehouse.livestream_viewership
			    WHERE start_timestamp >= '{self.formatted_dates['start']}'
			      AND start_timestamp < '{self.formatted_dates['next']}'
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
			    event_timestamp >= '{self.formatted_dates['start']}' AND
			    event_timestamp < '{self.formatted_dates['next']}'
			GROUP BY 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			community.add(result[0])

		# Commentors
		query = f"""
			SELECT
			    owner_uuid as uuid
			FROM comments
			WHERE
			    created_at >= '{self.formatted_dates['start']}' AND
			    created_at < '{self.formatted_dates['next']}' AND
			    created_by_staff != 1 AND
			    spam_auto_detected is Null
		    GROUP BY 1
		"""
		results = self.db_connector.query_comments_db_connection(query)
		for result in results:
			community.add(result[0].decode('utf-8'))

		# Likers
		query = f"""
			SELECT
			    user_uuid as uuid
			FROM likes
			WHERE
			    created_at >= '{self.formatted_dates['start']}' AND
			    created_at < '{self.formatted_dates['next']}'
			GROUP BY 1;
		"""
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
		    created_at >= '{self.formatted_dates['start']}' AND
		    created_at < '{self.formatted_dates['next']}'
		GROUP BY 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			community.add(result[0])

		# Turn set into list and set self.community
		self.community = list(community)


	def dedupe_lists(self):
		self.loggerv3.info("De-duping lists")
		deduped_audience = [x for x in self.audience if x not in self.fans and x not in self.community]
		deduped_fans = [x for x in self.fans if x not in self.community]

		self.audience = deduped_audience
		self.fans = deduped_fans


	def build_final_dataframe(self):
		self.loggerv3.info("Creating Dataframe")
		record = {
			'month_start': self.formatted_dates['start'],
			'month_end': self.formatted_dates['end'],
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


	def write_to_red_shift(self):
		self.loggerv3.info("Writing to Red Shift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Monthly Audience/Fan/Community Job: {self.formatted_dates['start']} - {self.formatted_dates['end']}")
		self.process()
		self.write_to_red_shift()
		self.loggerv3.success("All Processing Complete!")
