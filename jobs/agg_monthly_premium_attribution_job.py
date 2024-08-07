import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggMonthlyPremiumAttributionJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname=__name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_monthly_premium_attribution')
		self.Dater = DateHandler()
		self.target_date = target_date.replace('-','')
		self.start_month = self.Dater.format_date(self.Dater.get_start_of_month(self.target_date))
		self.end_month = self.Dater.format_date(self.Dater.find_next_day(self.Dater.get_end_of_month(self.Dater.get_start_of_month(self.target_date))))
		self.prev_month = self.Dater.format_date(self.Dater.get_previous_month(self.Dater.get_start_of_month(self.target_date)))
		self.attributions = []
		self.series_brand_mappings = []
		self.attributions_df = None
		self.mappings_df = None
		self.final_dataframe = None


	def get_premium_attributions(self):
		self.loggerv3.info('Getting Premium Attributions')
		query = f"""
		WITH cte as (
			SELECT
				dse.channel_title,
				dse.series_title,
				dse.episode_title,
				dse.episode_key,
				pa.membership_plan,
				(CASE
					WHEN membership_plan = '1month' THEN count(distinct user_key)
					WHEN membership_plan = '6month' THEN count(distinct user_key) * 6
					WHEN membership_plan = '1year' THEN count(distinct user_key) * 12
				END) as attributions,
				min(pa.source) as source
			FROM warehouse.premium_attributions_v3 pa
			LEFT JOIN warehouse.dim_segment_episode dse on dse.episode_key = pa.episode_key
			WHERE pa.subscription_start >= '{self.start_month}'
				AND pa.subscription_start < '{self.end_month}'
			GROUP BY 1, 2, 3, 4, 5
		)
		SELECT
			channel_title,
			series_title,
			episode_title,
			episode_key,
			sum(attributions),
			min(source)
		FROM cte
		WHERE
			channel_title is NOT NULL
			AND series_title is NOT NULL
			AND episode_title is NOT NULL
		GROUP BY 1, 2, 3, 4;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.attributions.append({
				"viewership_month": self.start_month,
				'channel_title': result[0].lower().strip(),
				'series_title': result[1].lower().strip(),
				'episode_title': result[2].lower().strip(),
				'key': result[3],
				'attributions': result[4],
				'source': result[5]
				})


	def get_signup_flow_campaign_attributions(self):
		self.loggerv3.info('Getting Signup Flow Campaign Attributions')
		query = f"""
		
		WITH cte as (
			SELECT campaign,
				   membership_plan,
				   (CASE
					  WHEN membership_plan = '1month' THEN count(distinct user_key)
					  WHEN membership_plan = '6month' THEN count(distinct user_key) * 6
					  WHEN membership_plan = '1year' THEN count(distinct user_key) * 12
					END) as attributions
			FROM warehouse.signup_flow_campaign_attributions
			WHERE subscription_start >= '{self.start_month}'
			  AND subscription_start < '{self.end_month}'
			  AND campaign != 'standard'
			  AND campaign != 'firstlanding'
			  AND campaign is NOT NULL
			GROUP BY 1, 2
		)
		SELECT
			campaign,
			sum(attributions) as attributions
		FROM cte
		GROUP BY 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.attributions.append({
				"viewership_month": self.start_month,
				'channel_title': None,
				'series_title': None,
				'episode_title': None,
				'key': result[0].strip(),
				'attributions': result[1],
				'source': 'signup_campaign'

				})


	def get_youtube_membership_attributions(self):
		self.loggerv3.info('Getting YouTube Memberships Attributions')
		query = f"""
		WITH current_cte as (
			SELECT sponsor_type,
				   user_key
			FROM warehouse.youtube_rt_members
			WHERE sponsorship_starts_at >= '{self.start_month}'
			  AND sponsorship_starts_at < '{self.end_month}'
			GROUP BY 1, 2
		), previous_cte as (
			SELECT sponsor_type,
				   user_key
			FROM warehouse.youtube_rt_members
			WHERE sponsorship_starts_at >= '{self.prev_month}'
			  AND sponsorship_starts_at < '{self.start_month}'
			GROUP BY 1, 2
		), filter_cte as (
			SELECT c.sponsor_type,
				   c.user_key
			FROM current_cte c
			LEFT JOIN previous_cte p on p.user_key = c.user_key and p.sponsor_type = c.sponsor_type
			WHERE p.sponsor_type is NULL
		)
		SELECT
			sponsor_type,
			count(distinct user_key)
		FROM filter_cte
		GROUP BY 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.attributions.append({
				"viewership_month": self.start_month,
				'channel_title': None,
				'series_title': None,
				'episode_title': None,
				'key': result[0].strip(),
				'attributions': result[1],
				'source': 'youtube_membership'
				})


	def get_series_brand_mappings(self):
		self.loggerv3.info('Getting series brand mappings')
		query = f"""
		SELECT
			channel_title,
			series_title,
			brand
		FROM warehouse.series_brand_mappings
		WHERE month >= '{self.start_month}'
			AND month < '{self.end_month}'
		GROUP BY 1, 2, 3;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.series_brand_mappings.append({
				'channel_title': result[0].lower().strip(),
				'series_title': result[1].lower().strip(),
				'brand': result[2].lower()
				})


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		# Join in Brands
		self.attributions_df = pd.DataFrame(self.attributions)
		self.mappings_df = pd.DataFrame(self.series_brand_mappings)
		self.final_dataframe = self.attributions_df.merge(self.mappings_df, on=['channel_title', 'series_title'], how='left')
		# Set Brands from other sources
		self.final_dataframe['brand'].mask(self.final_dataframe['key'] == 'death_battle_youtube', 'death battle', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'] == 'funhaus_youtube', 'funhaus', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('fface'), 'f**kface', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('funhaus'), 'funhaus', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('facejam'), 'face jam', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('rwby'), 'rwby', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('red-web'), 'red web', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('redweb'), 'red web', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('tftsd'), 'stinky dragon', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('stinky'), 'stinky dragon', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('dogbark'), 'dogbark', inplace=True)
		self.final_dataframe['brand'].mask(self.final_dataframe['key'].str.contains('campcamp'), 'other animation', inplace=True)
		# Calculate
		self.final_dataframe['total_attributions'] = self.final_dataframe['attributions'].sum()
		self.final_dataframe['pct_of_total'] = self.final_dataframe['attributions'] / self.final_dataframe['total_attributions']
		self.final_dataframe['pct_of_total'] = self.final_dataframe['pct_of_total'].round(4)


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Redshift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Monthly Premium Attributions Job for {self.start_month}")
		self.get_premium_attributions()
		self.get_signup_flow_campaign_attributions()
		self.get_youtube_membership_attributions()
		self.get_series_brand_mappings()
		self.build_final_dataframe()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
