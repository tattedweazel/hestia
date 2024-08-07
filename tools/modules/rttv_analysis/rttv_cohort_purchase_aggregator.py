from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater as DateHandler
from utils.components.logger import Logger
import pandas as pd


class RttvCohortPurchaseAggregator:

	def __init__(self, target_date):
		self.db_connector = DatabaseConnector('')
		self.Dater = DateHandler()
		self.logger = Logger()
		self.table_name = 'agg_rttv_shopify_orders'
		self.window_end = target_date.replace('-','')
		self.window_start = self.Dater.find_x_days_ago(self.window_end, 20)
		self.view_cap = self.Dater.find_x_days_ago(self.window_end, 13)
		self.window_cap = self.Dater.find_next_day(self.window_end)
		self.formatted_dates = self.get_formatted_dates()
		self.tiers = ['free', 'premium', 'trial']
		self.records = []


	def get_formatted_dates(self):
		return {
			"window_start": self.Dater.format_date(self.window_start),
			"view_cap": self.Dater.format_date(self.view_cap),
			"window_cap": self.Dater.format_date(self.window_cap)
		}


	def get_aggregate(self, tier):
		query = f"""with cohort_orders as (
					    SELECT order_id,
					           created_at,
					           shopify_id,
					           order_total_cents - order_total_discounts_cents as total
					    FROM warehouse.shopify_orders
					    WHERE created_at >= '{self.formatted_dates['window_start']}'
					      AND created_at < '{self.formatted_dates['window_cap']}'
					      AND financial_status = 'paid'
					      AND shopify_id in (
					        SELECT dsr.shopify_id
					        FROM warehouse.livestream_viewership lv
					                 INNER JOIN warehouse.dim_shopify_rt dsr
					                            ON dsr.rt_id = lv.user_id
					        WHERE lv.start_timestamp >= '{self.formatted_dates['window_start']}'
					          AND lv.start_timestamp < '{self.formatted_dates['view_cap']}'
					          AND lv.user_tier = '{tier}'
					    )
					    GROUP BY 1, 2, 3, 4
					), unique_viewers as (
						SELECT
							count(distinct user_id) as total
						FROM warehouse.livestream_viewership lv2
						WHERE lv2.start_timestamp >= '{self.formatted_dates['window_start']}'
					          AND lv2.start_timestamp < '{self.formatted_dates['view_cap']}'
					          AND lv2.user_tier = '{tier}' 
					)
					SELECT
					    count(*) as orders,
					    (select total from unique_viewers) as unique_viewers,
					    count(distinct shopify_id) as unique_shoppers,
					    sum( (co.total / 100.0) ) as total
					FROM cohort_orders co;"""

		results = self.db_connector.read_redshift(query)
		for result in results:
			self.records.append({
				"cohort_date": self.formatted_dates['window_start'],
				"tier": tier,
				"unique_viewers": result[1],
				"shoppers": result[2],
				"orders": result[0],
				"purchase_total": result[3],
				"avg_total": float(result[3]) / (result[2])
				})


	def build_aggregates(self):
		for tier in self.tiers:
			self.get_aggregate(tier)


	def write_to_redshift(self):
		self.logger.log("Writing results to Red Shift")
		df = pd.DataFrame(self.records)
		self.db_connector.write_to_sql(df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')


	def execute(self):
		self.logger.log(f"Running RTTV Shopper Aggregate for {self.formatted_dates['window_start']}")
		self.build_aggregates()
		self.write_to_redshift()
		self.db_connector.update_redshift_table_permissions(self.table_name)
		self.logger.log("Process Complete!")
		