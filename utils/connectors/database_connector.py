import pandas as pd
import mysql.connector as mysql_con
from base.connector import Connector
from pandas.io import sql
from sqlalchemy import create_engine


class DatabaseConnector(Connector):

	def __init__(self, file_location, dry_run=False):
		super().__init__(file_location)
		self.queries = []
		self.dry_run = dry_run


	def sv2_engine(self):
		host = self.creds['SV2_HOST']
		user = self.creds['SV2_USER']
		pwd = self.creds['SV2_PASS']
		dw_engine = create_engine(
			'postgresql+psycopg2://{}:{}@{}:5439/dw'
			.format(user,pwd,host)
		)
		return dw_engine


	def v2_db_connection(self):
		host = self.creds['V2_DB_HOST']
		db = self.creds['V2_DB']
		user = self.creds['V2_DB_USER']
		password = self.creds['V2_DB_PASS']
		cnx = mysql_con.connect(host=host,user=user,password=password,database=db)
		return cnx


	def business_service_engine(self):
		host = self.creds['BS_DB_HOST']
		user = self.creds['BS_DB_USER']
		pwd = self.creds['BS_DB_PASS']
		db_engine = create_engine(
			'mysql://{}:{}@{}:3306/business-service'
			.format(user,pwd,host)
		)
		return db_engine


	def business_service_connection(self):
		host = self.creds['BS_DB_HOST']
		db = self.creds['BS_DB']
		user = self.creds['BS_DB_USER']
		pwd = self.creds['BS_DB_PASS']
		cnx = mysql_con.connect(host=host, user=user, password=pwd, database=db)
		return cnx


	def svod_be_engine(self):
		host = self.creds['SVOD_BE_DB_HOST']
		user = self.creds['SVOD_BE_DB_USER']
		pwd = self.creds['SVOD_BE_DB_PASS']
		db_engine = create_engine(
			'mysql://{}:{}@{}:3306/rt_svod_be_production'
			.format(user,pwd,host)
		)
		return db_engine

	def svod_be_connection(self):
		host = self.creds['SVOD_BE_DB_HOST']
		db = self.creds['SVOD_BE_DB']
		user = self.creds['SVOD_BE_DB_USER']
		pwd = self.creds['SVOD_BE_DB_PASS']
		cnx = mysql_con.connect(host=host, user=user, password=pwd, database=db)
		return cnx


	def comments_engine(self):
		host = self.creds['COMMENTS_HOST']
		user = self.creds['COMMENTS_USER']
		pwd = self.creds['COMMENTS_PASS']
		db_engine = create_engine(
			'mysql://{}:{}@{}:3306/rt_comments_production'
			.format(user,pwd,host)
		)
		return db_engine


	def comments_connection(self):
		host = self.creds['COMMENTS_HOST']
		db = 'rt_comments_production'
		user = self.creds['COMMENTS_USER']
		pwd = self.creds['COMMENTS_PASS']
		cnx = mysql_con.connect(host=host, user=user, password=pwd, database=db)
		return cnx


	def community_connection(self):
		host = self.creds['COMMUNITY_HOST']
		db = 'community_be'
		user = self.creds['COMMUNITY_USER']
		pwd = self.creds['COMMUNITY_PASS']
		cnx = mysql_con.connect(host=host, user=user, password=pwd, database=db)
		return cnx


	def query_v2_db(self, query):
		self.queries.append(query)
		cnx = self.v2_db_connection()
		cursor = cnx.cursor()
		cursor.execute(query)
		results = []
		for result in cursor:
			results.append(result)
		cnx.close()
		return results


	def read_redshift(self, query: str):
		if 'update' in query.lower() or 'set' in query.lower() or 'delete' in query.lower() or 'insert into' in query.lower() or 'truncate' in query.lower() or 'drop table' in query.lower():
			raise ValueError('Query contains write logic! read_redshift method only reads from redshift')

		self.queries.append(query)
		sv2_connection = self.sv2_engine().connect()
		results = sv2_connection.execute(query)
		sv2_connection.close()
		return results


	def write_redshift(self, query: str, dry_run: bool = False):
		if dry_run is True or self.dry_run is True:
			return
		else:
			self.queries.append(query)
			sv2_connection = self.sv2_engine().connect()
			results = sv2_connection.execute(query)
			sv2_connection.close()
			return results


	def query_business_service_db(self, query):
		conn = self.business_service_engine().connect()
		trans = conn.begin()
		results = conn.execute(query)
		trans.commit()
		conn.close()
		return results.fetchall()


	def query_business_service_db_connection(self, query):
		self.queries.append(query)
		cnx = self.business_service_connection()
		cursor = cnx.cursor()
		cursor.execute(query)
		results = []
		for result in cursor:
			results.append(result)
		cnx.close()
		return results


	def query_svod_be_db(self, query):
		conn = self.svod_be_engine().connect()
		trans = conn.begin()
		results = conn.execute(query)
		trans.commit()
		conn.close()
		return results.fetchall()


	def query_svod_be_db_connection(self, query):
		self.queries.append(query)
		cnx = self.svod_be_connection()
		cursor = cnx.cursor()
		cursor.execute(query)
		results = []
		for result in cursor:
			results.append(result)
		cnx.close()
		return results


	def query_comments_db(self, query):
		conn = self.comments_engine().connect()
		trans = conn.begin()
		results = conn.execute(query)
		trans.commit()
		conn.close()
		return results.fetchall()


	def query_comments_db_connection(self, query):
		self.queries.append(query)
		cnx = self.comments_connection()
		cursor = cnx.cursor()
		cursor.execute(query)
		results = []
		for result in cursor:
			results.append(result)
		cnx.close()
		return results


	def query_community_db_connection(self, query):
		self.queries.append(query)
		cnx = self.community_connection()
		cursor = cnx.cursor()
		cursor.execute(query)
		results = []
		for result in cursor:
			results.append(result)
		cnx.close()
		return results


	def update_redshift_table_permissions(self, table_name: str, schema: str = 'warehouse', dry_run: bool = False):
		if dry_run is True or self.dry_run is True:
			return
		else:
			sv2_conn = self.sv2_engine().connect()
			sv2_conn.execute(f"grant select on table {schema}.{table_name} to dwuser")
			sv2_conn.execute(f"grant select on table {schema}.{table_name} to readonly")
			sv2_conn.execute(f"grant select on table {schema}.{table_name} to looker")
			sv2_conn.execute(f"grant select on table {schema}.{table_name} to admin")
			sv2_conn.close()


	def write_to_sql(
			self,
			dataframe: pd.DataFrame,
			name: str,
			con,
			schema=None,
			if_exists: str = "fail",
			index: bool = True,
			index_label=None,
			chunksize=None,
			dtype: None = None,
			method=None,
			dry_run: bool = False
	):
		if dry_run is True or self.dry_run is True:
			return
		else:
			return sql.to_sql(
				dataframe,
				name,
				con,
				schema=schema,
				if_exists=if_exists,
				index=index,
				index_label=index_label,
				chunksize=chunksize,
				dtype=dtype,
				method=method,
			)