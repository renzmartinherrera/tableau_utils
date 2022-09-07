import os, logging
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


load_dotenv()

snowflake_logger = logging.getLogger('snowflake')
snowflake_logger.setLevel(logging.WARNING) # Limit logging output by Snowflake module


SNOWFLAKE_USER_NAME = os.getenv('SNOWFLAKE_USER_NAME')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT_NAME = os.getenv('SNOWFLAKE_ACCOUNT_NAME')
SNOWFLAKE_ROLE_NAME = os.getenv('SNOWFLAKE_ROLE_NAME')


def snowflake_connect():
	ctx = snowflake.connector.connect(
		user=SNOWFLAKE_USER_NAME,
		password=SNOWFLAKE_PASSWORD,
		account=SNOWFLAKE_ACCOUNT_NAME,
		role=SNOWFLAKE_ROLE_NAME
		)
	return ctx


def snowflake_query(query):
	ctx = snowflake_connect()

	cs = ctx.cursor()

	try:
		cs.execute(query)
		query_results = cs.fetchall()
		return query_results
	finally:
		cs.close()
		ctx.close()


def snowflake_query_string(query):
	ctx = snowflake_connect()

	try:
		query_results_cursors = ctx.execute_string(query)
		return query_results_cursors
	finally:
		ctx.close()
