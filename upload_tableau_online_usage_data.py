import common
import database
import tableau_online
from datetime import datetime
import argparse

def get_cmd_parameters():
	parser = argparse.ArgumentParser()
	"""
		'output_path'			Filepath to store data source file. '/' required at end of path.
		'num_look_back_days'	(Optional) Number of days to insert. Default value is 1.
		'table_name'			(Optional) Default value is 'TABLEAU_ONLINE_USAGE'.
		'schema_name'			(Optional) Default value is 'BUSINESS_INTELLIGENCE'.
		'database_name'			(Optional) Default value is 'PATTERN_DB'.
	"""

	parser.add_argument(
		"-p",
		"--output_path",
		type=str,
		default='/',
		help="(Optional) Filepath to store data source file. '/' required at end of path."
	)

	parser.add_argument(
		"-n",
		"--num_look_back_days",
		type=str,
		default=1,
		help="(Optional) Number of days to insert. Default value is 1."
	)

	parser.add_argument(
		"-t",
		"--table_name",
		type=str,
		default='TABLEAU_ONLINE_USAGE',
		help="(Optional) Default value is 'TABLEAU_ONLINE_USAGE'."
	)

	parser.add_argument(
		"-s",
		"--schema_name",
		type=str,
		default='BUSINESS_INTELLIGENCE',
		help="(Optional) Default value is 'BUSINESS_INTELLIGENCE'."
	)

	parser.add_argument(
		"-d",
		"--database_name",
		type=str,
		default='PATTERN_DB',
		help="(Optional) Default value is 'PATTERN_DB'."
	)

	args = parser.parse_args()

	return args


def remove_duplicates_on_tableau_online_usage_snowflake_table(table_name, schema_name, database_name):
	"""
	upload_tableau_online_usage_data uses insert rather than merge. This ensures there are no duplicates after the insert.
	"""

	common.standard_logger.info(f"Removing duplicates on Snowflake table {database_name}.{schema_name}.{table_name}...")
	
	delete_statement = f"""

		USE SCHEMA PUBLIC;

		CREATE OR REPLACE TEMP TABLE _latest_event_ingestions AS
	
			SELECT
				tou.EVENT_ID 
			,	MAX(tou.DATETIME_INGESTED_AT) LATEST_INGESTION
			,	COUNT(DISTINCT tou.DATETIME_INGESTED_AT) NUM_INGESTIONS
						
			FROM
				{database_name}.{schema_name}.{table_name} tou
				
			GROUP BY
				1

			HAVING 
				NUM_INGESTIONS > 1
		;


		DELETE
		
		FROM 
			{database_name}.{schema_name}.{table_name} tou
		
		USING 
			_latest_event_ingestions lei 
		
		WHERE 
			tou.EVENT_ID = lei.EVENT_ID
		AND tou.DATETIME_INGESTED_AT < lei.LATEST_INGESTION 
		;

		SELECT
			"number of rows deleted"

		FROM
			TABLE(RESULT_SCAN(LAST_QUERY_ID()))
		;
	"""
	
	num_deleted_records = database.snowflake_query_string(delete_statement)[-1]

	# Has to be a for loop despite being only one row.
	for record in num_deleted_records:
		num_deleted_records = record[0]
		common.standard_logger.debug(f'num_deleted_records: {num_deleted_records}')
	
	return num_deleted_records


def upload_tableau_online_usage_data(output_path='/', num_look_back_days=1, table_name='TABLEAU_ONLINE_USAGE', schema_name='BUSINESS_INTELLIGENCE', database_name='PATTERN_DB'):
	"""
	Downloads TDSX file, extracts to Hyper, converts it to Dataframe, changes the date data types, inserts into Snowflake table,
	and removes potential duplicate rows.
	'output_path'			Filepath to store data source file. '/' required at end of path.
	'num_look_back_days'	(Optional) Number of days to insert. Default value is 1.
	'table_name'			(Optional) Default value is 'TABLEAU_ONLINE_USAGE'.
	'schema_name'			(Optional) Default value is 'BUSINESS_INTELLIGENCE'.
	'database_name'			(Optional) Default value is 'PATTERN_DB'.
	"""

	ts_events_datasource_id = 'd398510b-7ed4-40c7-a560-d08464033063'

	ts_events_datasource = tableau_online.Datasource.get(tableau_online.site, ts_events_datasource_id)
	downloaded_ts_events_file = ts_events_datasource.download(tableau_online.site, output_path, hyper_output_file_name=ts_events_datasource.name, extract_as_hyper=True, delete_zip_file=True)

	df_data = tableau_online.convert_hyper_file_to_dataframe(downloaded_ts_events_file['full_hyper_file_path'], 'Extract')

	common.standard_logger.info('Setting data types...')
	df_data['datetime_ingested_at'] = str(datetime.utcnow()) + '-00:00'
	df_data.columns = map(str.upper, df_data.columns)
	df_data['EVENT_DATE'] = df_data['EVENT_DATE'].astype(str) # Convert to string for formatting purposes - database will automatically convert back.
	cnx = database.snowflake_connect()

	common.standard_logger.info('Attempting to write to Snowflake..')
	write_pandas_result = database.write_pandas(cnx, df_data, table_name, schema=schema_name, database=database_name)
	write_pandas_success_bool = write_pandas_result[0]
	write_pandas_chunks = write_pandas_result[1]
	write_pandas_rows = write_pandas_result[2]
	write_pandas_return_message = write_pandas_result[3]

	common.standard_logger.debug(f'write_pandas_success_bool: {write_pandas_success_bool}, write_pandas_chunks: {write_pandas_chunks}, write_pandas_rows: {write_pandas_rows}, write_pandas_return_message: {write_pandas_return_message}')
	num_deleted_records = remove_duplicates_on_tableau_online_usage_snowflake_table(table_name, schema_name, database_name)

	num_rows_inserted = write_pandas_rows

	common.standard_logger.info(f'num_rows_inserted: {num_rows_inserted}. num_deleted_records: {num_deleted_records}.')

	return {'num_rows_inserted': num_rows_inserted, 'num_deleted_records': num_deleted_records}


if __name__ == "__main__":
	common.standard_logger.debug("File is being run directly")
	
	args = get_cmd_parameters()
	main_arguments = common.return_args(vars(args).items())
 
	upload_tableau_online_usage_data(*main_arguments)
