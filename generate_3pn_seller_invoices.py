import database
import common
import sys
import tableau_online
import argparse
from pathlib import Path


def get_cmd_parameters():
	parser = argparse.ArgumentParser()
	"""
        'invoice_view_id'   Tableau view id to be used as the invoice
		'output_folder'		Full path to destination directory to save invoices.
		'vendor_names'		(Optional) Comma-separated list of vendor names for which to generate invoices.
		'seller_names'		(Optional) Comma-separated list of seller names for which to generate invoices.
		'start_week'		(Optional) Starting week for which to generate invoices in format YYYY-MM-DD. Date should be a Monday.
		'end_week'			(Optional) Starting week for which to generate invoices in format YYYY-MM-DD. Date should be a Monday.
	"""

	parser.add_argument(
		"-i",
		"--invoice_view_id",
		type=str,
		default="ad13d3e8-bf32-4884-9578-21d7319b3fd1",
		help="(Optional) Tableau view id to be used as the invoice. Default: ad13d3e8-bf32-4884-9578-21d7319b3fd1"
	)
	
	parser.add_argument(
        "-o",
        "--output_folder",
        type=str,
        help="Full path to destination directory to save invoices.",
		required=True
    )   

	parser.add_argument(
        "-v",
        "--vendor_names",
        type=str,
        help="""(Optional) Python comma-separated list of vendor names for which to generate invoices. Eg. "['vendor_name']" """
    )    

	parser.add_argument(
        "-s",
        "--seller_names",
        type=str,
        help="""(Optional) Python comma-separated list of seller names for which to generate invoices. Eg. "['seller_name']" """
    )    

	parser.add_argument(
        "-w",
        "--start_week",
        type=str,
        help="(Optional) Starting week for which to generate invoices in format YYYY-MM-DD. Date should be a Monday."
    )   
	
	 
	parser.add_argument(
        "-e",
        "--end_week",
        type=str,
        help="(Optional) Starting week for which to generate invoices in format YYYY-MM-DD. Date should be a Monday."
    )

	args = parser.parse_args()

	return args


def main(invoice_view_id, output_folder, vendor_names=None, seller_names=None, start_week=None, end_week=None):
	"""
	Generates Borderless invoices for 3PN sellers that we can submit to Amazon to show chain of custody.
	'invoice_view_id'   Tableau view id to be used as the invoice
	'output_folder'		Full path to destination directory to save invoices.
	'vendor_names'		(Optional) Comma-separated list of vendor names for which to generate invoices.
	'seller_names'		(Optional) Comma-separated list of seller names for which to generate invoices.
	'start_week'		(Optional) Starting week for which to generate invoices in format YYYY-MM-DD. Date should be a Monday.
	'end_week'			(Optional) Ending week for which to generate invoices in format YYYY-MM-DD. Date should be a Monday.
	
	Returns the number of invoices successfully created.
	"""


	# TODO: Make asynchronous so it doesn't take so long

	# query except for last select copied from 3PN Seller Invoice report becuase I'm not sure how to get the query via the API
	# https://us-west-2b.online.tableau.com/#/site/iservetableau/workbooks/188374/views
	# TODO: retreive dynamically via API
	seller_vendor_week_combos_query = """
	-- Python tableau_utils repo: Generating 3PN Seller Invoices
	-- Tableau: 3PN Seller Invoice: https://us-west-2b.online.tableau.com/#/site/iservetableau/workbooks/188374/views

	USE SCHEMA PUBLIC;
	
	
	-- Try to get warehouse time zone for old shipments that don't have a shipments.warehouse_id. Imperfect. Shouldn't matter much because
	-- should happen only for old invoices that we wouldn't expect Amazon to ask for, and we fall back to UTC in the final query if we don't get a match
	
	CREATE OR REPLACE TEMP TABLE _addresses_timezones AS
	
		SELECT DISTINCT
			sadd.ID ADDRESS_ID
		,	wh.TIME_ZONE TIME_ZONE
		
		FROM
			SHIPMENTCZAR_PUBLIC.ADDRESSES sadd
			JOIN SHIPMENTCZAR_PUBLIC.SHIPMENTS ship ON sadd.ID = ship.SHIP_FROM_ADDRESS_ID
				AND	ship._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			JOIN AMACZAR_PUBLIC.CUSTOMER_ADDRESSES cadd ON sadd.CITY = cadd.CITY
				AND cadd._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			JOIN AMACZAR_PUBLIC.WAREHOUSES wh ON cadd.WAREHOUSE_ID = wh.ID
				AND wh._FIVETRAN_DELETED IS DISTINCT FROM TRUE
		
		WHERE
			sadd._FIVETRAN_DELETED IS DISTINCT FROM TRUE
	;
	
	
	CREATE OR REPLACE TEMP TABLE _product_info AS

		SELECT DISTINCT
			sa.SELLER_SKU
		,	amktpl.COUNTRY_CODE
		,	FIRST_VALUE(COALESCE(prod.CUSTOM_PART_NUMBER, prod.PART_NUMBER)) IGNORE NULLS OVER(PARTITION BY sa.SELLER_SKU, amktpl.COUNTRY_CODE ORDER BY prod.DELETED_AT DESC, prod.DISCONTINUED_DATE DESC, prod.ACTIVE DESC, prod.HIDDEN, prod.CREATED_AT DESC) PART_NUM
		,	COALESCE(tvend.ID, prod.VENDOR_ID) VENDOR_ID
		,	FIRST_VALUE(COALESCE(tvend.NAME, avend.VENDOR_NAME)) IGNORE NULLS OVER(PARTITION BY sa.SELLER_SKU ORDER BY prod.DELETED_AT DESC, prod.DISCONTINUED_DATE DESC, prod.ACTIVE DESC, prod.HIDDEN, prod.CREATED_AT DESC) SKU_BRAND
		,	FIRST_VALUE(COALESCE(tvend.NAME, avend.VENDOR_NAME)) IGNORE NULLS OVER(PARTITION BY COALESCE(prod.CUSTOM_PART_NUMBER, prod.PART_NUMBER) ORDER BY prod.DELETED_AT DESC, prod.DISCONTINUED_DATE DESC, prod.ACTIVE DESC, prod.HIDDEN, prod.CREATED_AT DESC) PART_NUM_BRAND
		,	COALESCE(SKU_BRAND, PART_NUM_BRAND) BRAND
		,	FIRST_VALUE(prod.TITLE) IGNORE NULLS OVER(PARTITION BY sa.SELLER_SKU, amktpl.COUNTRY_CODE ORDER BY prod.DELETED_AT DESC, prod.DISCONTINUED_DATE DESC, prod.ACTIVE DESC, prod.HIDDEN, prod.CREATED_AT DESC) PRODUCT_TITLE
		,	FIRST_VALUE(prod.WHOLESALE_PRICE) IGNORE NULLS OVER(PARTITION BY sa.SELLER_SKU, amktpl.COUNTRY_CODE ORDER BY prod.DELETED_AT DESC, prod.DISCONTINUED_DATE DESC, prod.ACTIVE DESC, prod.HIDDEN, prod.CREATED_AT DESC) WHOLESALE_PRICE

		FROM
			THREEPN_PUBLIC.SELLER_ASINS sa
			JOIN THREEPN_PUBLIC.ASINS asin ON sa.ASIN_ID = asin.ID
				AND	asin._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			LEFT JOIN THREEPN_PUBLIC.VENDORS tvend ON asin.VENDOR_ID = tvend.ID
				AND	tvend._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			JOIN THREEPN_PUBLIC.AMAZON_MARKETPLACES amktpl ON asin.AMAZON_MARKETPLACE_ID = amktpl.ID
				AND amktpl._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			LEFT JOIN PATTERN_DB.PUBLIC.RPT_THREEPN_MAP tpmap ON sa.ID = tpmap.SELLER_ASIN_ID
			LEFT JOIN AMACZAR_PUBLIC.PRODUCTS prod ON tpmap.PRODUCT_ID = prod.ID
				AND	prod._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			LEFT JOIN AMACZAR_PUBLIC.VENDORS avend ON prod.VENDOR_ID = avend.ID
				AND avend._FIVETRAN_DELETED IS DISTINCT FROM TRUE

		WHERE
			sa._FIVETRAN_DELETED IS DISTINCT FROM TRUE
	;
	
	
	CREATE OR REPLACE TEMP TABLE _final AS
	
		SELECT
			pi.BRAND
		,	pi.VENDOR_ID
		,	DATE_TRUNC('WEEK', CONVERT_TIMEZONE(COALESCE(wh.TIME_ZONE, addtz.TIME_ZONE, 'UTC'), UDF_NTZ_TO_TZ('UTC', ship.START_TIME)))::DATE WEEK
		,	sell.NAME THREEPN_SELLER_NAME
	-- 	,	pi.ASIN
		,	sku.SKU SELLER_SKU
		,	pi.PART_NUM
		,	pi.PRODUCT_TITLE
		,	pi.WHOLESALE_PRICE
		,	SUM(si.QUANTITY_SHIPPED) QUANTITY
		
		FROM
			SHIPMENTCZAR_PUBLIC.SHIPMENTS ship
			JOIN SHIPMENTCZAR_PUBLIC.SHIPMENT_ITEMS si ON ship.ID = si.SHIPMENT_ID
				AND	si._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			JOIN SHIPMENTCZAR_PUBLIC.SELLER_SKUS sku ON si.SELLER_SKU_ID = sku.ID
				AND	sku._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			JOIN SHIPMENTCZAR_PUBLIC.MARKETPLACES mktpl ON ship.MARKETPLACE_ID = mktpl.ID
				AND	mktpl._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			JOIN THREEPN_PUBLIC.SELLER_KEYS sk ON ship.SELLER_KEY_ID = sk.ID
				AND	sk._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			JOIN THREEPN_PUBLIC.SELLERS sell ON sk.SELLER_ID = sell.ID
				AND	sell._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			LEFT JOIN _addresses_timezones addtz ON ship.SHIP_FROM_ADDRESS_ID = addtz.ADDRESS_ID
			LEFT JOIN AMACZAR_PUBLIC.WAREHOUSES wh ON ship.WAREHOUSE_ID = wh.ID
				AND	wh._FIVETRAN_DELETED IS DISTINCT FROM TRUE
			LEFT JOIN _product_info pi ON sku.SKU = pi.SELLER_SKU
	-- 			AND mktpl.COUNTRY_CODE = pi.COUNTRY_CODE
		
		WHERE
			ship._FIVETRAN_DELETED IS DISTINCT FROM TRUE
		AND	ship.SELLER_KEY_ID IS NOT NULL -- 3PN only
		AND	ship.SHIPMENT_STATUS_ID NOT IN (4, 8) -- 'Cancelled', 'Deleted'
		
		GROUP BY
			1, 2, 3, 4, 5, 6, 7, 8 --, 9
	;
	
	
	SELECT DISTINCT
		fin.BRAND
	,	fin.THREEPN_SELLER_NAME
	,	fin.WEEK
	
	FROM
		_final fin
	
	WHERE
		TRUE"""

	if vendor_names != None:
		seller_vendor_week_combos_query += f"""
	AND	fin.BRAND IN ({str(vendor_names)[1:-1]})"""

	if seller_names != None:
		seller_vendor_week_combos_query += f"""
	AND	fin.THREEPN_SELLER_NAME IN ({str(seller_names)[1:-1]})"""

	if start_week != None:
		seller_vendor_week_combos_query += f"""
	AND	fin.WEEK >= '{start_week}'"""

	if end_week != None:
		seller_vendor_week_combos_query += f"""
	AND	fin.WEEK <= '{end_week}'"""
	
	seller_vendor_week_combos_query +="""
	
	ORDER BY
		fin.BRAND
	,	fin.THREEPN_SELLER_NAME
	,	fin.WEEK
	;"""

	common.standard_logger.debug(f'seller_vendor_week_combos_query: {seller_vendor_week_combos_query}')

	seller_vendor_week_combos = database.snowflake_query_string(seller_vendor_week_combos_query)[-1]

	# filter names have to match exactly with those in the report (including casing)
	seller_filter_name = 'Seller'
	vendor_filter_name = 'Brand'
	week_filter_name = 'Week'

	base_file_name = 'Seller Invoice'

	num_invoices_created = 0

	for seller_vendor_week_combo in seller_vendor_week_combos:
		common.standard_logger.debug(f'seller_vendor_week_combo: {seller_vendor_week_combo}')

		vendor_filter_value = seller_vendor_week_combo[0]
		seller_filter_value = seller_vendor_week_combo[1]
		week_filter_value = seller_vendor_week_combo[2]

		filters = []
		
		week_filter = {'filter_name': week_filter_name, 'filter_value': str(week_filter_value)}
		seller_filter = {'filter_name': seller_filter_name, 'filter_value': seller_filter_value}
		vendor_filter = {'filter_name': vendor_filter_name, 'filter_value': vendor_filter_value}
		
		filters.append(week_filter)
		filters.append(seller_filter)
		filters.append(vendor_filter)
		threepn_seller_invoice_view = tableau_online.View.get(tableau_online.site, invoice_view_id)

		view_pdf = threepn_seller_invoice_view.get_pdf(filters)

		view_pdf_file_size = sys.getsizeof(view_pdf)

		common.standard_logger.debug(f'view_pdf_file_size: {view_pdf_file_size}')

		# if view_pdf_file_size > 84640: # size of report if no data

		file_name = base_file_name + ' - ' + vendor_filter_value + ' - ' + seller_filter_value + ' - ' + str(week_filter_value)
		file_name = file_name.replace('/', '-')
		folder_path = f'{output_folder}/{vendor_filter_value}/{seller_filter_value}/'
		full_file_path_name = folder_path + file_name + '.pdf'
		full_file_path = Path(full_file_path_name)

		common.ensure_dir(folder_path)

		full_file_path.write_bytes(view_pdf)
		common.standard_logger.info(f'Created Invoice {full_file_path_name}')

		num_invoices_created += 1


	# 	TODO: Remove blank pages. Tried with PyPDF2, pdfrw, and other methods but was unsuccessful.

	common.standard_logger.info(f'Created {num_invoices_created} invoice(s).')
	
	return num_invoices_created


if __name__ == "__main__":
	common.standard_logger.debug("File is being run directly")
	
	args = get_cmd_parameters()
	main_arguments = common.return_args(vars(args).items())
 
	main(*main_arguments)

