Use Python 3.8

You'll likely need to install XCode Command-line Developer Tools first if on Mac.
<br><br><br>

Create virtual environment doing something like the following in the directory where you want it:

`python3 -m venv tableau_utils_venv`


Activate the virtual environment in your terminal session by executing the following inside of the bin folder in the venv folder:

`source activate`<br><br><br>


#### Once the venv is activated, run the following to set it up for the project:

Upgrade PIP:

`pip install --upgrade pip`


Install all Snowflake Connector-required packages by executing the following in the root directory of the project:

`pip install -r snowflake_connector_requirements.txt`


Install our repository requirements:

`pip install -r requirements.txt`


##### Disable Snowflake telemetry (often causes errors) in following file:

[virtual_env_name]/lib/python3.8/site-packages/snowflake/connector/telemetry.py

Set `self._enabled` = False in the init method of `class TelemetryClient`

Comment out all code in the add_log_to_batch, try_add_log_to_batch, and send_batch methods and put `pass` as their body instead
<br><br><br>
Add credentials and properties for connecting to Tableau and Snowflake near top of file.<br><br><br>
Execute by running `python main.py`
<br><br><br>
Snowflake connector docs:
https://docs.snowflake.com/en/user-guide/python-connector-install.html
<br><br>

### Sample Usage:

##### Change permissions for all projects, workbooks, and views:

	view_permission_capabilities = ['AddComment', 'ChangePermissions', 'Delete', 'ExportData', 'ExportImage', 'Filter', 'Read', 'ShareView', 'ViewComments', 'ViewUnderlyingData', 'WebAuthoring']
	workbook_permission_capabilities = ['AddComment', 'ChangeHierarchy', 'ChangePermissions', 'Delete', 'ExportData', 'ExportImage', 'ExportXml', 'Filter', 'Read', 'ShareView', 'ViewComments', 'ViewUnderlyingData', 'WebAuthoring', 'Write']
	view_permission_capability_mode = 'Allow' # Not used because the workbook permission is either cascaded to the view (if cascade_to_views = True) or it's locked to the workbook
	workbook_permission_capability_mode = 'Allow'

	project_permission_capabilities = ['Read', 'Write']
	project_permission_capability_mode = 'Allow'
	default_project_workbook_permission_capabilities = workbook_permission_capabilities
	default_project_datasource_permission_capabilities = ['ChangePermissions', 'Connect', 'Delete', 'ExportXml', 'Read', 'Write']

	project_default_permissions = []
	project_default_datasource_permissions_object = {'object_type': 'datasource', 'permission_capabilities': default_project_datasource_permission_capabilities, 'permission_capability_mode': 'Deny'}
	project_default_workbook_permissions_object = {'object_type': 'workbook', 'permission_capabilities': default_project_workbook_permission_capabilities, 'permission_capability_mode': 'Deny'}
	project_default_permissions.append(project_default_datasource_permissions_object)
	project_default_permissions.append(project_default_workbook_permissions_object)

	workbooks = Workbook.find(site)
	projects = site.get_projects()

	for project in projects:
		if 'finance' in project.name.lower():
			logging.info(f'Project: {project}')
			project.add_group_permissions(site, bi_group_id, project_permission_capabilities, project_permission_capability_mode)
			project.add_default_group_permissions(site, bi_group_id, project_default_permissions)

			if project.content_permissions != 'LockedToProject':
				for workbook in workbooks:
					if workbook.project_id == project.id:
						workbook.add_group_permissions(site, bi_group_id, workbook_permission_capabilities, workbook_permission_capability_mode, view_permission_capabilities, True)`


##### Change permissions for views of individual workbook:

	views = master_product_list_workbook.get_views(site)
	for view in views:
		logging.info(f'view: {view}')
		view.add_group_permissions(view, finance_upworkers_group_id, view_permission_capabilities, permission_mode, permission_mode)`


##### Generate 3PN Invoices:
	
	threepn_seller_invoice_view_id = 'ad13d3e8-bf32-4884-9578-21d7319b3fd1' # View ID for 3PN Seller Invoice - Printable

	threepn_seller_view = View.get(site, threepn_seller_invoice_view_id)
	logging.debug(f'view: {threepn_seller_view}')

	seller_names = ['BrandHere']
	start_week = '2021-03-8'
	output_folder = '/Users/[your_user_name]/Desktop/'

	generate_3pn_seller_invoices(threepn_seller_view, output_folder, start_week=start_week, seller_names=seller_names)



##### Update Workbook Connections:

	num_connection_updates = 0

	workbooks = Workbook.find(site)

	for workbook in workbooks:
		if workbook.name == '3PN Ordering Report':
		if workbook.project_id == 'f0d5040f-83a4-4cc7-b4dd-ce616450e8c6': # Borderless - Core Reports
		logging.debug(f'workbook: {workbook}')
		workbook_connections = workbook.get_connections(site)
		for workbook_connection in workbook_connections:
			logging.info(f'workbook_connection: {workbook_connection}')
			
			if workbook_connection['connection_type'] == 'postgres': # Tableau API updates all connection types (aka all Postgres connections) for a single workbook at once, even if you specify only one connection ID
				update_connection(site, workbook, workbook_connection['connection_id'], None, None, 'username', 'password')
				num_connection_updates += 1

			if workbook_connection['connection_type'] == 'dropbox': # Don't use - updating "works", but for some reason, all refreshes will fail thereafter with "unable to authenticate"
				update_connection(site, workbook, workbook_connection['connection_id'], None, None, 'username', 'password')
				num_connection_updates += 1
			
			if workbook_connection['connection_type'] == 'snowflake': # No way to update role or warehouse
				update_connection(site, workbook, workbook_connection['connection_id'], None, None, 'username', 'password')
				num_connection_updates += 1
			
			if workbook_connection['connection_type'] == 'google-sheets': # API gives unhelpful error each time "ApiCallError: Code: 400039. Summary: Bad Request. Detail: There was a problem updating connection"
				update_connection(site, workbook, workbook_connection['connection_id'], None, None, 'username', 'password')
				num_connection_updates += 1
					
		
	logging.info(f'Finished {num_connection_updates} connection update(s).')



##### Download Tableau Data Source & Write to Snowflake:

	ts_events_datasource_id = 'd398510b-7ed4-40c7-a560-d08464033063'
	output_path = 'C:\\Users\<USER>\Documents\Git Folder\\tableau_utils'

	ts_events_datasource = Datasource.get(site, ts_users_datasource_id)
	downloaded_ts_events_file_paths = ts_events_datasource.download(site, output_path, hyper_output_file_name=ts_events_datasource.name, extract_as_hyper=True, delete_zip_file=True)

	sign_out(site)
	df_data = convert_hyper_file_to_dataframe(downloaded_ts_events_file_paths['full_hyper_file_path'], 'Extract')

	logging.info('Setting data types...')
	df_data.columns = map(str.upper, df_data.columns)
	df_data['EVENT_DATE'] = df_data['EVENT_DATE'].astype(str) # Convert to string for formatting purposes - database will automatically convert back.
	
	cnx = snowflake_connect()

	logging.info('Attempting to write to Snowflake..')
	write_pandas(cnx, df_data, 'TABLEAU_ONLINE_USAGE', schema="PUBLIC", database="PATTERN_DB")