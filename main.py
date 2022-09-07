'hello world'
import sys, os, types, logging, urllib
from datetime import datetime, timedelta
import requests # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
from pathlib import Path
import snowflake.connector
import xml.dom.minidom
import zipfile
import pantab
import pandas
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
snowflake_logger = logging.getLogger('snowflake')
snowflake_logger.setLevel(logging.WARNING) # Limit logging output by Snowflake module


TABLEAU_API_VERSION = '3.9'

TABLEAU_SERVER_ADDRESS = os.getenv('TABLEAU_SERVER_ADDRESS')
TABLEAU_SITE_NAME = os.getenv('TABLEAU_SITE_NAME')
TABLEAU_USER_NAME = os.getenv('TABLEAU_USER_NAME')
TABLEAU_PASSWORD = os.getenv('TABLEAU_PASSWORD')

SNOWFLAKE_USER_NAME = os.getenv('SNOWFLAKE_USER_NAME')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT_NAME = os.getenv('SNOWFLAKE_ACCOUNT_NAME')
SNOWFLAKE_ROLE_NAME = os.getenv('SNOWFLAKE_ROLE_NAME')


xmlns = {'t': 'http://tableau.com/api'}

if sys.version[0] == '3': raw_input=input


class ApiCallError(BaseException):
	def __init__(self, code, summary, detail):
		self.code = code
		self.summary = summary
		self.detail = detail

	def __str__(self):
		return f"Code: {self.code}. Summary: {self.summary}. Detail: {self.detail}."


class UserDefinedFieldError(Exception):
	pass


class User:
	def __init__(self, user_id, user_name):
		self.user_id = user_id
		self.user_name = user_name
		self.server_address = server_address
		self.auth_token = auth_token
		

	def __str__(self):
		return f"User ID: {self.user_id}. User Name: {self.user_name}"


class Group:
	def __init__(self, group_id, group_name):
		self.group_id = group_id
		self.group_name = group_name
		

	def __str__(self):
		return f"Group ID: {self.group_id}. Group Name: {self.group_name}"


class Site:
	def __init__(self, site_id, site_name, server_address, auth_token):
		self.site_id = site_id
		self.site_name = site_name
		self.server_address = server_address
		self.auth_token = auth_token
		

	def __str__(self):
		return f"Server Address: {self.server_address}. Site ID: {self.site_id}. Site Name: {self.site_name}"


	def get_views(self):
		"""
		Queries all existing views on the current site.
		'server'            specified server address
		'site_id'           ID of the site that the user is signed into
		"""

		url = site.server_address + f"/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/views?pageSize=1000"
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		view_tree = server_response.findall('.//t:view', namespaces=xmlns)

		views_list = []
		counter = 0

		for view_element in view_tree:
			workbooks = view_element.findall('.//t:workbook', namespaces=xmlns)
			workbook_id = workbooks[0].get('id')
			view = View(view_element, workbook_id)
			counter += 1
			if counter == 1:
				Workbook.find(view)
			views_list.append(view)

		if len(views_list) == 0:
			logging.info('No views returned')
		else:
			return views_list


	def get_projects(self):
		"""
		Queries all existing projects on the current site.
		"""

		url = f"{self.server_address}/api/{TABLEAU_API_VERSION}/sites/{self.site_id}/projects?pageSize=1000&fields=_all_"

		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))
		
		# Find all projects
		project_elements = server_response.findall('.//t:project', namespaces=xmlns)

		projects_list = []

		for project_element in project_elements:
			content_counts_element = project_element.findall('.//t:contentsCounts', namespaces=xmlns)[0]
			
			project_count = content_counts_element.get('projectCount')
			workbook_count = content_counts_element.get('workbookCount')
			view_count = content_counts_element.get('viewCount')
			datasource_count = content_counts_element.get('datasourceCount')

			owner_element = project_element.findall('.//t:owner', namespaces=xmlns)[0]
			owner_email_address = owner_element.get('email')

			project = Project(project_element, owner_email_address, project_count, workbook_count, view_count, datasource_count)
			logging.debug(f'Project: {project}')

			projects_list.append(project)

		return projects_list


class Project:
	def __init__(self, project_element, owner_email_address, num_projects, num_workbooks, num_views, num_datasources):
		self.id = project_element.get('id')
		self.name = project_element.get('name')
		self.owner_email_address = owner_email_address
		self.content_permissions = project_element.get('contentPermissions')
		self.description = project_element.get('description')
		self.is_top_level_project = project_element.get('topLevelProject')
		self.parent_project_id = project_element.get('parentProjectId')
		self.controlling_permissions_project_id = project_element.get('controllingPermissionsProjectId')
		self.num_views = num_views
		self.num_workbooks = num_workbooks
		self.num_projects = num_projects
		self.num_datasources = num_datasources
		self.created_at = project_element.get('createdAt')
		self.updated_at = project_element.get('updatedAt')


	def __str__(self):
		return f"Name: {self.name}. ID: {self.id}. Owner: {self.owner_email_address}. Content permissions: {self.content_permissions}. Num projects: {self.num_projects}. Num workbooks: {self.num_workbooks}. Num datasources: {self.num_datasources}. Num views: {self.num_views}. Is top-level project: {self.is_top_level_project}. Description: {self.description}. Created at: {self.created_at}. Updated at: {self.updated_at}."


	def add_group_permissions(self, site, group_id, project_permission_capabilities, permission_mode):
		"""
		Adds permissions for one project.
		'site'                    				Site object
		'group_id'                  			ID of the group for whom to add the permission
		'project_permission_capabilities'   	List of permissions to add to a project. Available options: 'ProjectLeader' (available for "Allow" only), 'Read', 'Write'
		'permission_mode'           			Mode to set for the permissions_capabilities (available options: Allow or Deny)
		"""

		logging.info(f'Adding Project Permissions to project {self.name}...')

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/projects/{self.id}/permissions"

		xml_request = ET.Element('tsRequest')
		permissions_element = ET.SubElement(xml_request, 'permissions')
		project_element = ET.SubElement(permissions_element, 'project', id=self.id)
		grantee_capabilities_element = ET.SubElement(permissions_element, 'granteeCapabilities')
		group_element = ET.SubElement(grantee_capabilities_element, 'group', id=group_id)
		capabilities_element = ET.SubElement(grantee_capabilities_element, 'capabilities')

		for project_permission_capability in project_permission_capabilities:
			capability_element = ET.SubElement(capabilities_element, 'capability', name=project_permission_capability, mode=permission_mode)

		xml_request = ET.tostring(xml_request)
		# logging.info(xml_request)

		server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)

		logging.info(f'Added Project Permissions to project {self.name}.')


	def add_default_group_permissions(self, site, group_id, project_default_permissions):
		"""
		Adds default (cascading to future objects) permissions for one project.
		'site'                    								Site object
		'group_id'                  							ID of the group for whom to add the permission
		'default_project_workbook_permission_capabilities'   	List of default workbook permissions to add to a project. Available options: 'AddComment', 'ChangeHierarchy', 'ChangePermissions', 'Delete', 'ExportData', 'ExportImage', 'ExportXml', 'Filter', 'Read', 'ShareView', 'ViewComments', 'ViewUnderlyingData', 'WebAuthoring', 'Write'
		'default_project_datasource_permission_capabilities'   	List of default datasource permissions to add to a project. Available options: 'ChangePermissions', 'Connect', 'Delete', 'ExportXml', 'Read', 'Write
		'permission_mode'           							Mode to set for the permissions_capabilities (available options: Allow or Deny)
		'object_types'											List of object types to apply the permissions to. Avialable options: 'datasources', 'flows', 'workbooks'
		'project_default_permissions'							List of permissions objects with the following properties: object_type (string ('datasource', 'flow', or 'workbook')), permission_capabilities (list), permission_capability_mode (string ('Approve' or 'Deny')).
		"""

		logging.info(f'Adding default Project Permissions to project {self.name}...')

		base_url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/projects/{self.id}/default-permissions"

		for project_default_permission in project_default_permissions:
			url = base_url + f"/{project_default_permission['object_type']}s"

			xml_request = ET.Element('tsRequest')
			permissions_element = ET.SubElement(xml_request, 'permissions')
			project_element = ET.SubElement(permissions_element, 'project', id=self.id)
			grantee_capabilities_element = ET.SubElement(permissions_element, 'granteeCapabilities')
			group_element = ET.SubElement(grantee_capabilities_element, 'group', id=group_id)
			capabilities_element = ET.SubElement(grantee_capabilities_element, 'capabilities')

			for project_default_permission_capability in project_default_permission['permission_capabilities']:
				capability_element = ET.SubElement(capabilities_element, 'capability', name=project_default_permission_capability, mode=project_default_permission['permission_capability_mode'])

			xml_request = ET.tostring(xml_request)
			# logging.info(xml_request)

			server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
			_check_status(server_response, 200)

			logging.info(f"Added default Project Permissions to project {self.name} for object_type {project_default_permission['object_type']}.")


class Workbook:
	def __init__(self, workbook_element, project_id, owner_email_address, num_views):
		self.name = workbook_element.get('name')
		self.webpage_url = workbook_element.get('webpageUrl')
		self.id = workbook_element.get('id')
		self.show_tabs = workbook_element.get('showTabs')
		self.project_id = project_id
		self.owner = owner_email_address
		self.num_views = num_views
		self.created_at = workbook_element.get('createdAt')
		self.updated_at = workbook_element.get('updatedAt')


	def __str__(self):
		return f"Name: {self.name}. ID: {self.id}. URL: {self.webpage_url}. Project ID: {self.project_id}. Owner: {self.owner}. Show Tabs: {self.show_tabs}. Created At: {self.created_at}. Updated At: {self.updated_at}."


	def add_group_permissions(self, site, group_id, workbook_permission_capabilities, permission_mode, view_permission_capabilities=None, cascade_to_views=False):
		"""
		Adds permissions for one workbook for one group.
		'site'                    				Site object
		'group_id'                  			ID of the group for whom to add the permission
		'workbook_permission_capabilities'   	List of permissions to add. Available options: 'AddComment', 'ChangeHierarchy', 'ChangePermissions', 'Delete', 'ExportData', 'ExportImage', 'ExportXml', 'Filter', 'Read', 'ShareView', 'ViewComments', 'ViewUnderlyingData', 'WebAuthoring', 'Write'
		'view_permission_capabilities'			List of permissions to add to a view if cascade_to_views is True. Available options: 'AddComment', 'ChangeHierarchy', 'ChangePermissions', 'Delete', 'ExportData', 'ExportImage', 'Filter', 'Read', 'ShareView', 'ViewComments', 'ViewUnderlyingData', 'WebAuthoring'
		'permission_mode'           			Mode to set for the permissions_capabilities (available options: Allow or Deny)
		'cascade_to_views'						Boolean to control if permissions are also cascaded to workbook's views
		"""

		logging.info(f'Adding Workbook Permissions to workbook {self.name}...')

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/workbooks/{self.id}/permissions"

		xml_request = ET.Element('tsRequest')
		permissions_element = ET.SubElement(xml_request, 'permissions')
		workbook_element = ET.SubElement(permissions_element, 'workbook', id=self.id)
		grantee_capabilities_element = ET.SubElement(permissions_element, 'granteeCapabilities')
		group_element = ET.SubElement(grantee_capabilities_element, 'group', id=group_id)
		capabilities_element = ET.SubElement(grantee_capabilities_element, 'capabilities')

		for workbook_permission_capability in workbook_permission_capabilities:
			capability_element = ET.SubElement(capabilities_element, 'capability', name=workbook_permission_capability, mode=permission_mode)

		xml_request = ET.tostring(xml_request)
		# logging.info(xml_request)

		server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)

		logging.info(f'Added Workbook Permissions to workbook {self.name}.')

		if self.show_tabs == 'false' and cascade_to_views == True:
			views = self.get_views(site)

			for view in views:
				# logging.info(f'view: {view}')
				view.add_group_permissions(view, group_id, view_permission_capabilities, permission_mode)	


	def get_connections(self, site):
		logging.debug('Getting workbook connections...')

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/workbooks/{self.id}/connections"
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		logging.debug('Finished querying for connections.')

		# Find all connection dbs
		workbook_connections = server_response.findall('.//t:connection', namespaces=xmlns)
		# logging.debug(f'workbook_connections: {workbook_connections}')

		data_source_connections = []

		num_data_source_connections = 0

		for connection in workbook_connections:
			data_source_connection = {}

			data_source_connection['connection_id'] = connection.get('id')
			data_source_connection['server_address'] = connection.get('serverAddress')
			data_source_connection['connection_type'] = connection.get('type')
			data_source_connection['server_port'] = connection.get('serverPort')
			data_source_connection['user_name'] = connection.get('userName')

			data_source_connections.append(data_source_connection)

		num_data_source_connections = len(data_source_connections)

		logging.debug(f'Found {num_data_source_connections} data source connection(s).')
		# logging.debug(f'data_source_connections: {data_source_connections}')

		return data_source_connections

		# else:
		# 	error = "No connections found."
		# 	raise LookupError(error)


	def get_views(self, site):
		"""
		Queries all existing views on the current site.
		'site'           site that the user is signed into
		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/workbooks/{self.id}/views"

		# logging.info(f'url: {url}')

		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		# logging.info(f'_check_status: {_check_status}')
		_check_status(server_response, 200)
		# logging.info(f'server_response: {server_response}')
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		view_tree = server_response.findall('.//t:view', namespaces=xmlns)

		views_list = []

		for view_element in view_tree:
			view_id = view_element.get('id')
			view = View.get(site, view_id)
			views_list.append(view)

		if len(views_list) == 0:
			logging.info('No views returned')
		else:
			return views_list


	@classmethod
	def get(self, site, workbook_id):
		"""
		Finds an existing workbook by ID
		'server'            specified server address
		'auth_token'        authentication token that grants user access to API calls
		'user_id'           ID of user with access to workbooks
		'site_id'           ID of the site that the user is signed into
		'workbook_id'       (Optional) ID of workbook
		Returns a workbook object.
		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/workbooks/{workbook_id}"

		# logging.info(f'url: {url}')
		
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		workbook_element = server_response.findall('.//t:workbook', namespaces=xmlns)[0]
		project_element = server_response.findall('.//t:project', namespaces=xmlns)[0]
		owner_element = server_response.findall('.//t:owner', namespaces=xmlns)[0]
		view_elements = server_response.findall('.//t:view', namespaces=xmlns)

		num_views = len(view_elements)
		owner_email_address = owner_element.get('name')
		project_id = project_element.get('id')

		workbook = Workbook(workbook_element, project_id, owner_email_address, num_views)

		# logging.info(f'self: {self}')

		# logging.info(f'workbook: {workbook}')

		return workbook


	@classmethod
	def find(self, entity = None):
		"""
		Queries all existing workbooks for the given entity type
		'server'            specified server address
		'site_id'           ID of the site that the user is signed into
		entity       		Site object, View object, or User object
		Returns tuples for each workbook, containing its id and name.
		"""

		base_url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/"

		logging.debug(f'self: {self}')

		if isinstance(entity, Site):
			logging.debug('it is a Site')
			url = base_url + 'workbooks?pageSize=1000'
		elif isinstance(entity, View):
			logging.debug('it is a View')
			parent_workbook_id = entity.parent_workbook_id
			url = base_url + f'workbooks/{parent_workbook_id}'
		elif isinstance(entity, User):
			logging.debug('it is a User')
			user_id = entity.id
			url = base_url + f'users/{user_id}//workbooks?pageSize=1000' # two // intentional?
		else:
			raise Exception('Bad entity type!')

		logging.debug(f'Workbook.find url: {url}')
		
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		# Find all workbook ids
		workbook_elements = server_response.findall('.//t:workbook', namespaces=xmlns)
		# logging.info('workbook_elements: {}'.format(workbook_elements))

		# Tuples to store each workbook information:(workbook_id, workbook_name)
		# workbooks = [workbook.get('id') for workbook in workbook_elements]

		workbooks_list = []

		for workbook_element in workbook_elements:
			project_element = workbook_element.findall('.//t:project', namespaces=xmlns)[0]
			owner_element = workbook_element.findall('.//t:owner', namespaces=xmlns)[0]
			view_elements = workbook_element.findall('.//t:view', namespaces=xmlns)

			num_views = len(view_elements)
			owner_email_address = owner_element.get('name')
			project_id = project_element.get('id')

			workbook = Workbook(workbook_element, project_id, owner_email_address, num_views)
			# logging.info('Workbook: {}'.format(workbook))

			workbooks_list.append(workbook)

		return workbooks_list


class View:
	def __init__(self, view_element, parent_workbook_id):
		self.name = view_element.get('name')
		self.id = view_element.get('id')
		self.url = view_element.get('contentUrl')
		self.parent_workbook_id = parent_workbook_id
		self.server = site.server_address
		self.site_id = site.site_id


	def __str__(self):
		return "Name: {}. ID: {}. Parent Workbook ID: {}".format(self.name, self.id, self.parent_workbook_id)


	@classmethod
	def get(self, site, view_id):
		"""
		Finds an existing view by ID
		'site'				Site object of the site signed into.
		'view_id'       	ID of view
		Returns a view object.
		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/views/{view_id}"

		# logging.info(f'url: {url}')
		
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		# Find all workbook ids
		view_element = server_response.findall('.//t:view', namespaces=xmlns)[0]

		# logging.info(f'workbook_element: {workbook_element}')

		# logging.info(f'self: {self}')

		# logging.info(f'workbook: {workbook}')

		workbook_element = view_element.findall('.//t:workbook', namespaces=xmlns)
		workbook_id = workbook_element[0].get('id')
		view = View(view_element, workbook_id)

		return view


	def add_group_permissions(self, view, group_id, view_permission_capabilities, permission_mode):
		"""
		Adds permissions for one view for one group.
		'view'                   		View object to receive the permissions
		'group_id'                  	ID of the group for whom to add the permission
		'view_permission_capabilities'  List of permissions to add to a view. Available options: 'AddComment', 'ChangeHierarchy', 'ChangePermissions', 'Delete', 'ExportData', 'ExportImage', 'Filter', 'Read', 'ShareView', 'ViewComments', 'ViewUnderlyingData', 'WebAuthoring'
		'permission_mode'           	Mode to set for the permissions_capabilities (available options: Allow or Deny)
		"""

		workbook = Workbook.get(site, view.parent_workbook_id)

		if workbook.show_tabs == 'false':
			url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/views/{view.id}/permissions"
			xml_request = ET.Element('tsRequest')
			permissions_element = ET.SubElement(xml_request, 'permissions')
			view_element = ET.SubElement(permissions_element, 'view', id=view.id)
			grantee_capabilities_element = ET.SubElement(permissions_element, 'granteeCapabilities')
			group_element = ET.SubElement(grantee_capabilities_element, 'group', id=group_id)
			capabilities_element = ET.SubElement(grantee_capabilities_element, 'capabilities')
			for view_permission_capability in view_permission_capabilities:
				capability_element = ET.SubElement(capabilities_element, 'capability', name=view_permission_capability, mode=permission_mode)

			xml_request = ET.tostring(xml_request)
			logging.debug(f"view.add_group_permissions.xml_request: {xml_request}")

			logging.info('Adding View Permissions to view {}...'.format(view.name))

			try:
				server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
				_check_status(server_response, 200)
				logging.info(f'Added View Permissions to view {self.name}.')
			except ApiCallError as error:
					logging.error('error: {}', error)
		
		else:
			logging.info(f'View permissions not added because they are locked to Workbook (workbook.show_tabs = false) for workbook {workbook}.')

		
	@classmethod
	def find(self, site):
		"""
		Queries all existing views on the current site.
		'server'            specified server address
		'site'           	site that the user is signed into
		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/views?pageSize=1000"
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		view_tree = server_response.findall('.//t:view', namespaces=xmlns)

		views_list = []

		for view_element in view_tree:
			workbooks = view_element.findall('.//t:workbook', namespaces=xmlns)
			workbook_id = workbooks[0].get('id')
			view = View(view_element, workbook_id)
			views_list.append(view)

		if len(views_list) == 0:
			logging.info('No views returned')
		else:
			return views_list


	def get_pdf(self, filters=None):
		"""
		filters is a list of key:value filters
		Returns a PDF file of a view passed in

		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/views/{self.id}/pdf"

		logging.debug(f'url: {url}')

		num_filters = len(filters)
		counter = 0
		
		if filters != None:
			url += '?'
			for filter in filters:
				if filter['filter_name'] == 'Brand':
					brand =  filter['filter_value']
				elif filter['filter_name'] == 'Seller':
					seller =  filter['filter_value']
				
				url_appendage = 'vf_' + urllib.parse.quote_plus(filter['filter_name']) + '=' + urllib.parse.quote_plus(filter['filter_value']) # URL encode filter names and values
				
				counter += 1
				if counter < num_filters:
					url_appendage += '&'
				
				url += url_appendage
		
		logging.debug(f'url: {url}')
		
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)

		pdf = server_response.content

		return pdf


class Datasource:

	def __init__(self, datasource_element):
		self.name = datasource_element.get('name')
		self.id = datasource_element.get('id')
		self.url = datasource_element.get('contentUrl')
		self.num_connected_workbooks = datasource_element.get('connected-workbooks-count-number')
		self.type = datasource_element.get('datasource-type')
		self.created_at = datasource_element.get('datetime-created')
		self.updated_at = datasource_element.get('datetime-updated')
		self.size = datasource_element.get('data-source-size-number')
		self.server = site.server_address
		self.site_id = site.site_id


	def __str__(self):
		return f"Name: {self.name}. ID: {self.id}. URL: {self.url}"


	@classmethod
	def get(self, site, datasource_id):
		"""
		Finds an existing datasource by ID
		'site'					Site object of the site signed into.
		'datasource_id'       	ID of datasource
		Returns a datasource object.
		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/datasources/{datasource_id}"

		# logging.info(f'url: {url}')
		
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		# Find all datasource ids
		datasource_element = server_response.findall('.//t:datasource', namespaces=xmlns)[0]

		datasource = Datasource(datasource_element)

		return datasource


	@classmethod
	def find(self, site):
		"""
		Queries all existing datasources on the current site.
		'site'           	site that the user is signed into
		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/datasources?pageSize=1000"
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		datasource_tree = server_response.findall('.//t:datasource', namespaces=xmlns)

		datasources_list = []

		for datasource_element in datasource_tree:
			datasource = Datasource(datasource_element)
			datasources_list.append(datasource)

		if len(datasources_list) == 0:
			logging.info('No datasources returned')
		else:
			return datasources_list


	def download(self, site, output_folder, extract_as_hyper=False, hyper_output_file_name=None, delete_zip_file=False):
		"""
		Downloads & saves tdsx zip file.
		'site'           			site that the user is signed into
		'output_folder'				Full path to destination directory to save datasource.
		'extract_as_hyper'			(Optional) Boolean option to extract hyper from downloaded zip file.
		'hyper_output_file_name'	(Optional) Desired datasource file name.
		'delete_zip_file'			(Optional) Boolean choice whether to delete original zip file or not.
		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/datasources/{self.id}/content"
		logging.info(f"Downloading {self.name} datasource..")
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)

		hyper_zip_download = server_response.content

		file_name = self.name
		file_name = file_name.replace('/', '-')
		folder_path = f'{output_folder}/'
		full_file_path_string = folder_path + file_name + '.zip'
		full_file_path = Path(full_file_path_string)

		ensure_dir(folder_path)
		logging.info(f"Writing downloaded file to {full_file_path_string}")
		try:
			full_file_path.write_bytes(hyper_zip_download)
			logging.info(f"Write complete.")
		except Exception as error:
			logging.error(f"Write failed. Error: {error}")		

		full_hyper_file_path = None
		if extract_as_hyper == True:
			full_hyper_file_path = extract_hyper_from_tdsx_file(full_file_path_string, folder_path, hyper_output_file_name)

		if delete_zip_file == True:	
			os.remove(full_file_path_string)
			logging.info(f"Deleted original file: {full_file_path_string}")
			
		return {'tdsx_zip_file_path': full_file_path, 'full_hyper_file_path': full_hyper_file_path}


def extract_hyper_from_tdsx_file(input_file_path, output_folder_path, output_file_name=None, delete_zip_file=False):
	"""
	Extracts hyper file from the tdsx zip file.
	'input_file_path'           	Filepath of tdsx zip file
	'output_folder_path'			Destination directory to save datasource.
	'output_file_name'				(Optional) Desired datasource file name.
	'delete_zip_file'				(Optional) Boolean choice whether to delete original zip file or not. 
	"""

	input_file_name = input_file_path.split('/')[-1].split('.')[0]
	if output_file_name == None:
		output_file_name = input_file_name
	
	if output_file_name.endswith('.hyper') == False:
		output_file_name += '.hyper'

	with zipfile.ZipFile(input_file_path) as hyper_zip_file:
		for zip_info in hyper_zip_file.infolist():
			if zip_info.filename[-1] == '/' or not zip_info.filename.endswith('.hyper'):
				continue
			full_hyper_file_path = output_folder_path + output_file_name
			zip_info.filename = output_file_name #Remove subfolders from filename.
			hyper_zip_file.extract(zip_info, output_folder_path)
			logging.info(f"Created {full_hyper_file_path}")
	
	if delete_zip_file == True:
		os.remove(input_file_path)
		logging.info(f"Deleted original file: {input_file_path}")
	
	return full_hyper_file_path


def convert_hyper_file_to_dataframe(input_file_path, table_name, delete_hyper_file=False):
	"""
	Extracts hyper file from the tdsx zip file.
	'input_file_path'       Filepath of hyper file
	'table_name'			Table name inside hyper file
	'delete_hyper_file'		(Optional) Boolean choice whether to delete original hyper file or not.
	"""

	logging.info(f"Converting Hyper {input_file_path} to DataFrame...")
	df_data = pantab.frame_from_hyper(input_file_path, table=table_name)

	if delete_hyper_file == True:
		os.remove(input_file_path)
		logging.info(f"Deleted original file: {input_file_path}")
	
	return df_data


def remove_duplicates_on_tableau_online_usage_snowflake_table():
	"""
	upload_tableau_online_usage_data uses insert rather than merge. This ensures there are no duplicates after the insert.
	"""

	logging.info(f"Removing duplicates on Snowflake table...")
	
	delete_statement = """

		USE SCHEMA PUBLIC;
		CREATE OR REPLACE TEMP TABLE _latest_event_ingestions AS
	
			SELECT
				tou.EVENT_ID 
			,	MAX(tou.DATETIME_INGESTED_AT) LATEST_INGESTION
			,	COUNT(DISTINCT tou.DATETIME_INGESTED_AT) NUM_INGESTIONS
						
			FROM
				PATTERN_DB.PUBLIC.TABLEAU_ONLINE_USAGE tou
				
			GROUP BY
				1

			HAVING 
				NUM_INGESTIONS > 1
		;


		DELETE
		
		FROM 
			PATTERN_DB.PUBLIC.TABLEAU_ONLINE_USAGE tou
		
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
	
	num_deleted_records = snowflake_query_string(delete_statement)[-1]

	# Has to be a for loop despite being only one row.
	for record in num_deleted_records:
		num_deleted_records = record[0]
		logging.debug(f'num_deleted_records: {num_deleted_records}')

	return num_deleted_records


def upload_tableau_online_usage_data(output_path, num_look_back_days=1, table_name='TABLEAU_ONLINE_USAGE', schema='PUBLIC', database='PATTERN_DB'):
	"""
	Downloads TDSX file, extracts to Hyper, converts it to Dataframe, changes the date data types, inserts into Snowflake table,
	and removes potential duplicate rows.
	'output_path'			Filepath to store data source file.
	'num_look_back_days'	(Optional) Number of days to insert.
	'table_name'			(Optional) Default value is 'TABLEAU_ONLINE_USAGE'.
	'schema'				(Optional) Default value is 'PUBLIC'.
	'database'				(Optional) Default value is 'PATTERN_DB'.
	"""

	ts_events_datasource_id = 'd398510b-7ed4-40c7-a560-d08464033063'

	ts_events_datasource = Datasource.get(site, ts_events_datasource_id)
	downloaded_ts_events_file = ts_events_datasource.download(site, output_path, hyper_output_file_name=ts_events_datasource.name, extract_as_hyper=True, delete_zip_file=True)

	df_data = convert_hyper_file_to_dataframe(downloaded_ts_events_file['full_hyper_file_path'], 'Extract')

	logging.info('Setting data types...')
	df_data['datetime_ingested_at'] = str(datetime.utcnow()) + '-00:00'
	df_data.columns = map(str.upper, df_data.columns)
	df_data['EVENT_DATE'] = df_data['EVENT_DATE'].astype(str) # Convert to string for formatting purposes - database will automatically convert back.
	cnx = snowflake_connect()

	logging.info('Attempting to write to Snowflake..')
	write_pandas_result = write_pandas(cnx, df_data, table_name, schema=schema, database=database)
	write_pandas_success_bool = write_pandas_result[0]
	write_pandas_chunks = write_pandas_result[1]
	write_pandas_rows = write_pandas_result[2]
	write_pandas_return_message = write_pandas_result[3]

	logging.debug(f'write_pandas_success_bool: {write_pandas_success_bool}, write_pandas_chunks: {write_pandas_chunks}, write_pandas_rows: {write_pandas_rows}, write_pandas_return_message: {write_pandas_return_message}')
	num_deleted_records = remove_duplicates_on_tableau_online_usage_snowflake_table()

	num_rows_inserted = write_pandas_rows

	logging.info(f'num_deleted_records: {num_deleted_records}. num_rows_inserted: {num_rows_inserted}.')

	return {'num_deleted_records': num_deleted_records, 'num_rows_inserted': num_rows_inserted}


def _encode_for_display(text):
	"""
	Encodes strings so they can display as ASCII in a Windows terminal window.
	This function also encodes strings for processing by xml.etree.ElementTree functions.
	Returns an ASCII-encoded version of the text.
	Unicode characters are converted to ASCII placeholders (for example, "?").
	"""
	return text.encode('ascii', errors="backslashreplace").decode('utf-8')


def _check_status(server_response, success_code):
	"""
	Checks the server response for possible errors.
	'server_response'       the response received from the server
	'success_code'          the expected success code for the response
	Throws an ApiCallError exception if the API call fails.
	"""

	if server_response.status_code != success_code:
		parsed_response = ET.fromstring(server_response.text)

		# Obtain the 3 xml tags from the response: error, summary, and detail tags
		error_element = parsed_response.find('t:error', namespaces=xmlns)
		summary_element = parsed_response.find('.//t:summary', namespaces=xmlns)
		detail_element = parsed_response.find('.//t:detail', namespaces=xmlns)

		# Retrieve the error code, summary, and detail if the response contains them
		code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
		summary = summary_element.text if summary_element is not None else 'unknown summary'
		detail = detail_element.text if detail_element is not None else 'unknown detail'
		error_message = '{0}: {1} - {2}'.format(code, summary, detail)
		raise ApiCallError(code, summary, detail)
	
	return


def sign_in(server, username, password, site):
	"""
	Signs in to the server specified with the given credentials
	'server'   specified server address
	'username' is the name (not ID) of the user to sign in as.
			   Note that most of the functions in this example require that the user
			   have server administrator permissions.
	'password' is the password for the user.
	'site'     is the ID (as a string) of the site on the server to sign in to. The
			   default is "", which signs in to the default site.
	Returns the authentication token and the site ID.
	"""
	
	signin_url = f'{server}/api/{TABLEAU_API_VERSION}/auth/signin'

	logging.debug(f'signin_url: {signin_url}')

	# Builds the request
	xml_request = ET.Element('tsRequest')
	credentials_element = ET.SubElement(xml_request, 'credentials', name=username, password=password)
	ET.SubElement(credentials_element, 'site', contentUrl=site)
	xml_request = ET.tostring(xml_request)

	# Make the request to server
	logging.debug(f"XML_Request: {xml_request}")
	server_response = requests.post(signin_url, data=xml_request)
	_check_status(server_response, 200)

	# ASCII encode server response to enable displaying to console
	server_response = _encode_for_display(server_response.text)

	# Reads and parses the response
	parsed_response = ET.fromstring(server_response)

	# Gets the auth token and site ID
	token = parsed_response.find('t:credentials', namespaces=xmlns).get('token')
	site_id = parsed_response.find('.//t:site', namespaces=xmlns).get('id')
	user_id = parsed_response.find('.//t:user', namespaces=xmlns).get('id')
	return token, site_id, user_id


def sign_out(site):
	"""
	Destroys the active session and invalidates authentication token.
	'server'        specified server address
	'auth_token'    authentication token that grants user access to API calls
	"""
	
	url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/auth/signout"
	server_response = requests.post(url, headers={'x-tableau-auth': site.auth_token})
	_check_status(server_response, 204)

	logging.info('Signed out.')
	
	return


def get_groups(site):
	"""
	Queries all existing user groups on the current site.
	'site'           Site that the user is signed into
	"""

	url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/groups?pageSize=300"
	server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
	_check_status(server_response, 200)
	server_response = ET.fromstring(_encode_for_display(server_response.text))

	group_tree = server_response.findall('.//t:group', namespaces=xmlns)

	for group_element in group_tree:
		group_id = group_element.get('id')
		group_name = group_element.get('name')
		group = Group(group_id, group_name)
		logging.info(f'Group.ID: {group.group_id}. Group.name: {group.group_name}')


def update_connection(site, workbook, connection_id, server_address=None, server_port=None, user_name=None, password=None, embed_password='True'):
	url = site.server_address + f'/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/workbooks/{workbook.id}/connections/{connection_id}'

	xml_request = ET.Element('tsRequest')

	connection_element = ET.SubElement(xml_request, 'connection')
	if server_address:
		connection_element.set('serverAddress', server_address)
	if server_port:
		connection_element.set('serverPort', server_port)
	if user_name:
		connection_element.set('userName', user_name)
	if password:
		connection_element.set('password', password)
	
	connection_element.set('embedPassword', embed_password)

	xml_request = ET.tostring(xml_request)

	logging.debug(f'xml_request: {xml_request}')

	server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
	_check_status(server_response, 200)

	logging.debug('Finished updating connection.')

	return


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


def ensure_dir(file_path):
	directory = os.path.dirname(file_path)
	if not os.path.exists(directory):
		os.makedirs(directory)
		logging.info(f'Made directory: {directory}')
	return


def generate_3pn_seller_invoices(view, output_folder, vendor_names=None, seller_names=None, start_week=None, end_week=None):
	"""
	Generates Borderless invoices for 3PN sellers that we can submit to Amazon to show chain of custody.
	'view'   		Tableau view object to be used as the invoice
	'output_folder'	Full path to destination directory to save invoices.
	'vendor_names'	(Optional) Comma-separated list of vendor names for which to generate invoices.
	'seller_names'	(Optional) Comma-separated list of vendor names for which to generate invoices.
	'start_week'	(Optional) Starting week for which to generate invoices in format YYYY-MM-DD. Date should be a Monday.
	'end_week'		(Optional) Starting week for which to generate invoices in format YYYY-MM-DD. Date should be a Monday.
	
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
		,	COALESCE(tvend.ID, prod.VENDOR_ID) VENDOR_ID
		,	COALESCE(tvend.NAME, avend.VENDOR_NAME) BRAND
	-- 	,	asin.ASIN
		,	FIRST_VALUE(COALESCE(prod.CUSTOM_PART_NUMBER, prod.PART_NUMBER)) IGNORE NULLS OVER(PARTITION BY sa.SELLER_SKU, amktpl.COUNTRY_CODE ORDER BY prod.DELETED_AT DESC, prod.DISCONTINUED_DATE DESC, prod.ACTIVE DESC, prod.HIDDEN, prod.CREATED_AT DESC) PART_NUM
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

	logging.debug(f'seller_vendor_week_combos_query: {seller_vendor_week_combos_query}')

	seller_vendor_week_combos = snowflake_query_string(seller_vendor_week_combos_query)[-1]

	# filter names have to match exactly with those in the report (including casing)
	seller_filter_name = 'Seller'
	vendor_filter_name = 'Brand'
	week_filter_name = 'Week'

	base_file_name = 'Seller Invoice'

	num_invoices_created = 0

	for seller_vendor_week_combo in seller_vendor_week_combos:
		logging.debug(f'seller_vendor_week_combo: {seller_vendor_week_combo}')

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

		view_pdf = view.get_pdf(filters)

		view_pdf_file_size = sys.getsizeof(view_pdf)

		logging.debug(f'view_pdf_file_size: {view_pdf_file_size}')

		# if view_pdf_file_size > 84640: # size of report if no data

		file_name = base_file_name + ' - ' + vendor_filter_value + ' - ' + seller_filter_value + ' - ' + str(week_filter_value)
		file_name = file_name.replace('/', '-')
		folder_path = f'{output_folder}/{vendor_filter_value}/{seller_filter_value}/'
		full_file_path_name = folder_path + file_name + '.pdf'
		full_file_path = Path(full_file_path_name)

		ensure_dir(folder_path)

		full_file_path.write_bytes(view_pdf)
		logging.info(f'Created Invoice {full_file_path_name}')

		num_invoices_created += 1


	# 	TODO: Remove blank pages. Tried with PyPDF2, pdfrw, and other methods but was unsuccessful.

	logging.info(f'Created {num_invoices_created} invoice(s).')
	
	return num_invoices_created


def main():

	sign_out(site)


if __name__ == "__main__":
	logging.debug("File is being run directly")
	
	auth_token, site_id, user_id = sign_in(TABLEAU_SERVER_ADDRESS, TABLEAU_USER_NAME, TABLEAU_PASSWORD, TABLEAU_SITE_NAME)
	site = Site(site_id, TABLEAU_SITE_NAME, TABLEAU_SERVER_ADDRESS, auth_token)

	main()

