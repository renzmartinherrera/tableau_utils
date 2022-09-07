import os
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import requests # Contains methods used to make HTTP requests
import common 
from pathlib import Path
import zipfile
import pantab
from dotenv import load_dotenv
import urllib


load_dotenv()


TABLEAU_API_VERSION = '3.13'

TABLEAU_SERVER_ADDRESS = os.getenv('TABLEAU_SERVER_ADDRESS')
TABLEAU_SITE_NAME = os.getenv('TABLEAU_SITE_NAME')
TABLEAU_USER_NAME = os.getenv('TABLEAU_USER_NAME')
TABLEAU_PASSWORD = os.getenv('TABLEAU_PASSWORD')

xmlns = {'t': 'http://tableau.com/api'}

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
			common.standard_logger.info('No views returned')
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
			common.standard_logger.debug(f'Project: {project}')

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

		common.standard_logger.info(f'Adding Project Permissions to project {self.name}...')

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
		# common.standard_logger.info(xml_request)

		server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)

		common.standard_logger.info(f'Added Project Permissions to project {self.name}.')


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

		common.standard_logger.info(f'Adding default Project Permissions to project {self.name}...')

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
			# common.standard_logger.info(xml_request)

			server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
			_check_status(server_response, 200)

			common.standard_logger.info(f"Added default Project Permissions to project {self.name} for object_type {project_default_permission['object_type']}.")


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

		common.standard_logger.info(f'Adding Workbook Permissions to workbook {self.name}...')

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
		# common.standard_logger.info(xml_request)

		server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)

		common.standard_logger.info(f'Added Workbook Permissions to workbook {self.name}.')

		if self.show_tabs == 'false' and cascade_to_views == True:
			views = self.get_views(site)

			for view in views:
				# common.standard_logger.info(f'view: {view}')
				view.add_group_permissions(view, group_id, view_permission_capabilities, permission_mode)	


	def get_connections(self, site):
		common.standard_logger.debug('Getting workbook connections...')

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/workbooks/{self.id}/connections"
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		common.standard_logger.debug('Finished querying for connections.')

		# Find all connection dbs
		workbook_connections = server_response.findall('.//t:connection', namespaces=xmlns)
		# common.standard_logger.debug(f'workbook_connections: {workbook_connections}')

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

		common.standard_logger.debug(f'Found {num_data_source_connections} data source connection(s).')
		# common.standard_logger.debug(f'data_source_connections: {data_source_connections}')

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

		# common.standard_logger.info(f'url: {url}')

		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		# common.standard_logger.info(f'_check_status: {_check_status}')
		_check_status(server_response, 200)
		# common.standard_logger.info(f'server_response: {server_response}')
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		view_tree = server_response.findall('.//t:view', namespaces=xmlns)

		views_list = []

		for view_element in view_tree:
			view_id = view_element.get('id')
			view = View.get(site, view_id)
			views_list.append(view)

		if len(views_list) == 0:
			common.standard_logger.info('No views returned')
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

		# common.standard_logger.info(f'url: {url}')
		
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

		# common.standard_logger.info(f'self: {self}')

		# common.standard_logger.info(f'workbook: {workbook}')

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

		common.standard_logger.debug(f'self: {self}')

		if isinstance(entity, Site):
			common.standard_logger.debug('it is a Site')
			url = base_url + 'workbooks?pageSize=1000'
		elif isinstance(entity, View):
			common.standard_logger.debug('it is a View')
			parent_workbook_id = entity.parent_workbook_id
			url = base_url + f'workbooks/{parent_workbook_id}'
		elif isinstance(entity, User):
			common.standard_logger.debug('it is a User')
			user_id = entity.id
			url = base_url + f'users/{user_id}//workbooks?pageSize=1000' # two // intentional?
		else:
			raise Exception('Bad entity type!')

		common.standard_logger.debug(f'Workbook.find url: {url}')
		
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		# Find all workbook ids
		workbook_elements = server_response.findall('.//t:workbook', namespaces=xmlns)
		# common.standard_logger.info('workbook_elements: {}'.format(workbook_elements))

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
			# common.standard_logger.info('Workbook: {}'.format(workbook))

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

		# common.standard_logger.info(f'url: {url}')
		
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)
		server_response = ET.fromstring(_encode_for_display(server_response.text))

		# Find all workbook ids
		view_element = server_response.findall('.//t:view', namespaces=xmlns)[0]

		# common.standard_logger.info(f'workbook_element: {workbook_element}')

		# common.standard_logger.info(f'self: {self}')

		# common.standard_logger.info(f'workbook: {workbook}')

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
			common.standard_logger.debug(f"view.add_group_permissions.xml_request: {xml_request}")

			common.standard_logger.info('Adding View Permissions to view {}...'.format(view.name))

			try:
				server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': auth_token})
				_check_status(server_response, 200)
				common.standard_logger.info(f'Added View Permissions to view {self.name}.')
			except ApiCallError as error:
					common.standard_logger.error('error: {}', error)
		
		else:
			common.standard_logger.info(f'View permissions not added because they are locked to Workbook (workbook.show_tabs = false) for workbook {workbook}.')

		
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
			common.standard_logger.info('No views returned')
		else:
			return views_list


	def get_pdf(self, filters=None):
		"""
		filters is a list of key:value filters
		Returns a PDF file of a view passed in

		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/views/{self.id}/pdf"

		common.standard_logger.debug(f'url: {url}')

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
				common.standard_logger.debug(f'url_appendage: {url_appendage}')
				
				counter += 1
				if counter < num_filters:
					url_appendage += '&'
				
				url += url_appendage
		
		common.standard_logger.debug(f'url: {url}')
		
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

		# common.standard_logger.info(f'url: {url}')
		
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
			common.standard_logger.info('No datasources returned')
		else:
			return datasources_list

	def download(self, site, output_folder='/', extract_as_hyper=False, hyper_output_file_name=None, delete_zip_file=False):
		"""
		Downloads & saves tdsx zip file.
		'site'           			site that the user is signed into
		'output_folder'				Full path to destination directory to save datasource. '/' required at end of path.
		'extract_as_hyper'			(Optional) Boolean option to extract hyper from downloaded zip file.
		'hyper_output_file_name'	(Optional) Desired datasource file name.
		'delete_zip_file'			(Optional) Boolean choice whether to delete original zip file or not.
		"""

		url = f"{site.server_address}/api/{TABLEAU_API_VERSION}/sites/{site.site_id}/datasources/{self.id}/content"
		common.standard_logger.info(f"Downloading {self.name} datasource..")
		server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
		_check_status(server_response, 200)

		hyper_zip_download = server_response.content

		file_name = self.name
		file_name = file_name.replace('/', '-')
		common.ensure_dir(output_folder)
		full_file_path_string = output_folder + file_name + '.zip'
		full_file_path = Path(full_file_path_string)

		common.standard_logger.info(f"Writing downloaded file to {full_file_path_string}")
		try:
			full_file_path.write_bytes(hyper_zip_download)
			common.standard_logger.info(f"Write complete.")
		except Exception as error:
			common.standard_logger.error(f"Write failed. Error: {error}")		

		full_hyper_file_path = None
		if extract_as_hyper == True:
			full_hyper_file_path = extract_hyper_from_tdsx_file(full_file_path_string, output_folder, hyper_output_file_name)

		if delete_zip_file == True:	
			os.remove(full_file_path_string)
			common.standard_logger.info(f"Deleted original file: {full_file_path_string}")
			
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
			common.standard_logger.info(f"Created {full_hyper_file_path}")
	
	if delete_zip_file == True:
		os.remove(input_file_path)
		common.standard_logger.info(f"Deleted original file: {input_file_path}")
	
	return full_hyper_file_path


def convert_hyper_file_to_dataframe(input_file_path, table_name, delete_hyper_file=False):
	"""
	Extracts hyper file from the tdsx zip file.
	'input_file_path'       Filepath of hyper file
	'table_name'			Table name inside hyper file
	'delete_hyper_file'		(Optional) Boolean choice whether to delete original hyper file or not.
	"""

	common.standard_logger.info(f"Converting Hyper {input_file_path} to DataFrame...")
	df_data = pantab.frame_from_hyper(input_file_path, table=table_name)

	if delete_hyper_file == True:
		os.remove(input_file_path)
		common.standard_logger.info(f"Deleted original file: {input_file_path}")
	
	return df_data


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


def sign_in(server, username, password, site_name):
	"""
	Signs in to the server specified with the given credentials
	'server'   specified server address
	'username' is the name (not ID) of the user to sign in as.
			   Note that most of the functions in this example require that the user
			   have server administrator permissions.
	'password' is the password for the user.
	'site_name'     is the name (as a string) of the site on the server to sign in to. The
			   default is "", which signs in to the default site.
	Returns the authentication token and the site ID.
	"""
	
	signin_url = f'{server}/api/{TABLEAU_API_VERSION}/auth/signin'

	common.standard_logger.debug(f'signin_url: {signin_url}')

	# Builds the request
	xml_request = ET.Element('tsRequest')
	credentials_element = ET.SubElement(xml_request, 'credentials', name=username, password=password)
	ET.SubElement(credentials_element, 'site', contentUrl=site_name)
	xml_request = ET.tostring(xml_request)

	# Make the request to server
	common.standard_logger.debug(f"XML_Request: {xml_request}")
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

	common.standard_logger.info('Signed out.')
	
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
		common.standard_logger.info(f'Group.ID: {group.group_id}. Group.name: {group.group_name}')


auth_token, site_id, user_id = sign_in(TABLEAU_SERVER_ADDRESS, TABLEAU_USER_NAME, TABLEAU_PASSWORD, TABLEAU_SITE_NAME)
site = Site(site_id, TABLEAU_SITE_NAME, TABLEAU_SERVER_ADDRESS, auth_token)


def main():

	pass


if __name__ == "__main__":
	common.standard_logger.debug("File is being run directly")
	
	main()
