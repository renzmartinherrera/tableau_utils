import tableau_online
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import requests # Contains methods used to make HTTP requests
import common
import argparse


def get_cmd_parameters():
	parser = argparse.ArgumentParser()
	"""
	WARNING, From Tableau Online API Docs: 
	"If the workbook contains multiple connections to the same data source type, 
	all the connections are updated. For example, if the workbook contains three connections to the same PostgreSQL database, 
	and you attempt to update the user name of one of the connections, the user name is updated for all three connections.
	Any combination of the attributes inside the <connection> element is valid. If no attributes are included, no update is made.
        'workbook_id'   			Tableau workbook id whose connection is to be updated."
		'connection_id'				Workbook's conection ID.
		'site'						(Optional) Tableau Online site. Default is already set on .env file.
		'connection_server_address'	(Optional) Database server address.
		'connection_server_port'	(Optional) Database server address port.
		'user_name'					(Optional) Database username.
		'password'					(Optional) Database password.
		'embed_password'			(Optional) Boolean to embed password. Default is true.
	"""

	parser.add_argument(
		"-w",
		"--workbook_id",
		type=str,
		help="Tableau workbook id whose connection is to be updated.",
		required=True
	)
	
	parser.add_argument(
        "-c",
        "--connection_id",
        type=str,
        help="Workbook's conection ID.",
		required=True
    )   

	parser.add_argument(
        "-s",
        "--site",
        type=str,
        help="(Optional) Tableau Online site. Default is already set on .env file."
    )    

	parser.add_argument(
        "-a",
        "--connection_server_address",
        type=str,
        help="(Optional) Database server address."
    )    	
	
	parser.add_argument(
        "-p",
        "--connection_server_port",
        type=str,
        help="(Optional) Database server address port."
    )    

	parser.add_argument(
        "-u",
        "--user_name",
        type=str,
        help="(Optional) Database username."
    )   
	
	 
	parser.add_argument(
        "-pw",
        "--password",
        type=str,
        help="(Optional) Database password."
    ) 
	
	 
	parser.add_argument(
        "-e",
        "--embed_password",
        type=str,
        help="(Optional) Boolean to embed password. Default is set to True."
    )

	args = parser.parse_args()

	return args


def main(workbook_id, connection_id, site=tableau_online.site, connection_server_address=None, connection_server_port=None, user_name=None, password=None, embed_password='True'):
	common.standard_logger.info(site)
	url = site.server_address + f'/api/{tableau_online.TABLEAU_API_VERSION}/sites/{site.site_id}/workbooks/{workbook_id}/connections/{connection_id}'

	xml_request = ET.Element('tsRequest')

	num_inputs = 0
	connection_element = ET.SubElement(xml_request, 'connection')
	if connection_server_address:
		connection_element.set('serverAddress', connection_server_address)
		num_inputs =+ 1
	if connection_server_port:
		connection_element.set('serverPort', connection_server_port)
		num_inputs =+ 1
	if user_name:
		connection_element.set('userName', user_name)
		num_inputs =+ 1
	if password:
		connection_element.set('password', password)
		num_inputs =+ 1
	
	if num_inputs < 1:
		common.standard_logger.info(f"num_inputs: {num_inputs}. From Tableau Online API Docs: Any combination of the attributes inside the <connection> element is valid. If no attributes are included, no update is made.")
	else:

		connection_element.set('embedPassword', embed_password)

		xml_request = ET.tostring(xml_request)

		common.standard_logger.debug(f'xml_request: {xml_request}')

		server_response = requests.put(url, data=xml_request, headers={'x-tableau-auth': tableau_online.auth_token})
		tableau_online._check_status(server_response, 200)

		common.standard_logger.debug('Finished updating connection.')

	return


if __name__ == "__main__":
	common.standard_logger.debug("File is being run directly")
	
	args = get_cmd_parameters()

	main_arguments = []

	for arg in vars(args).items():
		key,value = arg
		arg_dict = {arg}
		common.standard_logger.debug(f"value: {value}")
		main_arguments.append(value)
	
	main(*main_arguments)

