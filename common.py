import logging
import os

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO) 
standard_logger = logging


def ensure_dir(file_path):
	directory = os.path.dirname(file_path)
	if not os.path.exists(directory):
		os.makedirs(directory)
		standard_logger.info(f'Made directory: {directory}')
	return


def return_args(arguments):

	main_arguments = []

	for arg in arguments:
		key,value = arg
		arg_dict = {arg}
		standard_logger.debug(f"key: {key}. value: {value}")
		main_arguments.append(value)
	
	return main_arguments