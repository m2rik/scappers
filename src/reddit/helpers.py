import os
import json


def load_configuration(configuration_directory):
    configuration = {}
    with open(r'{}\config.json'.format(configuration_directory), 'r') as config_output:
        configuration = json.loads(config_output.read())
    return configuration
	

def remove_new_lines(text):
	return text.replace('\r\n', '\n').replace('\r', '\n').replace('\n', ' ').encode('ascii', 'ignore').decode('ascii')


def is_file_empty(file):
    rows = 0
    with open(file, 'r') as input_file:
        for line in input_file:
            rows += 1
    return rows == 0


def verify_directories(directories):
    for directory in ( directory for directory in directories if not os.path.exists(directory) ):
        os.makedirs(directory)
