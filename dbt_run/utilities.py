#!/usr/bin/env python
import logging
import json
import requests
from pprint import pformat
from google.cloud import pubsub


logging.basicConfig(level=logging.INFO)


def get_metadata(path):
    url = f'http://metadata.google.internal/computeMetadata/v1/{path}'
    metadata_flavor = {'Metadata-Flavor' : 'Google'}
    return requests.get(url, headers=metadata_flavor).text

def get_project_id():
    try:
        return get_metadata('project/project-id')
    except:
        return None

def get_instance_name():
    try:
        return get_metadata('instance/hostname').split('.')[0]
    except:
        return None


import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def load_yaml(filepath):
    with open(filepath) as f:
        return yaml.load(f, Loader=Loader)
    return None

class Config:

    def __init__(self, config_file_path):
        self.__dict__['_config'] = load_yaml(config_file_path)

    def __repr__(self):
        return pformat(self._config)

    def __getitem__(self, name):
        return self._config.get(name)

    def __getattr__(self, name):
        return self.__getitem__(name)
    
    def __setitem__(self, name, value):
        logging.info(f'set {name} = {value}')
        if name not in self._config:
            self._config[name] = value
    
    def __setattr__(self, name, value):
        self.__setitem__(name, value)




class PubSubInterface:
    """
    @TODO: add service account prevision
    """
    def __init__(self, pubsub_client, instance_control_topic, dbt_control_topic):

        self.project_id = get_project_id()

        self.pubsub = pubsub_client

        self.instance_control_topic = instance_control_topic
        self.dbt_control_topic = dbt_control_topic
    
    def publish(self, topic, message):
        if not self.project_id:
            logging.info("No project_id found - assuming local testing")
            logging.info(f"Message on {topic} sent.")
        else:        
            topic_path = f'projects/{self.project_id}/topics/{topic}'
            resp = self.pubsub.publish(topic_path, json.dumps(message).encode())
            logging.debug(resp.result())    
    
    def dbt_command(self, message):
        self.publish(self.dbt_control_topic, message)

    def instance_command(self, instance_name, command):
        self.publish(
            self.instance_control_topic,
            {
                'instance': instance_name,
                'command': command
            }
        )

    def start_instance(self, instance_name):
        self.instance_command(instance_name, 'start')

    def shutdown_instance(self):
        self.instance_command(instance_name, 'stop')

    def shutdown_self(self):
        instance_name = get_instance_name()
        if instance_name:
            self.instance_command(instance_name, 'stop')

