#!/usr/bin/env python
import os
import argparse
import logging

import subprocess
import io

from google.cloud import pubsub, storage

from dbt_run.utilities import Config, PubSubInterface, logging, load_yaml
from dbt_run.notifications import Notifier
from dbt_run.runner import Runner



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str)
    parser.add_argument('--profiles-dir', type=str, default='~/.dbt/')
    parser.add_argument('--target', type=str)
    args = parser.parse_args()
    
    if '~' in args.profiles_dir:
        profiles_dir = os.path.expanduser(args.profiles_dir)
    else:
        profiles_dir = os.path.abspath(args.profiles_dir)

    config = Config(args.config)

    config.profiles_dir = profiles_dir

    dbt_project = load_yaml('dbt_project.yml')

    if not dbt_project:
        raise Exception("Could not find DBT project")

    profiles = load_yaml(os.path.join(profiles_dir, 'profiles.yml'))

    target = args.target

    if not target:
        target = profiles.get(dbt_project.get('profile'), {}).get('target')
    
    config.target = target

    pubsub_publisher = pubsub.PublisherClient()
    storage_client = storage.Client()

    notify = Notifier(
        config.project_name,
        config.build_notifications_webhook
    )

    pubsubint = PubSubInterface(
        pubsub_publisher,
        config.pubsub_instance_control_topic,
        config.pubsub_dbt_job_topic
    )

    runner = Runner(
        pubsubint,
        notify,
        storage_client,
        config
    )

    runner.run_jobs()



if __name__ == "__main__":
    main()