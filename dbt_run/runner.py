#!/usr/bin/env python
import uuid
import datetime
import pytz
import json
import re
import io
import os
import subprocess

from dbt_run.utilities import logging, load_yaml, get_instance_name


class Runner():

    def __init__(self, pubsubint, notifier, storage_client, config):

        self._config = config
        self._start = self.now()
        self._ts = int(self._start.timestamp()*1000)
        self.run_id = str(uuid.uuid1(clock_seq=self._ts))
        self.cloud_path = f'{self._config.project_name}/run/{self._start:%Y%m%d}/{self.run_id}'
        self.pubsubint = pubsubint
        self.notifier = notifier
        self.storage_client = storage_client
        self.catalog_bucket = self.storage_client.get_bucket(
            self._config.catalog_storage_bucket)

        self.setup(config.jobs)

    def now(self):
        d = datetime.datetime.now()
        timezone = pytz.timezone(self._config.timezone)
        return timezone.localize(d)

    def setup(self, jobs_config):

        current_hour = self._start.hour

        jobs = []
        
        for job in jobs_config:
            _schedule = job['schedule']
            if type(_schedule) is str:
                if '<hourly>' == _schedule:
                    schedule = [h for h in range(0,24)]
            
            elif type(_schedule) is int:
                schedule = [_schedule]
            
            elif type(_schedule) is list:
                schedule = _schedule
            else:
                raise Exception(f"Incorrectly formatted schedule {_schedule}")

            for hour in schedule:
                if current_hour == hour:
                    jobs.append(job)

        run_only = list(filter(lambda x: x.get('run_only') == True, jobs ))
        
        if len(run_only) > 0:
            jobs = run_only
        
        self.jobs = jobs

        logging.info(self.jobs)

    def run_jobs(self):

        instance_name = get_instance_name()

        if not instance_name:
            logging.info("running jobs locally")

        if not self.jobs:
            raise Exception("Run setup first!")
        
        self.run('clean')
        self.run('deps')
        self.run('compile')

        for job in self.jobs:
            base = ['--partial-parse']
            models = []
            if job.get('models') != '<all>':
                models = ['--model', job.get('models')]

            self.run(base + ['run'] + models)
            self.run(base + ['test'] + models)

        if instance_name and self._config.chain_instance:
            self.pubsubint.start_instance(self._config.chain_instance)

        self.run(['docs', 'generate'])

        self.upload_catalog()



           
    def run(self, cmd):
        
        if type(cmd) is not list:
            cmd = [cmd]

        end = ['--target', self._config.target]
        if self._config.profiles_dir:
            end = end + ['--profiles-dir', self._config.profiles_dir]

        cmd = ['dbt', '--log-format', 'json'] + cmd + end

        logging.info(f"Run command: {' '.join(cmd)}")

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        messages = []
        while True:
            line = p.stdout.readline().rstrip()

            if not line:
                break

            try:
                msg = json.loads(line)
            except:
                logging.info(line)
                break

            logging.info(msg.get('message'))

            message = re.sub(r'\x1b\[[0-9]{1,2}m', '', msg.get('message'), flags=re.MULTILINE)

            model = re.search(r'model ([\w]+\.[\w]+)', message)
            if model:
                model = model.group(1)

            command = re.search(r'\[\*?([\w]+)', message)
            if command:
                command = command.group(1)
            else:
                command = ''

            if model and command:
                self.pubsubint.dbt_command({
                    'model': model,
                    'command': command,
                    'run-by': ' '.join(cmd)
                })
            
            messages.append(re.sub(r'\*WARNING', '\g<0>*', message)) 
        
        report_text = '\n'.join([m for m in messages if m != "*"])

        report_text = re.sub('\n\*\n', '\n\n', report_text)

        pre_message = None

        if 'Done. PASS' in report_text:
            matches = re.findall(
                r'Done\. PASS\=(?P<pass>[0-9]+) WARN=(?P<warn>[0-9]+) ERROR=(?P<error>[0-9]+) SKIP=(?P<skip>[0-9]+) TOTAL=([0-9]+)'
                ,report_text
                ,re.MULTILINE
            )

            summary = dict(zip(['pass', 'warn', 'error', 'skip', 'total'],[int(i) for i in matches[0]]))

            if summary.get('error') > 0:
                error_count = summary.get('error')
                pre_message = f'@here note: _*{error_count} errors raised below*_'

        self.notifier.send(self.run_id, ' '.join(cmd), report_text, pre_message)

    def upload_file(self,filename):
        blob = self.catalog_bucket.blob(os.path.join(self.cloud_path, filename))
        blob.upload_from_filename(f'target/{filename}')
        logging.info(f"File {filename} uploaded to {self.cloud_path}")

    def upload_catalog(self):
        self.upload_file('catalog.json')
        self.upload_file('manifest.json')
        self.upload_file('run_results.json')