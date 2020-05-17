#!/usr/bin/env python

import subprocess
import logging
import io
import os
import json
from parse import parse, compile

logging.basicConfig(level=logging.INFO)

def run(event, context):
    # cmd = f'gs -dSAFER -dNOPAUSE -dBATCH -sDEVICE=png16m -r600 -sOutputFile="{output_filepath}" {input_filepath}'.split(
    #     ' ')
    # cmd = "../.env/bin/dbt --log-format json run --target prod"

    if os.environ.get('VIRTUAL_ENV'):
        env_bin = os.path.join( os.environ.get('VIRTUAL_ENV'), 'bin')
    
    logging.info(env_bin)
    
    cmd = ['dbt', '--log-format', 'json', 'run', '--target', 'prod', '--model', 'activities+']
    # cmd = "pwd"/home/goddard/projects/dccc-data
    # p = subprocess.run(cmd, shell=True, capture_output=True, universal_newlines=True)
    #  universal_newlines=True, c
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
       cwd='/home/goddard/projects/dccc-dbt-internal/', 
       env={'PATH': env_bin},
       universal_newlines=True
    )
    # stderr=subprocess.PIPE, stdout=subproccess.PIPE
    logging.info("process run")
    while True:
        line = p.stdout.readline().rstrip()

        if not line:
            break
        msg = json.loads(line)
        message = msg.get('message')

        logging.info(message)
   
    # logging.info(rc) 

# output = process.stdout.readline()
#         if output == '' and process.poll() is not None:
#             break
#         if output:
#             print output.strip()
    # outs, errs = p.communicate()
    # print(type(errs))
        # if message.startswith('FOUND'):
        #     pass
        # else: 
        # logging.info(parse("{:2}:{:2}:{:2} |", message))
        # 1 of 3 START table model prod_ext_src_pdi.activities
        
        # logging.info(msg.get('timestamp'))
        # logging.info(msg.get('message'))
        # logging.info(msg.get('extra'))

    # pubsub:
    # * job start
    # * job - each table complete
    # * job errors
    # * job complete status

    # stdout, stderr = p.communicate()
    # msg = io.StringIO()vvvvvv
    # while True:
    #     logging.info("should have stdout now")
    #     logging.info(type(p.stdout))
    #     # line = stdout.readline()
    #     logging.info(stdout)
    #     if p.poll() is not None:
    #         break
    # if p.stderr:
    #     logging.error(p.stderr)
    #     return



if __name__ == "__main__":
    # logging.info("test!")
    run(None, None)