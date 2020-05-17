
import json
import re
import requests


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

class Notifier():

    def __init__(self, instance_name, data_notifications_url):
        self.instance_name = instance_name
        self.data_notifications_url = data_notifications_url
    
    def send(self, run_id, command, text, pre_message=None):
        blocks = [{
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f'*{self.instance_name} : ({run_id})*\n _*{command}*_'
                    }
                }]
        if pre_message:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": pre_message
                }
            })

        lines = text.split('\n')

        groups = chunks(lines, 20)

        for group in groups:
            _text = '\n'.join(group)
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f'```{_text}```'
                }
            })

        data = {
            "text": f"DBT Run: {command}",
            "blocks": blocks
        }

        resp = requests.post(self.data_notifications_url, json=data)
