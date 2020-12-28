import logging
import os
import requests


class AlertService:

    def send_message_to_slack(self, message_data):
        data = {'text': message_data}
        headers = {'Content-type': 'application/json'}
        requests.post(os.environ['SLACK_URL'], json=data, headers=headers)
