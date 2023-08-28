from airflow.models import Variable
from slack_sdk import WebClient
from datetime import datetime

class SlackAlert:
    def __init__(self, channel, token):
        self.channel = channel
        self.client = WebClient(token=token)

    def FailAlert(self, msg):
        text= f'''
        date : {datetime.today().strftime("%Y-%m-%d")}
        alert :
            Fail!
                dag id : {msg.get('task_instance').dag_id},         
                task id : {msg.get('task_instance').task_id},
        '''
        self.client.chat_postMessage(channel=self.channel, text=text)

# failure_token = Variable.get("SLACK_FAILURE_TOKEN")
# client = WebClient(token=failure_token)
# client.chat_postMessage(channel='#final_project', text='test~!~!~!')

