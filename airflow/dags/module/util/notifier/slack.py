from slack_sdk import WebClient
from datetime import datetime

# slack alert
class SlackAlert:
    def __init__(self, channel, token):
        self.channel = channel
        self.client = WebClient(token=token)

    # dag가 실패했을 때 slack에 알림 
    def FailAlert(self, msg):
        text= f'''
        🔥 Fail 🔥
        date : {datetime.today().strftime("%Y-%m-%d")}
        alert :
            dag id : {msg.get('task_instance').dag_id},         
            task id : {msg.get('task_instance').task_id}
        '''
        self.client.chat_postMessage(channel=self.channel, text=text)
