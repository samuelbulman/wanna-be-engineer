# Standard library imports
import inspect
import datetime
import pytz

# Third party imports
from slack_sdk import WebClient

# Local imports
from modules.aws.secrets import fetch_secret


class Slack:
    """
    Use me to interact with your company's Slack instance. The following methods are made available:
        - `post_job_notification`: Posts a consistent job completion Slack notification upon the success or failure of a script in this repo.
    """
    
    def __init__(self):
        secret = fetch_secret("<insert secret name as stored in AWS Secrets Manager here>")
        oauth_token = secret["<insert key name for slack app oauth token here?"]
        self.slack_web_client = WebClient(token=oauth_token)
    

    def post_job_notification(
            self,
            is_successful_job:bool,
            default_channel:bool = True,
            channel_id:str = None, 
            exception:str = None,
            is_test_job:bool = False
        ):
        """Call me in a `try` block to send job notifications via Slack. Strategically place me, with the proper `is_successfull_job` flag to notify users of a jobs success or failure, and cause of the failure."""

        # first, get the name of the script that's calling this function
        frame = inspect.stack()[-1]
        calling_script = frame[1].split("/")[-1]

        # second, get the current timestamp value
        utc_timestamp = datetime.datetime.now(pytz.utc)
        local_timezone = pytz.timezone('US/Central')
        local_timestamp = utc_timestamp.astimezone(local_timezone)
        current_timestamp = local_timestamp.strftime("%Y-%m-%d %I:%M:%S %p")

        # intentionally set channel id to default slack app channel if `default_channel` is set to True
        # otherwise, optionally specify the channel to send slack alerts to
        
        if default_channel:
            self.channel = "<hard code default channel id here>"
        else:
            self.channel = channel_id
        
        if is_successful_job:
            if is_test_job:
                self.slack_web_client.chat_postMessage(channel=self.channel, text=f"[TEST JOB]\n\n:rocket:  *SUCCESSFUL JOB*  :rocket:\n\nScript: `{calling_script}`\nCompleted at: `{current_timestamp}`")
            else:
                self.slack_web_client.chat_postMessage(channel=self.channel, text=f":rocket:  *SUCCESSFUL JOB*  :rocket:\n\nScript: `{calling_script}`\nCompleted at: `{current_timestamp}`")
        else:
            if is_test_job:
                self.slack_web_client.chat_postMessage(channel=self.channel, text=f"[TEST JOB]\n\n:sadbutstillcool:  *FAILED JOB*  :sad_yeehaw:\n\nScript:  `{calling_script}`\nFailed at: `{current_timestamp}` with the following error: \n```{exception}```")
            else:
                self.slack_web_client.chat_postMessage(channel=self.channel, text=f"<!here>  :sadbutstillcool:  *FAILED JOB*  :sad_yeehaw:\n\nScript:  `{calling_script}`\nFailed at: `{current_timestamp}` with the following error: \n```{exception}```")