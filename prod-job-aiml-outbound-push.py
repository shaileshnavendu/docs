import subprocess
import traceback
from s3_utils import *


def run_command(command):
    command_list = command.split(' ')

    try:
        print("Running shell command: \"{}\"".format(command))
        result = subprocess.run(command_list, stdout=subprocess.PIPE);
        # print("Command output:\n---\n{}\n---".format(result.stdout.decode('UTF-8')))
    except Exception as e:
        print("Exception: {}".format(e))
        print(traceback.format_exc())
        raise e
    else:
        print("shell command: \"{}\" executed successfully.".format(command))


if __name__ == "__main__":
    run_command('aws s3 sync s3://eurekapatient-j1/prod-jbi/config/params/test/ s3://aws-pp-ai-jbi-dev/inbound-feed/JBI/31129999/ --delete')



