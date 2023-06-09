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
    run_command('aws s3 cp s3://eurekapatient-j1-dev/inbound-feed/dv-files/sparqToUPK_jbi_AllProducts_20230602014531.dat s3://eurekapatient-j1/prod-jbi/source-feeds/datavant/JBI/20230602/ --recursive')

   



