'''
install:
sudo pip uninstall fabric
sudo pip install fabric==1.8.1
sudo pip install pycrypto

run :
fab -R test nss_deploy
fab -R 123 nss_deploy_test
fab -R nss-ambari nss_storm_deploy
'''

from fabric.api import *

env.roledefs = {
    'nss-ambari': ['10.8.0.1'],
}

env.user   = "ubuntu"
#env.password = "pwd"
env.key_filename = 'C:\\user\\john\your-perm.pem'

def nss_storm_deploy():
    run('rm -f /home/ubuntu/test_upload/nss_storm-1.0-SNAPSHOT.jar')
    upload = put('../nss_storm/target/nss_storm-1.0-SNAPSHOT.jar', '/home/ubuntu/')
    run('scp nss_storm-1.0-SNAPSHOT.jar ubuntu@ip.us-west-2.compute.internal:/home/ubuntu')
    upload.succeeded
