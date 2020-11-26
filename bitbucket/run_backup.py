from six.moves import configparser
import requests
import datetime
import boto3
from os import makedirs, system
import shutil
from os.path import exists, isdir
import sys, os
import zipfile
import subprocess as sub
import shlex


CONFIG_LOC = './config.cfg'


def read_config(section='', config_loc=CONFIG_LOC):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    # create parser and read ini configuration file
    parser = configparser.ConfigParser()

    parser.read(config_loc)

    # get section, default to mysql
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
        return db
    else:
        raise Exception('{0} not found in the {1} file'.format(section, CONFIG_LOC))


#params from config
API_USER = read_config('config')['api_user']
API_PASSWORD = read_config('config')['api_password']
API_URL = read_config('config')['api_url']
GIT_USER = read_config('config')['git_user']
GIT_PASSWORD = read_config('config')['git_password']
DIRECTORY = read_config('config')['directory']
BUCKET_NAME = read_config('config')['bucket_name']
SOURCE_MAIl_ALERT = read_config('config')['source_mail_alert']
DEST_MAIL_ALERT = read_config('config')['dest_mail_alert']

exclude = []
# add workspaces for backup
WORKSPACES = []


if sys.argv:
    if len(sys.argv) > 1 and sys.argv[1]:
        RUN_TYPE = sys.argv[1]
    else:
        print(datetime.datetime.now(), 'Invalid input: no run type defined e.g daily, weekly')
        sys.exit(2)
else:
    print((datetime.datetime.now(), 'Invalid input: no any parameters'))
    print((datetime.datetime.now(), 'Usage example: python BitBucket_backup.py <RUN_TYPE>'))
    print((datetime.datetime.now(), 'Usage example: python BitBucket_backup.py daily/weekly'))
    sys.exit(1)


class BitBackup:
    def __init__(self, api_user, api_password, api_url, git_user, git_password, directory, workspaces, bucket):
        self._api_user = api_user
        self._api_password = api_password
        self._api_url = api_url
        self._git_user = git_user
        self._git_password = git_password
        self._directory = directory
        self._workspaces = workspaces
        self._bucket = bucket

    def get_info(self, path):
        print("Starting get_info()")
        try:
            url = self._api_url + '/{}?pagelen=100'.format(path)
            print (url)
            response = requests.get(url, auth=requests.auth.HTTPBasicAuth(self._api_user, '#' + self._api_password))
            print(response.status_code)
            if response.status_code == 200:
                return response.json()
            else:
                print("Issue with getting information")
        except Exception as e:
            print (e)
            print("Failed to get info from bitbucket")

    def get_workspaces(self, data):
        for repository in data['values']:
            print(repository['links']['repositories'])

    def get_repositories(self, workspaces):
        repos = []
        for wp in workspaces:
            repositories = self.get_info('repositories/{}'.format(wp))
            for r in repositories['values']:
                repos.append((r['links']['clone'][0]['href']).split('@')[-1])
        print("Repositories for backup count: {}".format(len(repos)))
        return repos

    def upload_file_to_s3(self, fileName, days_back):
        print("Starting to upload backup of {} repository to s3".format(fileName))
        now = datetime.datetime.now()
        ndays = (now - datetime.timedelta(days=days_back)).strftime("%Y%m%d")
        s3 = boto3.client('s3', region_name='us-east-1')
        try:
            data = open(self._directory + '/backups/zipped/' + fileName, 'rb')
            response = s3.put_object(
                Bucket=self._bucket,
                Key='BitBucket_Backups/day_ts=' + ndays + '/' + fileName,
                Body=data
            )
            print("Uploading file to: ", '/BitBucket_Backups/day_ts=' + ndays + '/' + fileName)
            print(response)
            return True
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, 'line:', exc_tb.tb_lineno, sys.exc_info()[0], e)
            print("Failed to upload file to S3")
            return False

    def zip_file(self, fileName):
        # try:
        #     compression = zipfile.ZIP_DEFLATED
        # except:
        #     compression = zipfile.ZIP_STORED
        #
        # modes = {zipfile.ZIP_DEFLATED: 'deflated',
        #          zipfile.ZIP_STORED: 'stored',
        #          }
        #
        # print ('creating archive'
        # zf = zipfile.ZipFile(self._directory + '/backups/' + fileName, mode='w', allowZip64=True)
        # try:
        #     zf.write(self._directory + '/backups/' + fileName + '.zip', compress_type=compression)
        # except Exception as e:
        #     print("Failed to zip {}\n{}".format(fileName, e))
        # finally:
        #     print ('closing'
        #     zf.close()

        def zipdir(path, ziph):
            for root, dirs, files in os.walk(path):
                for file in files:
                    ziph.write(os.path.join(path, file), arcname=file)
        zipf = zipfile.ZipFile(fileName + '.zip', 'w', zipfile.ZIP_DEFLATED)
        zipdir(self._directory + '/backups/zipped/' + fileName, zipf)
        zipf.close()

    def remove_s3_backups(self, clean_up_days):
        now = datetime.datetime.now()
        days_back = 1
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self._bucket)
        while days_back <= clean_up_days:
            ndays = (now - datetime.timedelta(days=days_back)).strftime("%Y%m%d")
            try:
                bucket.objects.filter(Prefix="BitBucket_Backups/day_ts=" + ndays + "/").delete()
                days_back += 1
            except Exception as e:
                print("Failed to remove backup folder in s3 BitBucket_Backups/day_ts={}\n{}".format(ndays, e))
        print("Finished backup removal in s3 for last 6 days")

    def check_directories(self, backup_dir, backup_zip_dir):
        print("Starting verify_directories()")
        try:
            if not isdir(backup_dir):
                print('Created backup folder')
                makedirs(backup_dir)
            if not isdir(backup_zip_dir):
                print('Created backup zipped folder')
                makedirs(backup_zip_dir)
            return True
        except Exception as e:
            print("Failed to create backup\zipp backup folder")
            print(e)
            return False

    def check_local_backup(self, backup_dir, backup_zip_dir, repository, extension):
        print("Starting check_local_backup()")
        try:
            if isdir(backup_dir + repository):
                print ("Local backup exists, removing folder...", backup_dir + repository)
                shutil.rmtree(backup_dir + repository)
                print("Finished removing local backup of {}".format(repository))
            else:
                print("No local backup is found for repository {}".format(repository))
            if exists(backup_zip_dir + repository + extension):
                os.remove(backup_zip_dir + repository + extension)
                print("Finished removing local zipped backup of {}".format(repository))
            else:
                print("No local zipped backup is found for repository {}".format(repository))
        except Exception as e:
            print("Failed to remove local backup of repository:{}\n{}".format(repository, e))

    def download_bitbucket_repository(self, command, backup_dir, full_repo_name):
        repository = full_repo_name.split('/')[-1]
        try:
            args = shlex.split('{} https://{}:{}@{} {}'.format(command, self._git_user, self._git_password, full_repo_name,
                                                      backup_dir + repository))
            if self.run_cmd(args):

                print("Finished downloading repository {} from bitbucket".format(repository))
                return True
            else:
                print("Failed to run {} command for {}\n".format(command, repository))
                return False
        except Exception as e:
            print("Failed to run {} command for {}\n {}".format(command, repository, e))
            return False

    def check_repository_integrity(self, backup_dir, repository):
        print("Starting check_repository_itegrity()")
        os.chdir(backup_dir+repository)
        print(os.getcwd())
        args = shlex.split('git fsck --full')
        if self.run_cmd(args):
            print("Local Repository state is ok")
            return True
        else:
            print("Repository is not intact - abborting backup")
            return False

    def zip_bitbucket_repository(self, backup_dir, backup_zip_dir, extension, full_repo_name):
        repository = full_repo_name.split('/')[-1]
        try:
            tar_command = 'tar -zcvf {} -C {} {}'.format(backup_zip_dir + repository + extension, backup_dir,
                                                         repository)
            system(tar_command)
            print("Finished zipping repository {}".format(repository))
            return True
        except Exception as e:
            print("Failed to zip repository {}\n {}".format(repository, e))
            return False

    def check_for_backup(self, repositories):
        for r in repositories:
            repository = r.split('/')[-1]
            print(repository)

    def run_cmd(self, args):
        try:
            sub.check_call(args)
            print("complete sub.Popen")
            return True
        except sub.CalledProcessError as e:
            print(str(e))
            return False

    def send_alert(self, messagebody, message_subj="BitBucket backup alert"):
        print("begin send_alert()")
        msg = ''
        for m in messagebody:
            msg += '\n' + m
        try:
            emailmessage = "aws ses send-email --from" + " " + SOURCE_MAIl_ALERT + " " + \
                       "--to" + " " + DEST_MAIL_ALERT + " " \
                       "--subject" + " \"" + message_subj + "\" " + "--text \" " + \
                       msg + " \" --region us-east-1"
            sub.call(["bash", "-c", emailmessage])
        except Exception as e:
            print("Failed to send alert \n{}".format(e))

    def backup_repository(self, repositories):
        print("Starting backup_repository()")
        command = 'git clone --mirror'
        extension = '.tar.gz'
        backup_dir = self._directory + '/backups/'
        backup_zip_dir = backup_dir + 'zipped/'
        failed_backup = []
        counter = 0
        # run check for local directories
        if self.check_directories(backup_dir, backup_zip_dir):
            for r in repositories:
                repository = r.split('/')[-1]
                success = True
                print(repository)
                if repository in exclude:
                    continue
                else:
                    print("Working on {} repository".format(repository))
                    # run check for local backup, if exists - remove
                    self.check_local_backup(backup_dir, backup_zip_dir, repository, extension)
                    # download git repo from s3
                    if self.download_bitbucket_repository(command, backup_dir, r):
                        # zip downloaded repo from s3
                        if not self.check_repository_integrity(backup_dir, repository):
                            success = False
                            print("Failed check_repository_integrity repository {}".format(repository))
                        elif not self.zip_bitbucket_repository(backup_dir, backup_zip_dir, extension, r):
                            success = False
                            print("Failed zip_bitbucket_repository repository {}".format(repository))
                        elif not self.upload_file_to_s3(repository + extension, 0):
                            success = False
                            print("Failed upload_file_to_s3 repository {}".format(repository))
                    else:
                        print("Failed to download_bitbucket_repository repository {} from bitbucket".format(repository))
                        break
                    if success is False:
                        failed_backup.append(repository)
                    else:
                        counter += 1
        else:
            print("Check directories failed")
            exit(1)
        if len(failed_backup) > 0:
            print("Backup finished with errors")
            print("Failed repositories #{}".format(len(failed_backup)))
            #self.send_alert(failed_backup)
            return False
        else:
            print("Finished Backup successfully of #{} repositories".format(counter))
            return True


########################################################################################################################
bit_backup = BitBackup(API_USER, API_PASSWORD, API_URL, GIT_USER, GIT_PASSWORD, DIRECTORY, WORKSPACES, BUCKET_NAME)
repositories = bit_backup.get_repositories(bit_backup._workspaces)
if RUN_TYPE == 'weekly':
    if bit_backup.backup_repository(repositories):
        bit_backup.remove_s3_backups(6)
        exit(0)
    else:
        bit_backup.send_alert("Weekly BitBucket backup failed, please check")
        exit(1)
if RUN_TYPE == 'daily':
    if not bit_backup.backup_repository(repositories):
        print("Send mail")
        bit_backup.send_alert("Daily BitBucket backup failed, please check")
        exit(1)
if RUN_TYPE == 'check':
    bit_backup.check_for_backup(repositories)
    exit(0)


#bit_backup.backup_repository(repositories)


