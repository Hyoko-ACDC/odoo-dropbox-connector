from dotenv import load_dotenv
import dropbox
import redis
import os
import pytest
import time
from pathlib import Path
import json
import names

load_dotenv()

HOOK_WAITING_TIME = 30

NAME_1 = names.get_full_name()




REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = os.environ['REDIS_PORT']
REDIS_USER = os.environ['REDIS_USER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

# registers
REDIS_USER_DMS = os.environ['REDIS_USER_DMS']
REDIS_USER_ID_MAPPING = os.environ['REDIS_USER_ID_MAPPING']
REDIS_DOCUMENT_TEMPLATES = os.environ['REDIS_DOCUMENT_TEMPLATES']
REDIS_DBX_HOOKO_SUBSCRIBERS = os.environ['REDIS_DBX_HOOKO_SUBSCRIBERS'] # list but stored as a string (need eval())


DROPBOX_TOKEN = os.environ['DROPBOX_TOKEN']
DROPBOX_STUDENTS_PATH = os.environ['DROPBOX_STUDENTS_PATH']


dbx = dropbox.Dropbox(DROPBOX_TOKEN)
redis_client = redis.Redis(host=REDIS_HOST, 
                           port=REDIS_PORT,
                           username=REDIS_USER,
                           password=REDIS_PASSWORD,
                           decode_responses=True,
                           socket_timeout=None,
                           connection_pool=None,
                           charset='utf-8',
                           errors='strict')



print("THIS TEST MIGHT TAKE SOME TIME TO RUN SINCE WE HAVE TO WAIT FOR THE DROPBOX HOOK (WIAITNG TIME={}sec)".format(HOOK_WAITING_TIME))

# fixtures
@pytest.fixture
def student_paths():
  students_names = ["Rabi Jacob"]
  return [DROPBOX_STUDENTS_PATH + s for s in students_names]

@pytest.fixture
def one_student_name():
  return NAME_1

@pytest.fixture
def files_paths():
  p = Path('./files')
  return list(p.glob('**/*.py'))



def test_create_one_user(one_student_name):
  """Create a user and make sur that user is in the GED"""
  new_folder = dbx.files_create_folder(DROPBOX_STUDENTS_PATH + one_student_name)
  print(new_folder)
  time.sleep(HOOK_WAITING_TIME)
  # See if there is an entry in Redis
  user_folder_id_dict=json.loads(redis_client.get(REDIS_USER_ID_MAPPING))
  folder_id = user_folder_id_dict.get(DROPBOX_STUDENTS_PATH + one_student_name.lower(), None)
  assert folder_id == new_folder.id

def test_upload_simple_file(one_student_name):
  """Upload a file in user GED"""
  new_folder = dbx.files_create_folder(DROPBOX_STUDENTS_PATH + one_student_name)
  print(new_folder)
  time.sleep(HOOK_WAITING_TIME)
  # See if there is an entry in Redis
  user_folder_id_dict=json.loads(redis_client.get(REDIS_USER_ID_MAPPING))
  folder_id = user_folder_id_dict.get(DROPBOX_STUDENTS_PATH + one_student_name.lower(), None)
  assert folder_id == new_folder.id

  # tests
def test_supress_user(one_student_name):
  """Delete a user and make sur that user is in the GED"""
  # Assert that the user indeed exist before deleting it
  user_folder_id_dict = json.loads(redis_client.get(REDIS_USER_ID_MAPPING))
  folder_id = user_folder_id_dict.get(DROPBOX_STUDENTS_PATH + one_student_name.lower(), None)
  assert folder_id is not None, "Could not access the ged's id of user {}".format(one_student_name)
  user_ged = redis_client.hget(REDIS_USER_DMS, folder_id)
  assert user_ged is not None, "Could not access the user to be deleted"

  # delete the user space
  deleted_folder = dbx.files_delete_v2(DROPBOX_STUDENTS_PATH + one_student_name)
  assert isinstance(deleted_folder, dropbox.files.DeleteResult) 

  # wait for the hook to process the information
  time.sleep(HOOK_WAITING_TIME)

  # See if there is an entry in Redis
  assert redis_client.hget(REDIS_USER_DMS, folder_id) is None, "Redis found an entry for the user that was deleted"
  user_folder_id_dict= json.loads(redis_client.get(REDIS_USER_ID_MAPPING))
  folder_id = user_folder_id_dict.get(DROPBOX_STUDENTS_PATH + one_student_name.lower(), None)
  assert folder_id is None, "found an entry {} -> {} but it is supposed to be deleted".format(DROPBOX_STUDENTS_PATH + one_student_name.lower(), folder_id)


@pytest.mark.skip
def test_create_multiple_users():
  """Create a user and make sur that user is in the GED"""
  dbx.files_create_folder_batch(student_paths)
  time.sleep(4)
  redis_client

  raise NotImplementedError




@pytest.mark.skip
def test_upload_one_file():
  """Create a user and make sur that user is in the GED"""
  raise NotImplementedError


@pytest.mark.skip
def test_create_one_directory():
  """Create a user and make sur that user is in the GED"""
  raise NotImplementedError

@pytest.mark.skip
def test_upload_multiple_files():
  """Create a user and make sur that user is in the GED"""
  raise NotImplementedError

@pytest.mark.skip
def test_upload_complexe_directory():
  """Create a user and make sur that user is in the GED"""
  raise NotImplementedError

@pytest.mark.skip
def test_delete_user():
  """Delete a user and make sur that user is in the GED"""
  raise NotImplementedError
