import dropbox
import os
import redis
import json
import io
from dropbox.files import DeletedMetadata, FolderMetadata, FileMetadata
import xmlrpc.client
import base64
import requests
import re
from tqdm import tqdm


# REDIS
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = os.environ['REDIS_PORT']
REDIS_USER = os.environ['REDIS_USER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

# registers
REDIS_USER_DMS = os.environ['REDIS_USER_DMS']
REDIS_USER_ID_MAPPING = os.environ['REDIS_USER_ID_MAPPING']
REDIS_DOCUMENT_TEMPLATES = os.environ['REDIS_DOCUMENT_TEMPLATES']
REDIS_DBX_HOOKO_SUBSCRIBERS = os.environ['REDIS_DBX_HOOKO_SUBSCRIBERS'] # list but stored as a string (need eval())


# DROPBOX
DROPBOX_TOKEN = os.environ['DROPBOX_TOKEN']
# paths
DROPBOX_USER_DMS_PATH = os.environ['DROPBOX_USER_DMS_PATH']
DROPBOX_DOCUMENT_TEMPLATES_PATH = os.environ['DROPBOX_DOCUMENT_TEMPLATES_PATH']
DROPBOX_TEACHERS_PATH = os.environ['DROPBOX_TEACHERS_PATH']
DROPBOX_STUDENTS_PATH = os.environ['DROPBOX_STUDENTS_PATH']


# ODOO
ODOO_DB = os.environ['ODOO_DB']
ODOO_USER = os.environ['ODOO_USER']
ODOO_PASSWORD = os.environ['ODOO_PASSWORD']


# REGEX SPECIVIC USER SPACE PATH
USER_PATH = r'\/users\/teachers\/[a-z\d ]+|\/users\/students\/[a-z\d ]+'

redis_client = redis.Redis(host=REDIS_HOST, 
                           port=REDIS_PORT,
                           username=REDIS_USER,
                           password=REDIS_PASSWORD,
                           decode_responses=True,
                           socket_timeout=None,
                           connection_pool=None,
                           charset='utf-8',
                           errors='strict')


def iprint(str_='', i=True):
    if i:
        print(str_, flush=True)


def list_dropbox_content_with_targets(folder_results, targets, list_type='folder', full_path=False, format_dict_create=False):
    """List content of given folder_results filtered by the targets excluding the basic path
    params:
        folder_results: output of files_list_folder dropbox sdk function
        targets: exclude all path but the targets (name of the users to keep). If targets is null, 
            returns all the folder's path
        list_folder: If set to True, liste exclusively folder. Else list exclusively files
        list_type: Either folder, file or both
    """
    TYPES_TO_LIST = {
        'file' : FileMetadata,
        'folder' : FolderMetadata,
        'both' : object
    }
    paths = []
    results = folder_results.entries
    type_to_keep = TYPES_TO_LIST[list_type]
    
    
    for result in results: 
        if not isinstance(result,type_to_keep):
            continue
        
        path_lower = result.path_lower
        if format_dict_create:
            path_lower = path_lower[1:] if path_lower.find('/') == 0 else path_lower
            if isinstance(result, FolderMetadata):
                path_lower += '/'
        
        if not targets:
            paths.append(path_lower)
            continue
        
        for target in targets:
            base_index = path_lower.find(target)
            
            # Target found
            if base_index != -1:
                if full_path:
                    paths.append(path_lower)
                else:
                    paths.append(path_lower[base_index:])
    return paths


def build_nested_helper(path, container):
    is_dir = path.rfind('/') == len(path) - 1
    segs = path.split('/')
    head = segs[0]
    tail = segs[1:]
    if tail:
        if head not in container:
            container[head] = {}
        build_nested_helper('/'.join(tail), container[head])
    else:
        if not is_dir:
            if not 'files' in container:
                container['files'] = [head]
            else:
                container['files'].append(head)
            #iprint(container)

def build_nested(paths):
    container = {}
    for path in paths:
        build_nested_helper(path, container)
    return container




    

def get_user_folder_dict(path):
    """"Get the user folder """
    # get the path of the user's root folder
    user_path = re.findall(USER_PATH, path)[0]
    
    # get the folder's id of the path
    user_folder_id = json.loads(redis_client.get(REDIS_USER_ID_MAPPING)).get(user_path)
    if not user_folder_id:
        iprint("folder not found")
        return False, False, False
    
    # get the folder's dict
    user_folder_dict = json.loads(redis_client.hget(REDIS_USER_DMS, user_folder_id))

    # Seek the location in the folder dict that needs to be updated and updates it
    path = path.replace(user_path, '')

    return user_folder_dict, user_folder_id, path


def delete_file_or_folder(path):
    """Delete the file or folder in the relevant user folder dict at the path location"""
    
    # Test if the path 
    is_deletation_user = re.match(USER_PATH.replace(']+',']+$'), path)
    if is_deletation_user:
        # delete user space
        user_id_dict = json.loads(redis_client.get(REDIS_USER_ID_MAPPING))
        redis_client.hdel(REDIS_USER_DMS, user_id_dict[path])
        del user_id_dict[path]
        return redis_client.set(REDIS_USER_ID_MAPPING, json.dumps(user_id_dict))
        
        

    user_folder_dict, user_folder_id, path = get_user_folder_dict(path)

    segs = path.split('/')
    file_or_dir = segs[-1]
    segs = segs[:-1]
    current_folder = user_folder_dict
    for seg in segs:
        if seg:
            if seg in current_folder:
                current_folder = current_folder[seg]
            else:
                raise AttributeError("path: {} does not exist at {}".format(path, seg))

    iprint("SHOULD DELETE: {}".format(file_or_dir))
    
    if file_or_dir in current_folder:
        del current_folder[file_or_dir]

    
    if 'files' in current_folder and file_or_dir in current_folder['files'] :
        current_folder['files'].remove(file_or_dir)
    
    iprint(current_folder)
    iprint("----")
    iprint(user_folder_dict)
    iprint("----")
    
    return redis_client.hset(REDIS_USER_DMS, user_folder_id,  json.dumps(user_folder_dict))



def add_file(file_path):
    """Add the file in the relevant user folder dict at the path location"""
    user_folder_dict, user_folder_id, file_path_relative = get_user_folder_dict(file_path)

    segs = file_path_relative.split('/')
    file = segs[-1]
    segs = segs[:-1]
    current_folder = user_folder_dict
    for seg in segs:
        if seg:
            if seg in current_folder:
                
                current_folder = current_folder[seg]
            else:
                raise AttributeError("path: {} does not exist at {}".format(file_path_relative, seg))
    iprint("SHOULD ADDFILE or DICT: {}".format(file_path_relative, user_folder_dict))
    if not 'files' in current_folder:
        current_folder['files'] = []
    
    if file not in current_folder['files'] :
        current_folder['files'].append(file)

    # update the new file in redis
    return redis_client.hset(REDIS_USER_DMS, user_folder_id,  json.dumps(user_folder_dict))


def add_dir(change, dir_path):
    """Add the file in the relevant user folder dict at the path location"""
    # Test if the path 
    is_creation_user = re.match(USER_PATH.replace(']+',']+$'), dir_path)

    # New user
    if is_creation_user:
        # add the user in the redis user id mapping dict
        path_to_id = json.loads(redis_client.get(REDIS_USER_ID_MAPPING))
        path_to_id[dir_path] = change.id
        redis_client.set(REDIS_USER_ID_MAPPING, json.dumps(path_to_id))
        iprint('new user with id: {}, and path: {}'.format(change.id, dir_path))
        # create the user space
        return redis_client.hset(REDIS_USER_DMS, change.id, '{}')

    user_folder_dict, user_folder_id, file_path_relative = get_user_folder_dict(dir_path)

    segs = dir_path.split('/')
    dir_ = segs[-1]
    segs = segs[:-1]
    current_folder = user_folder_dict
    for seg in segs:
        if seg:
            if seg in current_folder:
                current_folder = current_folder[seg]
            else:
                error_msg = "path: {} does not exist at {}".format(dir_path, seg)
                iprint(error_msg)
                # raise AttributeError()
    if dir_ not in current_folder :
        current_folder[dir_] = {}
    
    iprint(user_folder_dict)

    # update the new dir in redis
    return redis_client.hset(REDIS_USER_DMS, user_folder_id,  json.dumps(user_folder_dict))


# Getters & Setters

def get_cursor():
    return redis_client.hget('cursors', 'cursor')

def set_cursor(cursor):
    return redis_client.hset('cursors', 'cursor', cursor)

def set_dict(register, path_dict):
    path_dict_json = json.dumps(path_dict)
    return redis_client.set(REDIS_USER_DMS, path_dict_json)

def get_dict(register):
    path_dict = redis_client.get(register)
    return json.loads(path_dict)

def get_subsrcibers():
    """Get the list of subscribers that registered to """
    subscribers = redis_client.get(REDIS_DBX_HOOKO_SUBSCRIBERS)
    if not subscribers:
        return []
    return eval(subscribers)

def test_subscriber(subscriber):
    try:
        resp = requests.get(subscriber, timeout=10)
    except:
        return False
    print(resp)
    if resp.status_code != 200:
        remove_subscriber(subscriber)
        return False
    return True

def remove_subscriber(url):
    subscribers = get_subsrcibers()
    subscribers.remove(url)
    redis_client.set(REDIS_DBX_HOOKO_SUBSCRIBERS, str(subscribers))


def set_subscriber(url):
    """Set the subscriber space. i.e. Enter the subscriber's url in the list of subscribers and set the appropriate user space in redis.
    Where the key 
    """
    
    # Add the subscriber to the list
    print(url)
    subscribers = redis_client.get(REDIS_DBX_HOOKO_SUBSCRIBERS)
    if not subscribers:
        subscribers = [url]
    else:
        subscribers = eval(subscribers)
        
        # Subscriber belongs already to the list
        if url in subscribers:
            return subscribers
        subscribers.append(url)

    redis_client.set(REDIS_DBX_HOOKO_SUBSCRIBERS, str(subscribers))

    iprint(subscribers)

    return subscribers

def load_user_dms():

    # Connection to dropbox
    dbx = dropbox.Dropbox(DROPBOX_TOKEN)

    # List users directories using DBX API
    paths=[DROPBOX_STUDENTS_PATH, DROPBOX_TEACHERS_PATH]
    path_to_id = dict()
    for path in paths:
        res = dbx.files_list_folder(path)
        for r in res.entries:
            path_to_id[r.path_lower] = r.id
            

    folder_results = dbx.files_list_folder("/users/", recursive=True )
    paths = list_dropbox_content_with_targets(folder_results, [], list_type='both', full_path=True, format_dict_create=True)



    while folder_results.has_more:
        # Folder has more results
        folder_results = dbx.files_list_folder_continue(folder_results.cursor)
        #print("In Load dms Continue :\n{}".format(folder_results), False)
        paths += list_dropbox_content_with_targets(folder_results, [], list_type='both', full_path=True, format_dict_create=True)  

    path_dict = build_nested(paths)

    # Construct the dict with all users as keys and their GED architecture as value
    users_dict = {DROPBOX_STUDENTS_PATH + k : v for (k, v) in path_dict["users"]["students"].items()}
    users_dict.update({DROPBOX_TEACHERS_PATH + k : v for (k, v) in path_dict["users"]["teachers"].items()})


    # Fill users GED architectures in Redis
    for path, d in tqdm(users_dict.items()):
        d_json = json.dumps(d)
        redis_client.hset(REDIS_USER_DMS, path_to_id[path], d_json)
    redis_client.set(REDIS_USER_ID_MAPPING, json.dumps(path_to_id))
        
            


    # Update cursor
    set_cursor(folder_results.cursor)

    dbx.close




def update_doc_templates(change):
    content_info = "Info : "
    

    # File is deleted
    if isinstance(change, DeletedMetadata):
        
        redis_client.delete(change.name)
        content_info += "file {} has been deleted".format(change.name)
        # TODO send info to odoo : 
        # send change.name
        
    if isinstance(change, FileMetadata):
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        subscribers = get_subsrcibers()

        if not change.name.endswith('.docx'):
            content_info += " but it was not a docx. we supress it"
            dbx.files_delete(change.path_lower)
            dbx.close()
            return False

        for url in subscribers:

            try :
                # Connect to odoo
                common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
                uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
                models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
            except Exception as e:
                iprint("ERROR WITH: {}\nRemove link".format(url))
                remove_subscriber(url)
                iprint()
                continue


            content_hash = redis_client.hget(change.name, 'content_hash')
            if content_hash:
                # compare the hashes to see if the content has changed
                if change.content_hash != content_hash:
                    # TODO 
                    # send change.name, 
                    #      base64 pdf
                    #      link
                    content_info += "content has changed"

            else:
                # TODO 
                # Newly created file
                # 
                content_info += "file {} has been added".format(change.name)
                

                redis_client.hset(change.name, 'content_hash', change.content_hash)
                link = dbx.sharing_create_shared_link(change.path_lower)
                redis_client.hset(change.name, 'link', link.url)
                # docx2html(change)

                # Create file in Odoo
                metadata, docx_resp = dbx.files_download(path=change.path_lower)
                docx_encoded = base64.b64encode(docx_resp.content)
                models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'template', 'create', [{
                    'name': change.name,
                    'shared_link' : link.url,
                    'file_docx' : docx_encoded
                }])
                    
        dbx.close()
                # TODO Download and generate html from docx

    iprint(content_info)


def update_user_dms(path, change):
    if isinstance(change, DeletedMetadata):
        sucess = delete_file_or_folder(path)
        iprint("DELETE! Sucess = {}".format(sucess))
    elif isinstance(change, FileMetadata):
        add_file(path)
        iprint("ADDFILE!")
    elif isinstance(change, FolderMetadata):
        iprint("ADDFOLDER!")
        add_dir(change, path)
    else:
        iprint("Error, change not taken into account")

