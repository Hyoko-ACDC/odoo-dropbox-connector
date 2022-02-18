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
USER_PATH = r'\/users\/teachers\/[a-zA-Za-zÀ-ÖØ-öø-ÿ\d ]+|\/users\/students\/[a-zA-Za-zÀ-ÖØ-öø-ÿ\d ]+'

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
    """Print log in the server. Usefull for debugging.

    Args:
        str_ (str, optional): Logs to print on the server. Defaults to ''.
        i (bool, optional): Global control Whether to print or not. Defaults to True.
    """
    if i:
        print(str_, flush=True)


def list_dropbox_content_with_targets(folder_results, targets, list_type='folder', full_path=False, format_dict_create=False):
    """List content of given folder_results filtered by the targets excluding the basic path

    Args:
        folder_results : output of files_list_folder dropbox sdk function
        targets (List[str]): exclude all path but the targets (name of the users to keep). If targets is null, 
        list_type (str, optional): Either folder, file or both. Defaults to 'folder'.

    Returns:
        List[str]: All the folder's path
    """
    
    TYPES_TO_LIST = {
        'file' : FileMetadata,
        'folder' : FolderMetadata,
        'both' : object
    }
    paths = []
    results = folder_results.entries
    type_to_keep = TYPES_TO_LIST[list_type]
    
    # Process all the entries to extract only usefull information
    for result in results: 
        if not isinstance(result,type_to_keep):
            continue
        
        path_lower = result.path_lower
        if format_dict_create:
            path_lower = path_lower[1:] if path_lower.find('/') == 0 else path_lower
            if isinstance(result, FolderMetadata):
                path_lower += '/'
        
        # Exclud if it does not belong to targets
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
    ''''Helper function of the build_nested function'''
    
    
    is_dir = path.rfind('/') == len(path) - 1
    segs = path.split('/')
    head = segs[0]
    tail = segs[1:]
    
    # Use recurtion to build the dictionnary
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

def build_nested(paths):
    ''''From a list of paths, returns a dictionnary representation'''
    container = {}
    for path in paths:
        build_nested_helper(path, container)
    return container




    

def get_user_folder_dict(path):
    """Get the user folder in redis according to the path 

    Args:
        path (str): path to get the user folder

    Raises:
        Exception: if the path does not exists in the redis register

    Returns:
        tuple(user_folder_dict, user_folder_id, path): the folder's dict of the user, id and path.
    """
    
    # get the path of the user's root folder
    user_path = re.findall(USER_PATH, path)[0]
    
    # get the folder's id of the path
    print(f"user_path: {user_path}")
    user_folder_id = json.loads(redis_client.get(REDIS_USER_ID_MAPPING)).get(user_path)
    if not user_folder_id:
        iprint(f"WARNING: folder not found for path: {path}")
        # reloading whole GED
        load_user_dms()
        user_folder_id = json.loads(redis_client.get(REDIS_USER_ID_MAPPING)).get(user_path)
        if not user_folder_id:
            raise Exception(f"USER of the path {path} does not exist in the GED")
        
    
    # get the folder's dict
    user_folder_dict = json.loads(redis_client.hget(REDIS_USER_DMS, user_folder_id))

    # Seek the location in the folder dict that needs to be updated and updates it
    path = path.replace(user_path, '')

    return user_folder_dict, user_folder_id, path


def delete_file_or_folder(path):
    """Delete a file or a folder in the relevant user folder dict at the path location

    Args:
        path (str): the file or folder path to be deleted

    Raises:
        AttributeError: If the path does not exists in redis

    Returns:
        int: The number of fields that were added
    """
    
    # Test if the path 
    is_deletation_user = re.match(USER_PATH.replace(']+',']+$'), path)
    if is_deletation_user:
        # delete user space
        user_id_dict = json.loads(redis_client.get(REDIS_USER_ID_MAPPING))
        redis_client.hdel(REDIS_USER_DMS, user_id_dict[path])
        del user_id_dict[path]
        return redis_client.set(REDIS_USER_ID_MAPPING, json.dumps(user_id_dict))
        
        

    user_folder_dict, user_folder_id, path = get_user_folder_dict(path)

    # Process the path
    segs = path.split('/')
    file_or_dir = segs[-1]
    segs = segs[:-1]
    current_folder = user_folder_dict
    
    # Recursively resolve the path
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
    """Add a file to the appropriate user's space

    Args:
        file_path (str): the file path (it contains also 

    Raises:
        AttributeError: If the path does not exists in redis

    Returns:
        int: The number of fields that were added
    """
    
    user_folder_dict, user_folder_id, file_path_relative = get_user_folder_dict(file_path)

    # Process the path
    segs = file_path_relative.split('/')
    file = segs[-1]
    segs = segs[:-1]
    
    # Recursively resolve the path
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
    """Add a folder in the relevant user folder dict at the path location

    Args:
        change : The information about the new directory.
        dir_path (str): Path of the new directory

    Returns:
        int: The number of fields that were added
    """
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

    # Recursively resolve the path
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
    if dir_ not in current_folder :
        current_folder[dir_] = {}
    iprint(user_folder_dict)

    # update the new dir in redis
    return redis_client.hset(REDIS_USER_DMS, user_folder_id,  json.dumps(user_folder_dict))


# Getters & Setters in Redis
def get_cursor():
    '''Get the current cursor stored in redis'''
    return redis_client.hget('cursors', 'cursor')


def set_cursor(cursor):
    """Set the current cursor stored in redis

    Args:
        cursor (str): The cursor to be set

    Returns:
        int: The number of fields that were added
    """
    return redis_client.hset('cursors', 'cursor', cursor)

def get_subsrcibers():
    '''Get the list of subscribers that registered themselves to this server.'''
    subscribers = redis_client.get(REDIS_DBX_HOOKO_SUBSCRIBERS)
    if not subscribers:
        return []
    return eval(subscribers)

def test_subscriber(subscriber):
    """Test if a subscriber is reachable. If not it deletes the subscriber.

    Args:
        subscriber (str): Subscriber to be tested

    Returns:
        bool: True if the subscriber is reachable, False otherwise.
    """
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
    """Remove a subscriber

    Args:
        url (str): url of the subscriber to be removed
    """
    subscribers = get_subsrcibers()
    subscribers.remove(url)
    redis_client.set(REDIS_DBX_HOOKO_SUBSCRIBERS, str(subscribers))


def set_subscriber(url):
    """Set the subscriber space. i.e. Enter the subscriber's url in the list of subscribers.

    Args:
        url (str): url of the subscriber to be added.

    Returns:
        list[str]: All the subscribers
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
    """Parse and fill the folder structure into redis of each user's space in Dropbox. 
    Script used at startup to bootstrap the register.
    """
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
    """Process a document template changement. 
    Attention, this method is not fully finished. Some work needs to be done

    Args:
        change: The changement in Dropbox
    """
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
    """Update the user folder structure representation according to the changement.

    Args:
        path (str): Where the changement took place
        change : The changement itself
    """
    
    # The changement concerns a deletation of a file/folder 
    if isinstance(change, DeletedMetadata):
        sucess = delete_file_or_folder(path)
        iprint("DELETE! Sucess = {}".format(sucess))
    
    # The changement concerns an upload of a file
    elif isinstance(change, FileMetadata):
        add_file(path)
        iprint("ADDFILE!")
        
    # The changement concerns an upload of a folder
    elif isinstance(change, FolderMetadata):
        iprint("ADDFOLDER!")
        add_dir(change, path)
        
    # This should not happend
    else:
        iprint("Error, change not taken into account")

