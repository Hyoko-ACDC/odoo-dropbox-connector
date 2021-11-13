import dropbox
import os
import redis
import json
import io
from dropbox.files import DeletedMetadata, FolderMetadata, FileMetadata
import mammoth
import xmlrpc.client
import base64
import requests


# REDIS REGISTERS
REDIS_USER_DMS = 'dmsFitspro'
REDIS_DOCUMENT_TEMPLATES = 'dmsDocumentTemplates'
REDIS_DBX_HOOKO_SUBSCRIBERS = 'subscribers'  # list but stored as a string (need eval())



# DROPBOX PATHS
DROPBOX_USER_DMS_PATH = 'users'
DROPBOX_DOCUMENT_TEMPLATES_PATH = '/admin/document templates'

# SETTINGS
DROPBOX_TOKEN = os.environ['DROPBOX_TOKEN']

REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

# url = os.environ['ODOO_URL']
ODOO_DB = os.environ['ODOO_DB']
ODOO_USER = os.environ['ODOO_USER']
ODOO_PASSWORD = os.environ['ODOO_PASSWORD']


redis_client = redis.Redis(host=REDIS_HOST, 
                           port=6379, 
                           username='default',
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


def get_folder_from_dict(path, path_dict):
    """Get the folder given in path from the dictionnary"""
    segs = path.split('/')
    current_folder = path_dict
    for seg in segs:
        if seg:
            if seg in current_folder:
                current_folder = current_folder[seg]
            else:
                raise AttributeError("path: {} does not exist at {}".format(path, seg))
    return current_folder


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



    
# TODO Make the rec function tail recursive
def list_folder_from_folder(folder, recursive=True, path=''):
    """Helper function for listing directories"""
    if not recursive:
        return folder.get(list(folder.keys()))
    dirs = []
    if isinstance(folder, dict):
        for k in folder.keys():
            rec_path = k if not path else path + '/' + k
            if k == 'files':
                continue  
            if isinstance(folder[k], dict):
                # go recursive
                dirs.append(rec_path)
                dirs += list_folder_from_folder(folder[k], path=rec_path)
    return dirs
    

def update_folder_dict(path, bootstrap=False):
    """Update the pickle dict representation of the GED
    args:
        path: path were the folder should be updated
        bootstrap: rather the dict should be updated entirely from scratch
        
    """

    # Get the folder where 
    dbx = dropbox.Dropbox(DROPBOX_TOKEN)
    folder_results = dbx.files_list_folder("", recursive=True )
    paths = list_dropbox_content_with_targets(folder_results, [DROPBOX_USER_DMS_PATH], list_type='both', full_path=True, format_dict_create=True)
    folder_dict = build_nested(paths)



    folder_dict_to_update = load_folder_dict()


    return

def delete_file_from_dict(file_path, path_dict):
    """In place delete file in the path dictionnary"""
    segs = file_path.split('/')
    file_or_dir = segs[-1]
    segs = segs[:-1]
    current_folder = path_dict
    for seg in segs:
        if seg:
            if seg in current_folder:
                current_folder = current_folder[seg]
            else:
                raise AttributeError("path: {} does not exist at {}".format(file_path, seg))

    iprint("SHOULD DELETE: {}".format(file_or_dir))
    
    if file_or_dir in current_folder:
        del current_folder[file_or_dir]
        return True

    
    if 'files' in current_folder and file_or_dir in current_folder['files'] :
        current_folder['files'].remove(file_or_dir)
        

        return True
    
    return False

def add_file_from_dict(file_path, path_dict):
    """In place add file in the path dictionnary"""
    segs = file_path.split('/')
    file = segs[-1]
    segs = segs[:-1]
    current_folder = path_dict
    for seg in segs:
        if seg:
            if seg in current_folder:
                current_folder = current_folder[seg]
            else:
                raise AttributeError("path: {} does not exist at {}".format(file_path, seg))

    iprint("SHOULD ADDFILE or DICT: {}".format(file_path, path_dict))
    if not 'files' in current_folder:
        current_folder['files'] = []
    
    if file not in current_folder['files'] :
        current_folder['files'].append(file)
        return True
    return False


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
        subscribers = []
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

    dbx = dropbox.Dropbox(DROPBOX_TOKEN)
    
        
    # Load user dms dict for the first time
    # Get list of folder
    folder_results = dbx.files_list_folder(path="", recursive=True)
    iprint("In Load dms \n{}".format(folder_results), False)
    # Parse the given result and return a clean list of paths
    paths = list_dropbox_content_with_targets(folder_results, [DROPBOX_USER_DMS_PATH], list_type='both', full_path=True, format_dict_create=True)


    
    while folder_results.has_more:
        # Folder has more results
        folder_results = dbx.files_list_folder_continue(folder_results.cursor)
        iprint("In Load dms Continue :\n{}".format(folder_results), False)
        paths += list_dropbox_content_with_targets(folder_results, [DROPBOX_USER_DMS_PATH], list_type='both', full_path=True, format_dict_create=True)        

    # build the user dms dictionnary
    folder_dict = build_nested(paths)

    # Save it to redis DB
    folder_dict_json = json.dumps(folder_dict)
    redis_client.set(REDIS_USER_DMS, folder_dict_json)

    # Update cursor
    set_cursor(folder_results.cursor)

    dbx.close


def add_dir_from_dict(dir_path, path_dict):
    """In place add file in the path dictionnary"""
    segs = dir_path.split('/')
    dir_ = segs[-1]
    segs = segs[:-1]
    current_folder = path_dict
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
        return True
    return False

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
                # TODO compute Docx b64 encoding
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


def update_user_dms(path, path_dict, change):
    if isinstance(change, DeletedMetadata):
        sucess = delete_file_from_dict(path, path_dict)
        iprint("DELETE! Sucess = {}".format(sucess))
    if isinstance(change, FileMetadata):
        add_file_from_dict(path, path_dict)
        iprint("ADDFILE!")
    if isinstance(change, FolderMetadata):
        iprint("ADDFOLDER!")
        add_dir_from_dict(path, path_dict)
    
    iprint(" - New -" * 3, False)
    #iprint(json.dumps(path_dict['Users']['Students']['Christopher Smith'], indent=2), False)


    set_dict(DROPBOX_USER_DMS_PATH, path_dict)

