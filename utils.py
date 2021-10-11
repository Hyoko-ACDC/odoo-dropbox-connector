import dropbox
import os
import redis
import json

REDIS_USER_DMS = 'dmsFitspro'
DROPBOX_USER_DMS_PATH = '/Users'

REDIS_DOCUMENT_TEMPLATES = 'dmsDocumentTemplates'
DROPBOX_DOCUMENT_TEMPLATES_PATH = '/Admin/Document Templates'

TOKEN = os.environ['DROPBOX_TOKEN']

host = os.environ['REDIS_HOST']
redis_password = os.environ['REDIS_PASSWORD']


redis_client = redis.Redis(host=host, 
                           port=6379, 
                           username='default',
                           password=redis_password,
                           decode_responses=True,
                           socket_timeout=None,
                           connection_pool=None,
                           charset='utf-8',
                           errors='strict')

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
        'file' : dropbox.files.FileMetadata,
        'folder' : dropbox.files.FolderMetadata,
        'both' : object
    }
    paths = []
    results = folder_results.entries
    type_to_keep = TYPES_TO_LIST[list_type]
    
    
    for result in results: 
        if not isinstance(result,type_to_keep):
            continue
        
        path_display = result.path_display
        if format_dict_create:
            path_display = path_display[1:] if path_display.find('/') == 0 else path_display
            if isinstance(result, dropbox.files.FolderMetadata):
                path_display += '/'
        
        if not targets:
            paths.append(path_display)
            continue
        
        for target in targets:
            base_index = path_display.find(target)
            
            # Target found
            if base_index != -1:
                if full_path:
                    paths.append(path_display)
                else:
                    paths.append(path_display[base_index:])
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
            #print(container)

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
    dbx = dropbox.Dropbox(TOKEN)
    folder_results = dbx.files_list_folder(path, recursive=True )
    paths = list_dropbox_content_with_targets(folder_results, [], list_type='both', full_path=True, format_dict_create=True)
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
                raise AttributeError("path: {} does not exist at {}".format(path, seg))

    print("SHOULD DELETE: {} for dictionnary: \n {}\n\n and current:  {}".format(file_or_dir, path_dict,current_folder) , flush=True)
    
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
                raise AttributeError("path: {} does not exist at {}".format(path, seg))

    print("SHOULD ADDFILE or DICT: {} for dictionnary: \n {}\n\n and current:  {}".format(file_path, path_dict,current_folder), flush=True)
    if not 'files' in current_folder:
        current_folder['files'] = []
    
    if file not in current_folder['files'] :
        current_folder['files'].append(file)
        return True
    return False

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

def load_dms(path, register):

    dbx = dropbox.Dropbox(TOKEN)

    # cursor for the user (None the first time)
    cursor = get_cursor()

    has_more = True

    while has_more:
        
        # Load user dms dict for the first time
        # Get list of folder
        folder_results = dbx.files_list_folder(path=path, recursive=True)

        # Parse the given result and return a clean list of paths
        paths = list_dropbox_content_with_targets(folder_results, [], list_type='both', full_path=True, format_dict_create=True)
        
        # build the dictionnary
        folder_dict = build_nested(paths)

        # Save it to redis DB
        folder_dict_json = json.dumps(folder_dict)
        redis_client.set(register, folder_dict_json)

        # Update cursor
        cursor = folder_results.cursor
        set_cursor(cursor)

        # Repeat only if there's more to do
        has_more = folder_results.has_more


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
                raise AttributeError("path: {} does not exist at {}".format(path, seg))
    if dir_ not in current_folder :
        current_folder[dir_] = {}
        return True
    return False