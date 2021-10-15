import dropbox
import os
import redis
import json
import io
from dropbox.files import DeletedMetadata, FolderMetadata, FileMetadata
import mammoth


REDIS_USER_DMS = 'dmsFitspro'
DROPBOX_USER_DMS_PATH = 'Users'

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
        
        path_display = result.path_display
        if format_dict_create:
            path_display = path_display[1:] if path_display.find('/') == 0 else path_display
            if isinstance(result, FolderMetadata):
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
    dbx = dropbox.Dropbox(TOKEN)
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
                raise AttributeError("path: {} does not exist at {}".format(path, seg))

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
                raise AttributeError("path: {} does not exist at {}".format(path, seg))

    iprint("SHOULD ADDFILE or DICT: {}".format(file_path, path_dict))
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

def load_user_dms():

    dbx = dropbox.Dropbox(TOKEN)
    
        
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
                raise AttributeError("path: {} does not exist at {}".format(path, seg))
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
        # TODO send info to frontend
        
    if isinstance(change, FileMetadata):
        dbx = dropbox.Dropbox(TOKEN)
        if change.name.endswith('.html'):
            dbx.close()
            return ''

        content_hash = redis_client.hget(change.name, 'content_hash')
        if content_hash:
            # compare the hashes to see if the content has changed
            if change.content_hash != content_hash:
                content_info += "content has changed"
                docx2html(change)

        else:
            content_info += "file {} has been added".format(change.name)
            
            
            if not (change.name.endswith('.docx') or change.name.endswith('.html')):

                content_info += " but it was not a docx. we supress it"
                dbx.files_delete(change.path_display)
            else:
                redis_client.hset(change.name, 'content_hash', change.content_hash)
                link = dbx.sharing_create_shared_link(change.path_display)
                redis_client.hset(change.name, 'link', link.url)
                docx2html(change)
                
        dbx.close()
            # TODO Download and generate html from docx

    iprint(content_info)
    

def docx2html(change):
    dbx = dropbox.Dropbox(TOKEN)
    _, docx_file = dbx.files_download(change.path_display)
    docx_file = io.BytesIO(docx_file.content)
    html_file = mammoth.convert_to_html(docx_file)
    iprint(html_file)
    dbx.files_upload(html_file.value.encode(),
                     '{}/{}'.format(DROPBOX_DOCUMENT_TEMPLATES_PATH,
                     change.name.replace("docx", "html")),
                     mode=dropbox.files.WriteMode.overwrite)
    dbx.close()

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
    
    iprint(" - New -" * 3)
    iprint(json.dumps(path_dict['Users']['Students']['Christopher Smith'], indent=2))


    set_dict(DROPBOX_USER_DMS_PATH, path_dict)

