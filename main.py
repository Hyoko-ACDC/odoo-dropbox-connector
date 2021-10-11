from hashlib import sha256
import hmac
import json
import os
import threading
import urllib.parse
import sys

from dropbox import Dropbox, DropboxOAuth2Flow
from dropbox.files import DeletedMetadata, FolderMetadata, WriteMode, FileMetadata
from flask import abort, Flask, redirect, render_template, Response, request, session, url_for
from utils import *
import redis 


app = Flask(__name__)

host = os.environ['REDIS_HOST']
redis_password = os.environ['REDIS_PASSWORD']

APP_SECRET = os.environ['APP_KEY']
TOKEN = os.environ['DROPBOX_TOKEN']


# A random secret used by Flask to encrypt session data cookies
app.secret_key = os.environ['FLASK_SECRET_KEY']


@app.route('/')
def hello_world():
    return 'Hey, we have Flask in a Docker container!'


@app.route('/webhook', methods=['GET'])
def verify():
    '''Respond to the webhook verification (GET request) by echoing back the challenge parameter.'''

    resp = Response(request.args.get('challenge'))
    resp.headers['Content-Type'] = 'text/plain'
    resp.headers['X-Content-Type-Options'] = 'nosniff'

    return resp
    

@app.route('/webhook', methods=['POST'])
def webhook():
    '''Receive a list of changed user IDs from Dropbox and process each.'''

    # Make sure this is a valid request from Dropbox
    signature = request.headers.get('X-Dropbox-Signature')
    key = bytes(APP_SECRET, encoding="ascii")
    print(request)
    computed_signature = hmac.new(key, request.data, sha256).hexdigest()
    if not hmac.compare_digest(signature, computed_signature):
        abort(403)
    print("HOOOOOOOK")

    for account in json.loads(request.data)['list_folder']['accounts']:
        # We need to respond quickly to the webhook request, so we do the
        # actual work in a separate thread. For more robustness, it's a
        # good idea to add the work to a reliable queue and process the queue
        # in a worker process.
        threading.Thread(target=process_user, args=(account,)).start()
        # process_user(account)
        
    return ''

def process_user(account):
    '''Call /files/list_folder for the given user ID and process any changes.'''

    # cursor for the user (Only changes should be listed here)
    cursor = get_cursor()

    dbx = Dropbox(TOKEN)
    has_more = True

    print("Web Hook Activated", flush=True)
    while has_more:
        assert(cursor is not None)

        list_folder_continue = dbx.files_list_folder_continue(cursor)

        path_dict = get_dict(REDIS_USER_DMS)

        print(" - OLD - " * 3, flush=True)
        print(json.dumps(path_dict['Users']['Students']['Christopher Smith'], indent=2), file=sys.stdout)
        print(" - Change - " * 3, flush=True)
        print(list_folder_continue)

        # Proceed to changes
        for change in list_folder_continue.entries:
            path = change.path_display

            print(change)


            if isinstance(change, DeletedMetadata):
                sucess = delete_file_from_dict(path, path_dict)
                print("DELETE! Sucess = {}".format(sucess), flush=True)
            if isinstance(change, FileMetadata):
                add_file_from_dict(path, path_dict)
                print("ADDFILE!", flush=True)
            if isinstance(change, FolderMetadata):
                print("ADDFOLDER!", flush=True)
                add_dir_from_dict(path, path_dict)
        
        print(" - New -" * 3, flush=True)
        print(json.dumps(path_dict['Users']['Students']['Christopher Smith'], indent=2), file=sys.stdout, flush=True)


        set_dict(DROPBOX_USER_DMS_PATH, path_dict)

        # Update cursor
        cursor = list_folder_continue.cursor
        set_cursor(cursor)

        # Repeat only if there's more to do
        has_more = list_folder_continue.has_more

if __name__ == '__main__':
    print("INIT REDIS DB")
    
    # Load user dms dict for the first time
    load_dms(DROPBOX_USER_DMS_PATH, REDIS_USER_DMS)
    
    # Load document templates dms dict for the first time
    # load_dms(DROPBOX_DOCUMENT_TEMPLATES_PATH, REDIS_DOCUMENT_TEMPLATES)

    app.run(debug=True, host='0.0.0.0')