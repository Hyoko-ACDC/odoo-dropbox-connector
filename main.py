from hashlib import sha256
import hmac
import json
import os
import threading
import urllib.parse
import sys

from dropbox import Dropbox
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
    iprint(request)
    computed_signature = hmac.new(key, request.data, sha256).hexdigest()
    if not hmac.compare_digest(signature, computed_signature):
        abort(403)

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
    iprint("Old cursor {}".format(cursor), False)

    dbx = Dropbox(TOKEN)
    has_more = True

    while has_more:
        assert(cursor is not None)

        list_folder_continue = dbx.files_list_folder_continue(cursor)

        path_dict = get_dict(REDIS_USER_DMS)

        iprint(" - OLD - " * 3, False)
        iprint(json.dumps(path_dict['Users']['Students']['Christopher Smith'], indent=2), False)
        iprint(" - Change - " * 3, False)
        iprint(list_folder_continue, True)

        # Cursor certinaly not up-to-date. 
        # -> refresh dms representation in redis and fetch latest cursor.
        if not list_folder_continue.entries:
            iprint(" - No entries: load dms - " * 3)
            load_user_dms()
            iprint(json.dumps(path_dict['Users']['Students']['Christopher Smith'], indent=2), False)
            break


        # Proceed to changes
        for change in list_folder_continue.entries:
            path = change.path_display
            iprint("Received following path : {}".format(path))

            # Changes in dms user
            if path.startswith("/Users"):
                update_user_dms(path, path_dict, change)

            if path.startswith("/Admin/Document Templates"):
                iprint("HERE in /Admin/Document Templates")
                update_doc_templates(change)

        # Update cursor
        cursor = list_folder_continue.cursor
        set_cursor(cursor)

        # Repeat only if there's more to do
        has_more = list_folder_continue.has_more
    dbx.close()

if __name__ == '__main__':
    iprint("INIT REDIS DB")
    
    # Load user dms dict for the first time
    load_user_dms()
    
    
    # Load document templates dms dict for the first time
    # load_dms(DROPBOX_DOCUMENT_TEMPLATES_PATH, REDIS_DOCUMENT_TEMPLATES)

    app.run(debug=True, host='0.0.0.0')