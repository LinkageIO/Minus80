import json
import uuid
import requests
import firebase_admin

from flask import abort,Response
from functools import wraps
from firebase_admin import auth,firestore
from google.cloud import storage

firebase_admin.initialize_app()

# Decorators ----------------------
def authenticated(fn):
    @wraps(fn)
    def wrapped(request):
        try:
            # Extract the firebase token from the HTTP header
            token = request.headers['Authorization'].replace('Bearer ','')
            # Validate the token 
            verified = auth.verify_id_token(token)
        except Exception as e:
            return abort(401,f'Invalid Credentials:{e}')
        # Execute the authenticated function
        return fn(request)
    return wrapped


# HTTP Methods --------------------
@authenticated
def stage_files(request):
    data = request.get_json()
    # Get the uid from the token
    dtype = data['dtype']
    name = data['name']
    uid = get_uid(request) 

    # Fire up firestore
    db = firestore.client()
    dataset_ref = db.document(
        f"Frozen/{uid}/DatasetType/{dtype}/Dataset/{name}"
    ) 
    dataset = dataset_ref.get()
    # Create a new dataset if needed
    if not dataset.exists:
        dataset = create_dataset(data['dtype'],data['name'],uid)
    # Figure out which files need to be uploaded
    dataset_files = set(dataset.get('available_files'))
    
    # create a response object to let client know what to upload
    # create a uuid for the staged files
    stage_uuid = str(uuid.uuid4())
    response = {
        'stage_uuid' : stage_uuid,
        'missing_files' : [],
        'uploaded_files' : []
    }
    # figure out what files are missing 
    client = storage.Client()
    bucket = client.bucket('minus80')
    for file_data in data['tag_data']['files'].values():
        if file_data['checksum'] not in dataset_files:
            # Create a staged blob and resumable url
            blob = bucket.blob(
                f'staged/{uid}/{dtype}/{name}/{stage_uuid}/{file_data["checksum"]}'
            )
            file_data['upload_url'] = blob.create_resumable_upload_session(
                content_type = 'application/octet-stream'
            )
            response['missing_files'].append(file_data)
        else:
            response['uploaded_files'].append(file_data)
    return Response(
        response=json.dumps(response),
        status=200,
        mimetype='application/json'
    )

# Helper Methods -------------------

def create_dataset(dtype,name,owner_uid):
    ''':
        creates a dataset

        Parameters
        ----------
        dtype : str
            The minus80 dataset type (e.g. Project,Cohort,etc)
        name : str
            The name of the dataset to add
        owner_uid : str
            The UID of the project owner

        Returns
        -------
        <google.cloud.firestore_v1.document.DocumentSnapshot>
            A snapshot of the added document 
    '''
    dataset_ref = firestore.client().document(
        f"Frozen/{owner_uid}/DatasetType/{dtype}/Dataset/{name}"
    )
    dataset = dataset_ref.get()
    if dataset.exists:
        raise ValueError('Dataset exists')
    dataset_ref.set({
        'dtype' : dtype,
        'name' : name,
        'owner' : owner_uid,
        'collaborators': [],
        'available_tags' : [],
        'available_files' : []
    })    
    return dataset_ref.get()

def get_uid(request):
    '''
    Extracts the UID from the Bearer token
    sent with the Authorization header

    Parameters
    -----------
    request : a Flask request object

    Returns
    -------
    str containing the user uid

    Raises
    ------
    ValueError
    '''
    try:
        # Extract the firebase token from the HTTP header
        token = request.headers['Authorization'].replace('Bearer ','')
        # Validate the token 
        uid = auth.verify_id_token(token)['uid']
    except Exception as e:
        raise ValueError('Unable to extract uid')
    return uid

if __name__ == "__main__":
    from flask import Flask, request
    app = Flask(__name__)

    @app.route('/push',methods=['POST'])
    def do_push():
        return push(request)

    @app.route('/stage_files',methods=['POST'])
    def do_stage_files():
        return stage_files(request)

    app.run(
        '127.0.0.1', 
        50000, 
        debug=True,
        ssl_context=('cert.pem', 'key.pem')
    )
