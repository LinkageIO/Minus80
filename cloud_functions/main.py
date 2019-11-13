import json
import uuid
import hashlib
import requests
import firebase_admin

from flask import abort,Response
from functools import wraps
from firebase_admin import auth,firestore
from google.cloud import storage

from google.api_core.exceptions import NotFound

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
def list_datasets(request):
    uid = get_uid(request)
    db = firestore.client()
    # fetch the doc
    doc = db.document(
        f'Frozen/{uid}'        
    ).get()
    # Create the response
    datasets = {}
    if doc.exists:
        datasets = doc.to_dict().get('available_datasets',{})
    return Response(
        response=json.dumps(datasets),
        status=200,
        mimetype='application/json'
    )

    

@authenticated
def stage_files(request):
    data = request.get_json()
    # Get the uid from the token
    uid = get_uid(request) 
    dtype = data['dtype']
    name = data['name']
    tag = data['tag']

    # Fire up firestore
    db = firestore.client()
    dataset_ref = db.document(
        f"Frozen/{uid}/DatasetType/{dtype}/Dataset/{name}"
    ) 
    dataset = dataset_ref.get()
    # Create a new dataset if needed
    if not dataset.exists:
        dataset = create_dataset(data['dtype'],data['name'],uid)

    available_tags = set(dataset.get('available_tags'))
    if tag in available_tags:
        # fetch the checksum of the tag to see if the user is 
        # trying to push the same tag
        tag_data = db.document(
            f"Frozen/{uid}/DatasetType/{dtype}/Dataset/{name}/Tag/{tag}"
        ).get().to_dict()
        # set up the response variables
        status = 409
        response = {
            'tag_checksum' : tag_data['total']
        }
    else:
        # Figure out which files need to be uploaded
        dataset_files = set(dataset.get('available_files'))
        # Keep a list of already staged files
        # Sometimes the tag contains duplicates (aka there are
        # copies of files) which is allowed, but only need to 
        # be uploaded once
        staged_files = set()
        
        # create a response object to let client know what to upload
        # create a uuid for the staged files
        stage_uuid = str(uuid.uuid4())
        response = {
            'stage_uuid' : stage_uuid,
            'missing_files' : [],
            'uploaded_files' : []
        }
        status = 200
        # figure out what files are missing 
        client = storage.Client()
        bucket = client.bucket('minus80-staging')
        for file_data in data['tag_data']['files'].values():
            if (file_data['checksum'] not in dataset_files and
                file_data['checksum'] not in staged_files):
                # Create a staged blob and resumable url
                blob = bucket.blob(
                    f'{uid}/{dtype}/{name}/{stage_uuid}/{file_data["checksum"]}'
                )
                file_data['upload_url'] = blob.create_resumable_upload_session(
                    content_type = 'application/octet-stream'
                )
                response['missing_files'].append(file_data)
                # add checksum to the staged files set
                staged_files.add(file_data['checksum'])
            else:
                response['uploaded_files'].append(file_data)
    return Response(
        response=json.dumps(response),
        status=status,
        mimetype='application/json'
    )

@authenticated
def commit_file(request):
    data = request.get_json()
    # Get the uid from the token
    uid = get_uid(request) 
    dtype = data['dtype']
    name = data['name']
    stage_uuid = data['stage_uuid']
    checksum = data['checksum']

    client = storage.Client()
    staged_bucket = client.bucket('minus80-staging')
    staged_blob = staged_bucket.blob(
        f'{uid}/{dtype}/{name}/{stage_uuid}/{checksum}'
    )
    # Calculate the checksum to verify that the data was uploaded right
    # Try this twice before reporting a bad file
    try:
        if (hashlib.sha256(staged_blob.download_as_string()).hexdigest() == checksum or 
            hashlib.sha256(staged_blob.download_as_string()).hexdigest() == checksum):
            # Try again ... 
            # Set up the target blob and transfer
            target_bucket = client.bucket('minus80')
            target_blob = target_bucket.blob(
                f'{uid}/{dtype}/{name}/{checksum}'                
            )
            target_blob.rewrite(staged_blob)
            # add the file name to the firebase document 
            db = firestore.client()
            dataset_ref = db.document(
                f"Frozen/{uid}/DatasetType/{dtype}/Dataset/{name}"
            ) 
            dataset_ref.update({
                    'available_files':firestore.ArrayUnion([checksum])
                }
            )
            status = 200
            data = {}
        else:
            status = 409
            data = {
                        
            }
    except NotFound as e:
        breakpoint()

    # delete the staged file blob
    staged_blob.delete()

    # return a success
    return Response(
        mimetype='application/json',
        status=status, 
        response=data,
    )

@authenticated
def commit_tag(request):
    data = request.get_json()
    uid = get_uid(request) 
    dtype = data['dtype']
    name = data['name']
    tag = data['tag']
    tag_data = data['tag_data']

    # update firestore
    db = firestore.client()
    dataset_ref = db.document(
        f"Frozen/{uid}/DatasetType/{dtype}/Dataset/{name}"
    ) 
    # Add the tag to the actual document
    dataset_ref.update({
            'available_tags':firestore.ArrayUnion([tag])
        }
    )
    tag_ref = db.document(
        f"Frozen/{uid}/DatasetType/{dtype}/Dataset/{name}/Tag/{tag}"
    )
    # Add the tad document
    tag_ref.set(
        tag_data 
    )
    # update the listing in the base document
    db.document(f"Frozen/{uid}").update({
        'available_datasets': { 
            dtype : {
                name : {
                    tag : {
                        'checksum': tag_data['total'],
                        'created' : tag_data['timestamp']
                    }
                }
            }
        }
    })

    # Return a response
    return Response(
        response=json.dumps({}),
        mimetype='application/json',
        status=200
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
    db = firestore.client()
    db.document(f'Frozen/{owner_uid}').set({
        'available_datasets':{}           
    })
    dataset_ref = db.document(
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

    @app.route('/list_datasets', methods=['GET'])
    def do_list_datasets():
        return list_datasets(request)

    @app.route('/push',methods=['POST'])
    def do_push():
        return push(request)

    @app.route('/stage_files',methods=['POST'])
    def do_stage_files():
        return stage_files(request)

    @app.route('/commit_file', methods=['POST'])
    def do_commit_file():
        return commit_file(request)

    @app.route('/commit_tag', methods=['POST'])
    def do_commit_tag():
        return commit_tag(request)

    app.run(
        '127.0.0.1', 
        50000, 
        debug=True,
        ssl_context=('cert.pem', 'key.pem')
    )
