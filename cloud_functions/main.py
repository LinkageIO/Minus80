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
def commit_tag(request):
    #  Check if the tag exists
   #if data['tag'] in dataset.get('available_tags'):
   #    tag_ref = db.document(
   #        f"Frozen/{uid}/DatasetType/{dtype}/Dataset/{name}/Tags/{tag}"
   #    )
   #    tag_data = tag_ref.get().to_dict()
   #    # If the tag already exists and the checksum is different, 
   #    # do not allow the push to happen
   #    return abort(409,'tag exists')

   #    if tag_data['status'] == 'COMPLETE':
   #        return abort(409,'tag exists')
   #else:
   #    # Otherwise add the tag data
   #    tag_data = data['tag_data']
   #    # Set the status to pending
   #    tag_data['status'] = 'PENDING'
   #    # Grab the tag document and add the new tag data
   #    tag_ref = db.document(
   #        f"Frozen/{uid}/DatasetType/{dtype}/Dataset/{name}/Tags/{tag}"
   #    )
   #    tag_ref.set(
   #        tag_data
   #    )
   #    # Add the tag data to the project entry
   #    dataset_ref.update({
   #        'available_tags' : firestore.ArrayUnion([tag,])
   #    })
    pass
    for tagfile,fileinfo in tag_data['files'].items():
        # Fetch the file document
        file_ref = (
            dataset_ref
            .collection('Files')
            .document(fileinfo['checksum'])
        )
        filedata = file_ref.get()
        if not filedata.exists:
            # create the document and add the file to the results
            fields = fileinfo
            fields.update({'url':None})
            file_ref.create(fields)
            missing_files.append(fileinfo)
        elif filedata.get('url') is None:
            # If the URL isn't set, try to upload
            missing_files.append(fileinfo)
        else:
            pass

@authenticated
def stage_file(request):
    data = request.get_json()
    # Get the uid from the token
    dtype = data['dtype']
    name = data['name']
    uid = get_uid(request) 

    checksum = data['']

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
        'missing_files' : {},
        'uploaded_files' : []
    }
    # figure out what files are missing 
    client = storage.Client()
    bucket = client.bucket('minus80')
    for file_data in data['tag_data']['files'].values():
        if file_data['checksum'] not in dataset_files:
            # Create a staged blob and resumable url
            blob = bucket.blob(
                f'{uid}/staged/{stage_uuid}/{dtype}/{name}'
            )
            url = blob.create_resumable_upload_session(
                content_type = 'application/octet-stream'
            )
            response['missing_files'][file_data['checksum']] = url
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
