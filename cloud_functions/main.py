import json
import firebase_admin

from flask import abort
from functools import wraps
from firebase_admin import auth,firestore

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
def push(request):
    """
    Push a tagged dataset to the cloud. This function gets activated
    by the minus80.FireBaseCloudData.push method
    """
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
    #  Check if the tag exists
    if data['tag'] in dataset.get('tags'):
        return abort(409,'tag exists')
    # Otherwise
    dataset_ref.document(f"tags/{data['tag']}").set(
        data['tag_data']
    )
    return json.dumps(data,200)

# Helper Methods -------------------

def create_dataset(dtype,name,owner_uid):
    '''
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
        'dtype' : data['dtype'],
        'name' : data['name'],
        'onwer' : uid,
        'files' : [],
        'collaborators': [],
        'tags' : []
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
