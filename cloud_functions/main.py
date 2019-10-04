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
    token = request.headers['Authorization'].replace('Bearer ','')
    userId = auth.verify_id_token(token)['uid']
    # Fire up firestore
    db = firestore.client()
    rack = db.collection('rack').document(data['dtype'])
    # Get the dataset 
    dataset = list(
        rack.collection('datasets')
        .where('owner','==',userId)
        .where('name','==',data['name'])
        .stream()
    )
    # the dataset does not exist, add it
    if len(dataset) == 0:
        dataset = add_dataset(rack, data['name'], userId)
    else:
        # we need to pop this 
        dataset = dataset[0]    

    if user_ref.collection('tags').document()
    # set the tag
    tag_ref.set(data['data'])


# Helper Methods -------------------

def add_dataset(rack,name,owner):
    '''
        Adds a dataset to a rack

        Parameters
        ----------
        name : str
            The name of the dataset to add
        owner : uid
            The uid of the owner of the dataset

        Returns
        -------
        <google.cloud.firestore_v1.document.DocumentSnapshot>
            A snapshot of the added document 
    '''
    rack.collection('datasets').add({
        'name'  : data['name'],
        'owner' : userId,
        'files' : [],
    })
    # return a reference to the new dataset
    return list(
        rack.collection('datasets')
        .where('owner','==',userId)
        .where('name','==',data['name'])
        .stream()
    ).pop()

