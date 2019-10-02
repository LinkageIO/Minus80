import json
import firebase_admin

from flask import abort
from functools import wraps
from firebase_admin import auth

firebase_admin.initialize_app()

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

@authenticated
def push(request):
    """
    """
    request_json = request.get_json()
    return json.dumps(request_json) 

@authenticated
def echo(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    return json.dumps(request_json) 

