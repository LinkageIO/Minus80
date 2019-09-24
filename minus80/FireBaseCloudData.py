import os
import json
import getpass
import pyrebase

from pathlib import Path
from tinydb import TinyDB, where
from requests.exceptions import HTTPError

from .CloudData import BaseCloudData
from .Config import cf

from .Exceptions import (
    TagDoesNotExistError,
    UserNotLoggedInError,
    UserNotVerifiedError
)

from minus80 import SLUG_VERSION

def ensure_valid_user(fn):
    from functools import wraps

    @wraps(fn)
    def  wrapped(self, *args, **kwargs):
        # make sure logged in
        if os.path.exists(self._token_file):
            self._load_token()
            self._refresh_token()
        else:
            raise NotLoggedInError
        # execute the function
        result = fn(self, *args, **kwargs)
        return result
    return wrapped


class FireBaseCloudData(BaseCloudData):

    config = {
        "apiKey": "AIzaSyCK8ItbVKqvBfwgBU74_EjvKHtl0Pi8r04",
        "authDomain": "minus80.firebaseapp.com",
        "databaseURL": "https://minus80.firebaseio.com",
        "storageBucket": "minus80.appspot.com"
    }

    def __init__(self):
        # get a firebase instance
        self.firebase = pyrebase.initialize_app(self.config)
        self.auth = self.firebase.auth()
        self._user = None

    @property
    def user(self):
        # user pre-loaded user
        if self._user is None:
            try: 
                self._user = self._load_token()
            except FileNotFoundError:
                raise UserNotLoggedInError()
        # else refresh
        refresh = self._refresh_token(self._user)
        self._user.update(refresh)
        # Dump refreshed token
        self._dump_token(self._user)
        # return fresh token 
        return self._user

    @user.setter
    def user(self, new_user):
        # Return the new token
        if self._user is None:
            self._user = {}
        self._user.update(new_user)
        # Try to refresh token
        refresh = self._refresh_token(self._user)
        self._user.update(refresh)
        # Dump refreshed token
        self._dump_token(self._user)

    @property        
    def _token_file(self):
        '''
            Returns a path to a token file based
            on the base directory defined in the 
            config file.
        '''
        return os.path.join(
            os.path.expanduser(cf.options.basedir),
            'JWT.json'
        )

    @staticmethod
    def _validate_token_fields(token):
        '''
            Validates a token
        '''
        valid_keys = [
            'kind', 'localId', 'email', 'displayName', 
            'idToken', 'registered', 'refreshToken', 
            'expiresIn', 'userId'
        ]
        for k in valid_keys:
            if k not in token:
                return False
        return True

    def _dump_token(self,token):
        json.dump(token, open(self._token_file,'w'))

    def _load_token(self):
        if not os.path.exists(self._token_file):
            raise FileNotFoundError('No token file to load')
        token = json.load(open(self._token_file,'r')) 
        return token

    def _refresh_token(self,token):
        new_user = self.auth.refresh(token['refreshToken'])
        return new_user

    def login(self,email,password):
        '''
            Log into minus80.linkage.io using a username and password

            Parameters
            ----------
            email: str
                 

        '''
        try:
            user = self.auth.sign_in_with_email_and_password(email,password)
            self.user = user
        except HTTPError as e:
            raise e

    def push(self, dtype, name, tag):
        manifest = TinyDB(
            Path(cf.options.basedir)/'datasets'/SLUG_VERSION/f'{dtype}.{name}'/'MANIFEST.json'
        )
        tag_data = manifest.get(where('tag') == tag)
        if tag_data is None:
            raise TagDoesNotExistError 
        # build the REST query
        headers = {"content-type": "application/json; charset=UTF-8"}
        data = {
            'user' : self.user,
            'slug' : f'{dtype}/{name}:{tag}',
            'tag' : tag_data
        }
        req_url = 'https://us-central1-minus80.cloudfunctions.net/function-1'

    def pull(self, dtype, name, tag):
        raise NotImplementedError("This engine does not support pulling")

    def list(self, dtype=None, name=None):
        raise NotImplementedError("This engine does not support listing")
