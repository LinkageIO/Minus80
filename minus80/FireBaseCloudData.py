import os
import json
import getpass
import pyrebase

from requests.exceptions import HTTPError

from .CloudData import BaseCloudData
from .Config import cf


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

        # Initialize
        if os.path.exists(self._token_file):
            self._load_token()
        else:
            self._login()

    @property
    def user(self):
        # user pre-loaded user
        if self._user is None:
            try: 
                self._load_token()
            except FileNotFoundError:
                self._login()
        return self._user

    @user.setter
    def user(self, new_user):
        # Return the new token
        if self._user is None:
            self._user = new_user 
        else:
            self._user.update(new_user)
        self._refresh_token()
        self._dump_token()

    @property        
    def _token_file(self):
        return os.path.join(
            os.path.expanduser(cf.options.basedir),
            'JWT.json'
        )

    @staticmethod
    def _validate_token_fields(token):
        valid_keys = [
            'kind', 'localId', 'email', 'displayName', 
            'idToken', 'registered', 'refreshToken', 
            'expiresIn', 'userId'
        ]
        for k in valid_keys:
            if k not in token:
                return False
        return True

    def _dump_token(self):
        self._refresh_token()
        token = self._user
        if not self._validate_token_fields(token):
            raise ValueError(f"Current token is invalid: {token}")
        json.dump(token, open(self._token_file,'w'))

    def _load_token(self):
        if not os.path.exists(self._token_file):
            raise FileNotFoundError('No token file to load')
        token = json.load(open(self._token_file,'r')) 
        if not self._validate_token_fields(token):
            raise ValueError(f"Current token is invalid: {token}")
        self._user = token
        self._refresh_token()

    def _refresh_token(self):
        old_user = self.user
        new_user = self.auth.refresh(self.user['refreshToken'])
        self._user.update(new_user)

    def _login(self):
        email = input('email: ')
        password = getpass.getpass('password: ')
        try:
            user = self.auth.sign_in_with_email_and_password(email,password)
            self.user = user
        except HTTPError as e:
            raise e
    


    def push(self, name, dtype, raw=False, compress=False):
        raise NotImplementedError("This engine does not support pushing")

    def pull(self, name, dtype, raw=False):
        raise NotImplementedError("This engine does not support pulling")

    def list(self, name=None, dtype=None, raw=None):
        raise NotImplementedError("This engine does not support listing")
