import os
import json
import asyncio
import aiohttp
import getpass
import pyrebase
import requests

from pathlib import Path
from tinydb import TinyDB, where
from functools import wraps
from requests.exceptions import HTTPError

from .CloudData import BaseCloudData
from .Config import cf

from .Exceptions import (
    TagDoesNotExistError,
    UserNotLoggedInError,
    UserNotVerifiedError,
    PushFailedError
)

from minus80 import API_VERSION

def async_entry_point(fn): 
    @wraps(fn)
    def wrapped(self,*args,**kwargs):
        asyncio.run(fn(self,*args,**kwargs))
    return wrapped

class FireBaseCloudData(BaseCloudData):

    URL_BASE = 'https://us-central1-minus80.cloudfunctions.net/'
    #URL_BASE = 'https://127.0.0.1:50000/'
    VERIFY = True

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
        self._req = requests.Session()

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
                The email the user signed up with
            password: str
                The password the user signed up with
        '''
        try:
            user = self.auth.sign_in_with_email_and_password(email,password)
            self.user = user
        except HTTPError as e:
            raise e

    @async_entry_point
    async def push(self, dtype, name, tag, max_conc_upload=5):
        '''
        Pushes frozen tag data to the cloud.

        Parameters
        ----------
        dtype: str 
            The primary data type of the frozen dataset.
        name: str
            The name of the frozen dataset
        tag: str
            The tag of the frozen dataset
    
        '''
        manifest = TinyDB(
            Path(cf.options.basedir)/'datasets'/API_VERSION/f'{dtype}.{name}'/'MANIFEST.json'
        )
        # Fetch the tag
        tag_data = manifest.get(where('tag') == tag)
        if tag_data is None:
            raise TagDoesNotExistError 
        # Add the additional information
        headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {self.user['idToken']}"
        }
        data = {
            'api_version' : API_VERSION, 
            'dtype': dtype,
            'name': name,
            'tag' : tag,
            'tag_data' : tag_data
        }
        # Start asyncing
        async with aiohttp.ClientSession() as session:
            # Stage the tags files
            res = await session.post(
                url = self.URL_BASE + 'stage_files',
                headers=headers,
                json=data,
                ssl=self.VERIFY # here for debugging on localhost
            )
            async with res:
                if res.status != 200:
                    raise PushFailedError()
                response = await res.json()
                # Create tasks to upload files
                sem = asyncio.Semaphore(max_conc_upload)
                upload_tasks = []
                for checksum,url in response['missing_files'].items():
                    upload_tasks.append(
                        asyncio.create_task(self._upload_file(checksum,url,sem))
                    )
                # wait on the uploads
                await asyncio.gather(*upload_tasks)

    async def _upload_file(self,checksum,size,url,sem):
        '''
        Asynchronously upload a file to a google cloud storage bucket
        using a resumable upload url
        '''
        async with sem:
            # Upload file 
            asyncio.sleep(1)

    async def _commit_staged(self)::
        pass

    def pull(self, dtype, name, tag):
        raise NotImplementedError("This engine does not support pulling")

    def list(self, dtype=None, name=None):
        raise NotImplementedError("This engine does not support listing")
