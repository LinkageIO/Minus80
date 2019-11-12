import os
import re
import json
import random
import asyncio
import aiohttp
import getpass
import logging
import hashlib
import pyrebase
import requests

from tqdm import tqdm
from pathlib import Path
from functools import wraps
from tinydb import TinyDB, where
from async_timeout import timeout

from .CloudData import BaseCloudData
from .Config import cf

from .Exceptions import (
    TagExistsError,
    TagDoesNotExistError,
    UserNotLoggedInError,
    UserNotVerifiedError,
    PushFailedError
)

from minus80 import API_VERSION

class FireBaseCloudData(BaseCloudData):

    # Production Options
    #URL_BASE = 'https://us-central1-minus80.cloudfunctions.net/'
    #VERIFY = False

    # Debug Options
    URL_BASE = 'https://127.0.0.1:50000/'
    VERIFY = False

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
        # set up the log
        self.log = logging.getLogger(f"minus80.CloudData")

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

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._session.close()
        self._session = None

    async def push(
        self, 
        dtype, 
        name, 
        tag, 
        max_conc_upload=5,
        progress=True
    ):
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
        # Stage the tags files
        async with self:
            resp = await self._session.post(
                url = self.URL_BASE + 'stage_files',
                headers=headers,
                json=data,
                ssl=self.VERIFY # here for debugging on localhost
            )
            # Process the results
            if resp.status == 409:
                raise TagExistsError('Tag already exists')
            elif resp.status != 200:
                raise PushFailedError(resp)
            resp = await resp.json()
            # Log what the server found
            self.log.debug(f'For tag: {data}, server says we need: {resp}')
            # Create tasks to upload files
            sem = asyncio.Semaphore(max_conc_upload)
            upload_tasks = []
            for i,file_data in enumerate(resp['missing_files']):
                # set up progress bars
                pbar = None
                if progress and self.log.getEffectiveLevel() > logging.DEBUG:
                    pbar = tqdm(
                        desc = file_data['checksum'][0:10],
                        total = file_data['size'],
                        position = i,
                        leave=False
                    )
                upload_tasks.append(
                    asyncio.create_task(
                        self._upload_file(
                            dtype,
                            name,
                            file_data['checksum'],
                            file_data['size'],
                            file_data['upload_url'],
                            resp['stage_uuid'],
                            sem, # Semaphore
                            pbar=pbar
                    ))
                )
            # wait on the uploads
            self.log.info(f'Need to upload {len(upload_tasks)} files')
            await asyncio.gather(*upload_tasks,return_exceptions=True)
            
            # Process the results and report back the results
            upload_success = True
            for task in upload_tasks:
                result = task.result()
                if result['status'] == 'SUCCESS':
                    self.log.info(f'Success uploading {result["checksum"]}')
                elif results['status'] == 'FAIL':
                    self.log.warning(f'Failed uploading {result["checksum"]}: {result["exception"]}')
                    upload_success = False 
            
            # If all files were uploaded, commit the tag 
            if upload_success == True:
                resp = await self._session.post(
                    url = self.URL_BASE + 'commit_tag',
                    headers=headers,
                    json=data,
                    ssl=self.VERIFY # here for debugging on localhost
                )
                if resp.status != 200:
                    breakpoint()
                    self.log.warning(
                        f'Failed to push tag "{dtype}.{name}:{tag}" to the cloud. '
                        f'See above error messages or try again later.'
                    )
                else:
                    self.log.info(f'Push complete: {dtype}.{name}:{tag}')
            else:
                self.log.warning(
                    f'Failed to push tag "{dtype}.{name}:{tag}" to the cloud. '
                    f'See above error messages or try again later.'
                )

    async def _upload_file(
        self,
        dtype,
        name,
        checksum,
        size,
        url,
        stage_uuid,
        sem,
        chunk_size=1*(1024*1024), # Megabytes
        max_tries=10,
        pbar=None,
        debug=False
    ):
        '''
        Asynchronously upload a file to a google cloud storage bucket
        using a resumable upload url.

        dtype: str 
            the m80 data type used in the tag
        name: str
            the m80 dataset name (used in the tag)
        checksum
        '''
        try:
            from contextlib import AsyncExitStack
            # Set some state variables
            upload_complete = False
            cur_byte = 0   
            cur_tries = 1
            # enter some async contexts
            async with AsyncExitStack() as stack:
                # await on the semaphore
                await stack.enter_async_context(sem)
                # You can use non async contexts too!
                file_path = Path(cf.options.basedir)/'datasets'/API_VERSION/f'{dtype}.{name}'/'frozen'/checksum
                f = stack.enter_context(open(file_path,'rb'))
                self.log.debug(f'Uploading {checksum}')
                # Loop and send the necessary Data
                while not upload_complete and cur_tries < max_tries:
                    if cur_byte is not None:
                        # Seek to the current byte
                        f.seek(cur_byte)
                        # Read in the chunk_size
                        chunk = f.read(chunk_size)
                        # Get the md5 of the chunk
                        chunk_checksum = hashlib.md5(chunk).hexdigest()
                        if debug and random.randint(0,10):
                            self.log.debug(f'DEBUG: Simulating a bad checksum for {checksum}')
                            chunk_checksum = 'x' + chunk_checksum[1:]
                        # Send it!
                        headers={
                            'Content-Length': f'{len(chunk)}',
                            'Content-Type'  : 'application/octet-stream',
                            'Content-MD5'   : chunk_checksum,
                        }
                        # If the size of the file is not zero, calculate
                        # the Content-Range
                        if size != 0:
                            start = cur_byte
                            stop = max(cur_byte, (cur_byte+len(chunk)-1))
                            headers['Content-Range'] = f'bytes {start}-{stop}/{size}'
                            self.log.debug(f'Uploading: {headers["Content-Range"]}')
                    else:
                        # Exponentially back-off
                        self.log.debug(f'Backing off for {2**cur_tries} seconds on {checksum}')
                        await asyncio.sleep(2 ** cur_tries)
                        cur_tries += 1
                        data=None
                        headers={
                            'Content-Length' : '0',
                            'Content-Type'  : 'application/octet-stream',
                            'Content-Range':f'bytes */{size}'
                        }
                    # Make our request and process the output
                    async with self._session.put(
                        url,
                        data=chunk,
                        headers=headers
                    ) as resp:
                        resp_text = await resp.text() # useful for debugging
                        if resp.status == 200 or resp.status == 201:
                            # The file was successfully uploaded!
                            upload_complete = True
                            if pbar is not None:
                                pbar.update(size)
                        elif resp.status == 308:
                            try:
                                # add some debug code to simulate a resumed upload
                                if debug:
                                    if random.randint(0, 10) == 0:
                                        self.log.debug(f"DEBUG: simulating a weird bug! on {checksum}")
                                        raise Exception
                                # Fetch the bounds of the uploaded bytes
                                m = re.fullmatch('bytes=(\d+)-(\d+)',resp.headers['Range'])
                                self.log.debug(f"Successful upload {m[0]} bytes on {checksum}")
                                if pbar is not None:
                                    pbar.update(int(m[2]))
                                stop = m[2]
                                # Reset the current byte to what ever google wants
                                cur_byte = int(stop) + 1
                                # Reset the exponential backoff to 0
                                cur_tries = 1
                            except Exception as e:
                                # If for some reason the header is weird
                                # Try to recover by initiatine a resume
                                cur_byte = None
                        else:
                            # Something bad happened! Try to resume next loop
                            cur_byte = None
                if not upload_complete: # We got here because cur_tries > max_tries
                    self.log.debug('Upload failed after {max_tries} times')
                    return False
                else:
                    self.log.debug(f'Successfully staged {size} bytes of {checksum}')
                    result = await self._commit_staged_file(
                        stage_uuid,
                        dtype,
                        name,
                        checksum
                    )
                status = {
                    'status': 'SUCCESS',
                    'checksum' : checksum
                }
        except Exception as e:
            status = {
                'STATUS' : 'FAIL',
                'checksum' : checksum,
                'exception' : e
            }
        finally:
            return status

    async def _commit_staged_file(self,stage_uuid,dtype,name,checksum):
        '''
            Called by push
        '''
        headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {self.user['idToken']}"
        }
        data = {
            "api_version" : API_VERSION,
            'stage_uuid' : stage_uuid,
            'dtype' : dtype,
            'name' : name,
            'checksum' : checksum,
        }
        async with self._session.post(
            self.URL_BASE + 'commit_file',
            headers=headers,
            json=data,
            ssl=self.VERIFY
        ) as resp:
            if resp.status != 200:
                raise PushFailedError(resp)
            resp_json = await resp.json()
        self.log.debug(f'File {checksum} committed to {dtype}.{name}')

    def pull(self, dtype, name, tag):
        raise NotImplementedError("This engine does not support pulling")

    def list(self, dtype=None, name=None):
         





