#!/usr/env/python3

import os
import yaml
import pprint
import getpass

global cf

default_config = '''--- # YAML Minus80 Configuration File
options:
    basedir: ~/.minus80/

gcp:
    credentials: ~/.minus80/gcp_creds.json
    endpoint: https://storage.cloud.google.com
    bucket: minus80

'''

class Level(dict):
    '''
        Ha! Take that config parser! I am accessing
        everything like an object.
    '''
    def __init__(self,*args,**kwargs):
        # Create the dict
        super().__init__(*args,**kwargs)
        # Convert to Levels

        for k,v in self.items():
            if isinstance(v,dict):
                self[k] = Level(v)
            else:
                self[k] = v

    def __getattr__(self,item):
        if 'dir' in item and '~' in self[item]:
            return os.path.expanduser(self[item])
        return self[item]
    def __setattr__(self,item,val):
        self[item] = val



class Config(object):

    def __init__(self,filename):
        filename = os.path.expanduser(filename)
        self.data = Level(yaml.safe_load(open(filename,'r')))

    def __getattr__(self,item):
        return self.data[item]

    def __getitem__(self,item):
        return self.data[item]

    def __repr__(self):
        return pprint.pformat(self.data)

#  -------------------------------------------------------------------------
#        Program Logic


cf_file = os.path.expanduser('~/.minus80.conf')

# Check to see if there is a config file available
if not os.path.isfile(cf_file): # pragma: no cover
    with open(cf_file, 'w') as CF:
        print(default_config, file=CF)

cf = Config(cf_file)
