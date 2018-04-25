#!/usr/bin/env python3

from setuptools import setup, find_packages, Extension
import os
from setuptools.command.develop import develop
from setuptools.command.install import install
from subprocess import check_call,CalledProcessError

import io
import re

def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

class PostDevelopCommand(develop):
    """
        Post-installation for development mode.
    """
    def run(self):
        try:
            print('Running post-installation for apsw')
            check_call('pip install -r requirements.txt'.split())
            check_call('''\
	    pip install --user https://github.com/rogerbinns/apsw/releases/download/3.22.0-r1/apsw-3.22.0-r1.zip \
	    --global-option=fetch --global-option=--version --global-option=3.22.0 --global-option=--all \
	    --global-option=build --global-option=--enable-all-extensions'''.split())
            develop.run(self)
        except CalledProcessError as e:
            pass
            

class PostInstallCommand(install):
    """
        Post-installation for installation mode.
    """
    def run(self):
        try:
            check_call('''\
	    pip install --user https://github.com/rogerbinns/apsw/releases/download/3.22.0-r1/apsw-3.22.0-r1.zip \
	    --global-option=fetch --global-option=--version --global-option=3.22.0 --global-option=--all \
	    --global-option=build --global-option=--enable-all-extensions'''.split())
            install.run(self)
        except CalledProcessError as e:
            print('a bad thing happened')
            raise e

setup(
    name = 'minus80',
    version = find_version('minus80','__init__.py'),#+'-dev3',
    description = 'A library for freezing, unfreezing and storing biological data.',
    url = 'http://linkage.io',
    author = 'Rob Schaefer',
    license = "Copyright Linkage Analytics 2016. Available under the MIT License",

    classifiers=[
	# How mature is this project? Common values are
	#   3 - Alpha
	#   4 - Beta
	#   5 - Production/Stable
	'Development Status :: 4 - Beta',

	# Indicate who your project is intended for
	'Intended Audience :: Developers',
	'Topic :: Software Development :: Build Tools',

	# Pick your license as you wish (should match "license" above)
	 'License :: OSI Approved :: MIT License',

	# Specify the Python versions you support here. In particular, ensure
	# that you indicate whether you support Python 2, Python 3 or both.
	'Programming Language :: Python :: 3',
	'Programming Language :: Python :: 3.6',
    ],
    keywords='data storage biology freeze', 
    project_urls={
        'Documentation' : 'http://linkage.io/docs/minus80',
        'Source' : 'https://github.com/LinkageIO/Minus80',
        'Tracker' : 'https://github.com/LinkageIO/Minus80/issues'
    },

    packages = find_packages(),
    scripts = [
    ],
    ext_modules = [],
    cmdclass = {
        'develop': PostDevelopCommand,
        'install': PostInstallCommand,
    },

    package_data = {
        '':['*.cyx']    
    },
    install_requires = [		
        'pandas >= 0.22.0',
        'bcolz >= 1.2.1',
        'blaze >= 0.10.1',
        'termcolor >= 1.1.0',
        'pyyaml >= 3.12',
        'click >= 6.7'
    ],
    include_package_data=True,
    entry_points='''
        [console_scripts]
        minus80=minus80.cli.minus80:cli
    ''',

    author_email = 'rob@linkage.io',
)
