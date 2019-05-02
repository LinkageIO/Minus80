#!/usr/bin/env python3

import io
import re
import os

from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install
from subprocess import check_call,CalledProcessError


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

def install_apsw(method='pip',version='3.27.2',tag='-r1'):
    if method == 'pip':
        print('Installing apsw from GitHub using ')
        version = '3.27.2'
        tag = '-r1'
        check_call(f'''\
            pip install  \
            https://github.com/rogerbinns/apsw/releases/download/{version}{tag}/apsw-{version}{tag}.zip \
            --global-option=fetch \
            --global-option=--version \
            --global-option={version} \
            --global-option=--all \
            --global-option=build  \
            --global-option=--enable=rtree \
        '''.split())
    else:
        raise ValueError(f'{method} not supported to install apsw')


class DevelopCommand(develop):
    """
        Installation (develop) command that pre-installs APSW since its not on pypi
    """
    def run(self):
        install_apsw()
        develop.run(self)
class InstallCommand(install):
    """
        Installation command that pre-installs APSW since its not on pypi
    """ 
    def run(self):
        install_apsw()
        install.run(self)



setup(
    name = 'minus80',
    version = find_version('minus80','__init__.py'),#+'-dev3',
    description = 'A library for freezing, unfreezing and storing biological data.',
    url = 'http://linkage.io',
    author = 'Rob Schaefer',
    author_email = 'rob@linkage.io',
    license = "Copyright Linkage Analytics 2016-2018. Available under the MIT License",

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
        'Documentation' : 'https://minus80.readthedocs.io/en/latest/',
        'Source' : 'https://github.com/LinkageIO/Minus80',
        'Tracker' : 'https://github.com/LinkageIO/Minus80/issues'
    },

    packages = find_packages(),
    scripts = [
    ],
    ext_modules = [],
    cmdclass = {
        'develop': DevelopCommand,
        'install': InstallCommand,
    },

    package_data = {
        '':['*.cyx']    
    },
    install_requires = [		
        'google-cloud >= 0.34.0',
        'google-cloud-storage >= 1.14.0',
        'pandas<=0.23.9',		
        'bcolz >= 1.2.1',
        'blaze >= 0.10.1',
        'termcolor >= 1.1.0',
        'pyyaml >= 3.12',
        'click >= 6.7',
        'asyncssh >= 1.12.2',
        'networkx == 1.11',
        'urllib3 == 1.24.2',
        'boto3 >= 1.7.84',
        'requests >= 2.19.1',
        'fuzzywuzzy >= 0.17.0',
        'python-Levenshtein >= 0.12.0',
        'tqdm >= 4.28.1',
        'backoff >= 1.7.1'
    ],
    extras_require={
        'docs' : ['ipython>=6.5.0','matplotlib>=2.2.3']
    },
    #dependency_links = [
    #    'git+https://github.com/rogerbinns/apsw'
    #],
    include_package_data=True,
    entry_points='''
        [console_scripts]
        minus80=minus80.cli.minus80:cli
    ''',
)
