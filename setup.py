#!/usr/bin/env python3

from setuptools import setup, find_packages
import os

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
        'click >= 6.7',
        'asyncssh >= 1.12.2',
        'networkx == 1.11',
        'boto3>=1.7.84',
        'apsw'
    ],
    extras_require={
        'docs' : ['ipython>=6.5.0','matplotlib>=2.2.3']
    },
    dependency_links = [
        'git+https://github.com/rogerbinns/apsw'
    ],
    include_package_data=True,
    entry_points='''
        [console_scripts]
        minus80=minus80.cli.minus80:cli
    ''',
    author_email = 'rob@linkage.io',
)
