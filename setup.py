#!/usr/bin/env python3

from setuptools import setup, find_packages, Extension
from Cython.Distutils import build_ext
import os

import io
import re
import numpy

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
    name = 'M80',
    version = find_version('M80','__init__.py'),
    packages = find_packages(),
    scripts = [
    ],
    ext_modules = [],
    cmdclass = {'build_ext': build_ext},

    package_data = {
        '':['*.cyx']    
    },
    install_requires = [		
        'pandas>=0.18',		
        'Cython'
    ],
    include_package_data=True,

    author = 'Rob Schaefer',
    author_email = 'schae234@gmail.com',
    description = 'Library for handling Samples.',
    license = "Copyright Rob Schaefer 2016",
    url = ''
)
