#!/usr/bin/env python3

import sys
import argparse
import minus80 as m80
import click



@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    '''
    \b
        __  ____                  ____  ____ 
       /  |/  (_)___  __  _______( __ )/ __ \\
      / /|_/ / / __ \/ / / / ___/ __  / / / /
     / /  / / / / / / /_/ (__  ) /_/ / /_/ / 
    /_/  /_/_/_/ /_/\__,_/____/\____/\____/  
    
    Minus80 is a library for storing biological data. See minus80.linkage.io 
    for more details.
    '''
@cli.command(short_help='List the available minus80 datasets',
    help='Reports the available datasets **Frozen** in the minus80 database.'
)
@click.option('--name',  default='*', 
    help="The name of the dataset you want to check is available. The default value is the wildcard '*' which will return all available datasets with the specified dtype."
)
@click.option('--dtype', default='',
    help='Each dataset has a datatype associated with it. E.g.: `Cohort`. If no dtype is specified, all available dtypes  will be returned.'
)
def available(name,dtype):
    m80.Tools.available(name,dtype)
