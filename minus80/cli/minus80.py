#!/usr/bin/env python3

import click
import minus80 as m80


@click.group(epilog=f'Made with Love in St Paul -- Version {m80.__version__}')
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

#----------------------------
#    Available Commands
#----------------------------
@click.command(short_help='List the available minus80 datasets',
    help='Reports the available datasets **Frozen** in the minus80 database.'
)
@click.option('--name',  default='*',
    help="The name of the dataset you want to check is available. The default value is the wildcard '*' which will return all available datasets with the specified dtype."
)
@click.option('--dtype', default='',
    help='Each dataset has a datatype associated with it. E.g.: `Cohort`. If no dtype is specified, all available dtypes  will be returned.'
)
def available(name, dtype):
    m80.Tools.available(name, dtype)

cli.add_command(available)

#----------------------------
#    delete Commands
#----------------------------
@click.command(help='Delete a minus80 dataset')
@click.option('--name',help='The name of the dataset to delete')
@click.option('--dtype',help='The dtype of the dataset to delete')
def delete(name, dtype):
    m80.Tools.delete(name,dtype)
cli.add_command(delete)

#----------------------------
#    Cloud Commands
#----------------------------
@click.group()
@click.option('--engine', default='s3', help='Cloud engine.')
@click.option('--raw/--no-raw', default=False, help='Flag to list raw data')
@click.option('--name', default=None, help='Name of m80 dataset')
@click.option('--dtype', default=None, help='Type of m80 dataset')
@click.pass_context
def cloud(ctx, engine, raw, name, dtype):
    '''
    Manage your minus80 datasets in the cloud.
    '''
    ctx.obj = {}
    ctx.obj['engine'] = engine
    ctx.obj['RAW'] = raw
    ctx.obj['NAME'] = name
    ctx.obj['DTYPE'] = dtype

cli.add_command(cloud)

@click.command()
@click.pass_context
def available(ctx):
    '''List available datasets'''
    cloud = m80.CloudData()
    raw = ctx.obj['RAW']
    cloud.list(
        raw=ctx.obj['RAW'],
        name=ctx.obj['NAME'],
        dtype=ctx.obj['DTYPE']
    )


@click.command()
def get():
    'Download a dataset from the cloud'
    pass

@click.command()
def put():
    'Upload a dataset in the cloud'
    pass

cloud.add_command(available)
cloud.add_command(get)
cloud.add_command(put)
