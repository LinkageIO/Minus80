#!/usr/bin/env python3

import click
import minus80 as m80


@click.group(epilog=f'Made with Love in St Paul -- Version {m80.__version__}\n{m80.__file__}')
def cli():
    '''
    \b
        __  ____                  ____  ____
       /  |/  (_)___  __  _______( __ )/ __ \\
      / /|_/ / / __ \/ / / / ___/ __  / / / /
     / /  / / / / / / /_/ (__  ) /_/ / /_/ /
    /_/  /_/_/_/ /_/\__,_/____/\____/\____/


    Minus80 is a library for storing biological data. See linkage.io/docs/minus80
    for more details.
    '''

#----------------------------
#    List Commands
#----------------------------
@click.command(short_help='List the available minus80 datasets',
    help='Reports the available datasets **Frozen** in the minus80 database.'
)
@click.option('--name',  default=None,
    help="The name of the dataset you want to check is available. The default value is the wildcard '*' which will return all available datasets with the specified dtype."
)
@click.option('--dtype', default=None,
    help='Each dataset has a datatype associated with it. E.g.: `Cohort`. If no dtype is specified, all available dtypes  will be returned.'
)
def list(name, dtype):
    m80.Tools.available(dtype=dtype,name=name)

cli.add_command(list)

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
@click.option('--dtype', metavar='<dtype>',default=None)
@click.option('--name', metavar='<name>',default=None)
@click.option('--raw', is_flag=True, default=False, help='Flag to list raw data')
def list(dtype,name,raw):
    '''List available datasets'''
    cloud = m80.CloudData()
    cloud.list(
        dtype,
        name,
        raw
    )


@click.command()
@click.argument('dtype', metavar='<dtype>')
@click.argument('name', metavar='<name>')
@click.option('--raw', is_flag=True, default=False, help='Flag to list raw data')
def push(dtype, name, raw):
    '''
    \b
    Push a minus80 dataset in the cloud.

    \b
    Positional Arguments:
    <dtype> - the data type of the m80 dataset or a raw file description (e.g. Cohort).
    <name> - the name of the m80 dataset or raw filename if --raw is set.
    '''
    cloud = m80.CloudData()
    cloud.push(
        dtype,
        name,
        raw=raw
    )

@click.command()
@click.argument('dtype', metavar='<dtype>')
@click.argument('name', metavar='<name>')
@click.option('--raw', is_flag=True, default=False, help='Flag to list raw data')
@click.option('--output',default=None,help="Output filename, defaults to <name>. Only valid with --raw")
def pull(dtype,name,raw,output):
    '''
    Pull a minus80 dataset from the cloud.
    '''
    cloud = m80.CloudData()
    cloud.pull(
        dtype,
        name,
        raw=raw,
        output=output
    )

cloud.add_command(list)
cloud.add_command(push)
cloud.add_command(pull)
