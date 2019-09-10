#!/usr/bin/env python3

import click
import minus80 as m80
import minus80.Tools

from minus80.Exceptions import (TagInvalidError,
                                FreezableNameInvalidError,
                                TagExistsError,
                                TagDoesNotExistError)

class NaturalOrderGroup(click.Group):
    '''
        This subclass orders the commands in the @click.group
        in the order in which they are defined in the script
        **Ref** https://github.com/pallets/click/issues/513
    '''
    def list_commands(self, ctx):
        return self.commands.keys()


@click.group(
    cls=NaturalOrderGroup,
    epilog=f"Made with ❤️  in Denver -- Version {m80.__version__}"
)
def cli():
    """
    \b
        __  ____                  ____  ____
       /  |/  (_)___  __  _______( __ )/ __ \\
      / /|_/ / / __ \/ / / / ___/ /_/ / / / /
     / /  / / / / / / /_/ (__  ) /_/ / /_/ /
    /_/  /_/_/_/ /_/\__,_/____/\____/\____/


    Minus80 is a library for storing biological data. 

    See https://github.com/LinkageIO/minus80
    for more details.
    """


# ----------------------------
#    init Commands
# ----------------------------
@click.command(
    short_help='Initialize a new minus80 project'        
)
@click.argument(
    'name',
)
@click.argument(
    'directory'        
)
def init(name,directory):
    x = m80.Project(name)
    x.dir = directory

cli.add_command(init)

# ----------------------------
#    List Commands
# ----------------------------
@click.command(
    short_help="List the available minus80 datasets",
    help="Reports the available datasets **Frozen** in the minus80 database.",
)
@click.option(
    "--dtype",
    default=None,
    help=("Each dataset has a datatype associated with it. "
         "E.g.: `Cohort`. If no dtype is specified, all "
         "available dtypes  will be returned."),
)
@click.option(
    "--name",
    default=None,
    help=("The name of the dataset you want to check is available. "
         "The default value is the wildcard '*' which will return "
         "all available datasets with the specified dtype."),
)
@click.option(
    "--tags",
    default=False,
    is_flag=True,
    help=("List available tags of frozen datasets"),
)
def list(name, dtype, tags):
    minus80.Tools.available(dtype=dtype, name=name, tags=tags)

cli.add_command(list)

# ----------------------------
#    delete Commands
# ----------------------------
@click.command(help="Delete a minus80 dataset")
@click.argument("slug", metavar="<slug>")
def delete(slug):
    # Validate the input
    try:
        dtype,name,tag = minus80.Tools.parse_slug(slug) 
        if tag is not None:
            raise TagInvalidError()
    except (TagInvalidError, FreezableNameInvalidError):
        click.echo(
            f'Please provide a valid tag in "{slug}"'
        )
        return 0
    # Make sure that the dataset is available
    if not minus80.Tools.available(dtype,name):
        click.echo(
            f'"{dtype}.{name}" not in minus80 datasets! '
            'check available datasets with the list command'
        )
        return 0
    else:
        minus80.Tools.delete(dtype, name)


cli.add_command(delete)

# ----------------------------
#    Freeze Command
# ----------------------------

@click.command(help='Freeze a minus80 dataset')
@click.argument("slug",metavar="<slug>")
def freeze(slug):
    # Validate the input
    try:
        dtype,name,tag = minus80.Tools.parse_slug(slug) 
        if tag is None:
            raise TagInvalidError()
    except (TagInvalidError, FreezableNameInvalidError):
        click.echo(
            f'Please provide a valid tag in "{slug}"'
        )
        return 0
    # Make sure that the dataset is available
    if not minus80.Tools.available(dtype,name):
        click.echo(
            f'"{dtype}.{name}" not in minus80 datasets! '
            'check available datasets with the list command'
        )
        return 0
    else:
        # Create the minus80 
        try:
            dataset = getattr(minus80,dtype)(name)
        except Exception as e:
            click.echo(f'Could not build {dtype}.{name}')
            raise e
            return 1
        # Freeze with tag
        try:
            dataset.m80.freeze(tag)
            click.echo(click.style("SUCCESS!",fg="green",bold=True))
        except TagExistsError:
            click.echo(f'tag "{tag}" already exists for {dtype}.{name}')

cli.add_command(freeze)

@click.command(help='Thaw a minus80 dataset')
@click.argument("slug",metavar="<slug>")
def thaw(slug):
    try:
        dtype,name,tag = minus80.Tools.parse_slug(slug) 
        if tag is None:
            raise TagInvalidError()
    except (TagInvalidError, FreezableNameInvalidError):
        click.echo(
            f'Please provide a valid tag in "{slug}"'
        )
        return 0
    # Make sure that the dataset is available
    if not minus80.Tools.available(dtype,name):
        click.echo(
            f'"{dtype}.{name}" not in minus80 datasets! '
            'check available datasets with the list command'
        )
        return 0
    else:
        # Create the minus80 
        try:
            dataset = getattr(minus80,dtype)(name)
        except Exception as e:
            click.echo(f'Could not build {dtype}.{name}')
        # Freeze with tag
        try:
            dataset.m80.thaw(tag)
            click.echo(click.style("SUCCESS!",fg="green",bold=True))
        except TagDoesNotExistError:
            click.echo(f'tag "{tag}" does not exist for {dtype}.{name}')
        
cli.add_command(thaw)

# ----------------------------
#    Cloud Commands
# ----------------------------
@click.group()
def cloud():
    """
    Manage your frozen minus80 datasets in the cloud.
    """
cli.add_command(cloud)

@click.command()
@click.option("--dtype", metavar="<dtype>", default=None)
@click.option("--name", metavar="<name>", default=None)
def list(dtype, name):
    """List available datasets"""
    cloud = m80.CloudData()
    cloud.list(dtype=dtype, name=name)


@click.command()
@click.argument("slug", metavar="<slug>")
def push(slug):
    """
    \b
    Push a frozen minus80 dataset to the cloud.

    \b
    Positional Arguments:
    <slug> - A slug of a frozen minus80 dataset
    """
    try:
        dtype,name,tag = minus80.Tools.parse_slug(slug) 
        if tag is None:
            raise TagInvalidError()
    except (TagInvalidError, FreezableNameInvalidError):
        click.echo(
            f'Please provide a valid tag in "{slug}"'
        )
        return 0
    # Make sure that the dataset is available
    if not minus80.Tools.available(dtype,name):
        click.echo(
            f'"{dtype}.{name}" not in minus80 datasets! '
            'check available datasets with the list command'
        )
        return 0
    else:
        cloud = m80.CloudData()
        try:
            cloud.push(dtype, name, tag)
        except TagDoesNotExistError as e:
            click.echo(f'tag "{tag}" does not exist for {dtype}.{name}')


@click.command()
@click.argument("dtype", metavar="<dtype>")
@click.argument("name", metavar="<name>")
@click.option("--raw", is_flag=True, default=False, help="Flag to list raw data")
@click.option(
    "--output",
    default=None,
    help="Output filename, defaults to <name>. Only valid with --raw",
)
def pull(dtype, name, raw, output):
    """
    Pull a minus80 dataset from the cloud.
    """
    cloud = m80.CloudData()
    cloud.pull(dtype, name, raw=raw, output=output)


@click.command()
@click.argument("dtype", metavar="<dtype>")
@click.argument("name", metavar="<name>")
@click.option("--raw", is_flag=True, default=False, help="Flag to list raw data")
def remove(dtype, name, raw):
    """
    Delete a minus80 dataset from the cloud.
    """
    cloud = m80.CloudData()
    cloud.remove(dtype, name, raw)


cloud.add_command(list)
cloud.add_command(push)
cloud.add_command(pull)
cloud.add_command(remove)



@click.command(help='Additional information')
def info():
    print(f'Version: {m80.__version__}')
    print(f'Installation Path: {m80.__file__}')

cli.add_command(info)
