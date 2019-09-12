#!/usr/bin/env python3

import os
import json
import click
import minus80 as m80
import minus80.Tools

from pathlib import Path

from minus80.Exceptions import (TagInvalidError,
                                FreezableNameInvalidError,
                                TagExistsError,
                                TagDoesNotExistError,
                                UserNotLoggedInError,
                                UserNotVerifiedError,
                                UnsavedChangesInThawedError)
from requests.exceptions import HTTPError


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
    epilog=f"Made with ❤️  in Denver, Colorado"
)
def cli():
    """
    \b
        __  ____                  ____  ____
       /  |/  (_)___  __  _______( __ )/ __ \\
      / /|_/ / / __ \/ / / / ___/ /_/ / / / /
     / /  / / / / / / /_/ (__  ) /_/ / /_/ /
    /_/  /_/_/_/ /_/\__,_/____/\____/\____/


    Track, tag, store, and share biological datasets. 

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
@click.option(
    '--path',
    default=None,
    help='If specified, the minus80 project directory for NAME will be created here'
)
def init(name,path):
    x = m80.Project(name)
    if path is not None:
        path = str(Path(path)/name)
    else:
        path = str(Path.cwd()/name)
    try:
        x.create_link(path)
    except ValueError as e:
        click.echo(f'cannot create project directroy at: "{path}", directory already exists')


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
@click.option("--force",is_flag=True,default=False,help='forces a thaw, even if there are unsaved changes',)
def thaw(slug,force):
    try:
        cwd = Path.cwd().resolve()
    except FileNotFoundError as e:
        cwd = '/' 
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
            dataset.m80.thaw(tag,force=force)
            click.echo(click.style("SUCCESS!",fg="green",bold=True))
        except TagDoesNotExistError:
            click.echo(f'tag "{tag}" does not exist for {dtype}.{name}')
            return 0
        except UnsavedChangesInThawedError as e:
            click.secho(
                'freeze your current changes or use "force" to dispose of '
                'any unsaved changes in current thawed dataset',fg='red'
            )
            for status,files in {'Changed':e.changed,'New':e.new,'Deleted':e.deleted}.items(): 
                for f in files:
                    click.secho(f"    {status}: {f}",fg='yellow')
            return 0

    # Warn the user if they are in a directory (cwd) that was deleted
    # in the thaw -- theres nothing we can do about this ...
    if str(cwd).startswith(str(dataset.m80.thawed_dir)):
        click.echo(
            'Looks like you are currently in a directory that was just thawed, '
            'update your current working directory with, e.g.:\n'
            '$ cd `pwd`\n'
            f'$ cd {cwd}'
        )
        
cli.add_command(thaw)

# ----------------------------
#    Cloud Commands
# ----------------------------
@click.group()
def cloud():
    """
    Manage your frozen minus80 datasets in the cloud (minus80.linkage.io).
    """
cli.add_command(cloud)

@click.command()
@click.option('--username',default=None)
@click.option('--password',default=None)
@click.option('--force',is_flag=True,default=False)
@click.option('--reset-password',is_flag=True,default=False)
def login(username,password,force,reset_password):
    """
        Log into your cloud account at minus80.linkage.io
    """
    cloud = m80.CloudData() 
    if force:
        try:
            os.remove(cloud._token_file)
        except FileNotFoundError:
            pass
    try:
        # See if currently logged in
        cloud.user
    except UserNotLoggedInError:
        if username is None:
            username = click.prompt('Username (email)',type=str)
        if password is None:
            password = click.prompt('Password', hide_input=True, type=str)
        try:
            cloud.login(username,password)
        except HTTPError as e:
            error_code = json.loads(e.args[1])['error']['message']
            if error_code == 'INVALID_EMAIL':
                click.secho('Error logging in. Invalid email address!.',fg='red')
            elif error_code == 'INVALID_PASSWORD':
                click.secho('Error logging in. Incorrect Password!',fg='red')
            else:
                click.secho(f'Error logging in. {error_code}',fg='red')
            return 0
    account_info = cloud.auth.get_account_info(cloud.user['idToken'])
    # double check that the user is verified
    if account_info['users'][0]['emailVerified'] == False:
        # make sure they have email verified
        click.secho("Your email has not been verified!")
        if click.confirm('Do you want to resend the verification email?'):
            cloud.auth.send_email_verification(cloud._user['idToken'])
        click.secho("Please follow the link sent to your email address, then re-run this command")
        return 0
    click.secho('Successfully logged in',bg='green')

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
    cloud = m80.CloudData()
    try:
        cloud.user
    except UserNotLoggedInError as e:
        click.secho("Please log in to use this feature")
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

cloud.add_command(login)
cloud.add_command(list)
cloud.add_command(push)
cloud.add_command(pull)
cloud.add_command(remove)



@click.command(help='Additional information information')
def version():
    print(f'Version: {m80.__version__}')
    print(f'Installation Path: {m80.__file__}')

cli.add_command(version)
