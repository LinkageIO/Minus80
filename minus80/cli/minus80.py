#!/usr/bin/env python3

import os
import sys
import json
import click
import random
import asyncio
import minus80.Tools

import minus80 as m80

from pathlib import Path
from datetime import datetime

from minus80.Exceptions import (
        TagInvalidError,
        TagConflictError,
        FreezableNameInvalidError,
        TagExistsError,
        TagDoesNotExistError,
        UserNotLoggedInError,
        UserNotVerifiedError,
        UnsavedChangesInThawedError,
        CloudDatasetDoesNotExistError,
        CloudTagDoesNotExistError,
        CloudPullFailedError
)
from requests.exceptions import HTTPError


class NaturalOrderGroup(click.Group):
    '''
        This subclass orders the commands in the @click.group
        in the order in which they are defined in the script
        **Ref** https://github.com/pallets/click/issues/513
    '''
    def list_commands(self, ctx): #pragma: no cover
        return self.commands.keys()

cute_emojis = [ "⛷"  , "❄️"  , "🏔"  , "🏂" , "☃️"  ]

@click.group(
    cls=NaturalOrderGroup,
    epilog=f"Made with {random.choice(cute_emojis)}  in Denver, Colorado"
)
@click.option('--debug/--no-debug', default=False)
def cli(debug): #pragma: no cover
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

    if debug: 
        click.echo("Debug mode is on")
        import sys
        from IPython.core import ultratb
        sys.excepthook = ultratb.FormattedTB(mode='Verbose',
            color_scheme='Linux', call_pdb=1)
        import logging
        log = logging.getLogger('minus80')
        log.setLevel(logging.DEBUG)

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
    help=(
        'If specified, the minus80 project directory for NAME will be created here, '
        'otherwise, it will be created in the current directory'
    )
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
        sys.exit(1)
    sys.exit(0)

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
    sys.exit(0)

cli.add_command(list)

# ----------------------------
#    delete Commands
# ----------------------------
@click.command(help="Delete a minus80 dataset")
@click.argument("slug", metavar="<slug>")
@click.option("--force",is_flag=True,default=False,help='Force delete without confirmation prompt',)
def delete(slug,force):
    # Validate the input
    if force is False: #pragma: no cover
        click.confirm(f'Are you sure you want to delete "{slug}"')
    try:
        dtype,name,tag = minus80.Tools.parse_slug(slug) 
        if tag is not None:
            raise TagInvalidError()
    except TagInvalidError as e:
        click.echo(f'Cannot delete tags, only entire datasets.')
        sys.exit(1)
    except FreezableNameInvalidError:
        click.echo(
            f'Please provide a valid dataset name: <dtype>.<name>. E.g. Project.foobar'
            f'Note: do not include tags'
        )
        sys.exit(1)
    # Make sure that the dataset is available
    if not minus80.Tools.available(dtype,name):
        click.echo(
            f'"{dtype}.{name}" not in minus80 datasets! '
            'check available datasets with the list command'
        )
        sys.exit(1)
    else:
        minus80.Tools.delete(dtype, name)
        sys.exit(0)


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
        sys.exit(1)
    # Make sure that the dataset is available
    if not minus80.Tools.available(dtype,name):
        click.echo(
            f'"{dtype}.{name}" not in minus80 datasets! '
            'check available datasets with the list command'
        )
        sys.exit(1)
    else:
        # Create the minus80 
        try:
            dataset = getattr(minus80,dtype)(name)
        except Exception as e: #pragma: no cover
            click.echo(f'Could not build {dtype}.{name}')
            raise e
            sys.exit(1)
        # Freeze with tag
        try:
            dataset.m80.freeze(tag)
            click.echo(click.style("SUCCESS!",fg="green",bold=True))
            sys.exit(0)
        except TagExistsError:
            click.echo(f'Tag "{tag}" already exists in the cloud for {dtype}.{name}')
            sys.exit(1)

cli.add_command(freeze)

@click.command(help='Thaw a minus80 dataset')
@click.argument("slug",metavar="<slug>")
@click.option("--force",is_flag=True,default=False,help='forces a thaw, even if there are unsaved changes',)
def thaw(slug,force):
    try:
        cwd = Path.cwd().resolve()
    except FileNotFoundError as e: #pragma: no cover
        cwd = '/' 
    try:
        dtype,name,tag = minus80.Tools.parse_slug(slug) 
        if tag is None:
            raise TagInvalidError()
    except (TagInvalidError, FreezableNameInvalidError):
        click.echo(
            f'Please provide a valid tag in "{slug}"'
        )
        sys.exit(1)
    # Make sure that the dataset is available
    if not minus80.Tools.available(dtype,name):
        click.echo(
            f'"{dtype}.{name}" not in minus80 datasets! '
            'check available datasets with the list command'
        )
        sys.exit(1)
    else:
        # Create the minus80 
        try:
            dataset = getattr(minus80,dtype)(name)
        except Exception as e: #pragma: no cover
            click.echo(f'Could not build {dtype}.{name}')
        # Freeze with tag
        try:
            dataset.m80.thaw(tag,force=force)
            click.echo(click.style("SUCCESS!",fg="green",bold=True))
            sys.exit(0)
        except TagDoesNotExistError:
            click.echo(f'tag "{tag}" does not exist for {dtype}.{name}')
            sys.exit(1)
        except UnsavedChangesInThawedError as e:
            click.secho(
                'freeze your current changes or use "force" to dispose of '
                'any unsaved changes in current thawed dataset',fg='red'
            )
            for status,files in {'Changed':e.changed,'New':e.new,'Deleted':e.deleted}.items(): 
                for f in files:
                    click.secho(f"    {status}: {f}",fg='yellow')
            sys.exit(1)

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
@click.group(
    cls=NaturalOrderGroup,
)
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
    # check to see if we are doing a reset
    if reset_password == True:
        if username is None: 
            username = click.prompt('Username (email)',type=str)
        cloud.auth.send_password_reset_email(username)
        click.secho("Check your email to reset your password",fg='green')
        sys.exit(0)
    elif force:
        try:
            os.remove(cloud._token_file)
        except FileNotFoundError:
            pass
    try:
        # See if currently logged in
        cloud.user
    except HTTPError as e:
        error_code = json.loads(e.args[1])['error']['message']
        if error_code == 'TOKEN_EXPIRED':
            click.secho('Current Session is EXPIRED. Use the --force option to re-login!',fg='red')
        else:
            click.secho('An error occurred trying to sign in. Try again shortly or use the --force option.',fg='red')
        sys.exit(1)
    except UserNotLoggedInError:
        if username is None: 
            if 'MINUS80_USERNAME' in os.environ:
                username = os.environ['MINUS80_USERNAME']
            else:
                username = click.prompt('Username (email)',type=str)
        if password is None:
            if 'MINUS80_PASSWORD' in os.environ:
                username = os.environ['MINUS80_PASSWORD']
            else:
                password = click.prompt('Password', hide_input=True, type=str)
        try:
            cloud.login(username,password)
        except HTTPError as e:
            error_code = json.loads(e.args[1])['error']['message']
            if error_code == 'INVALID_EMAIL':
                click.secho('Error logging in. Invalid email address!.',fg='red')
                #click.secho('Sign up for an account at https://minus80.linkage.io')
            elif error_code == 'INVALID_PASSWORD':
                click.secho('Error logging in. Incorrect Password!',fg='red')
            else:
                click.secho(f'Error logging in. {error_code}',fg='red')
            sys.exit(1)

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
@click.option(
    '--tags',
    default=False,
    is_flag=True,
    help=('List the available tags for the specified dataset')
)
def list(dtype, name, tags=False):
    """
        List available datasets
    """
    cloud = m80.CloudData()
    try:
        cloud.user
    except UserNotLoggedInError as e:
        click.secho("Please log in to use this feature")
        sys.exit(1)
    except HTTPError as e:
        click.secho(
            "An error occurred trying to login, please re-login using "
            "the command:\n\t$ minus80 cloud login --force \n",fg='red'
        )
        sys.exit(1)

    # Make the list call
    datasets = asyncio.run(cloud.list())
    if len(datasets) == 0:
        click.echo("Nothing here yet!")
    for avail_dtype,avail_names in datasets.items():
        click.echo(f'{avail_dtype}:') 
        if dtype and avail_dtype != dtype:
            continue
        for avail_name in avail_names.keys():
            if name and avail_name != name:
                continue
            click.echo(f' └──{avail_name}')
            if tags:
                for avail_tag,metadata in avail_names[avail_name].items():
                    csum = metadata['checksum'][0:10]
                    timestamp = datetime.fromtimestamp(metadata['created']).strftime('%I:%M%p - %b %d, %Y')
                    click.echo(f'   └──{avail_tag} {csum} ({timestamp})')


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
            # run the push method in an event loop
            asyncio.run(cloud.push(dtype, name, tag))
        except TagDoesNotExistError as e:
            click.echo(f'tag "{tag}" does not exist for {dtype}.{name}')
        except TagExistsError as e:
            click.echo(f'Cannot push {dtype}.{name}:{tag} to the cloud.')
            click.echo(f'The tag already exists there.')
        except TagConflictError as e:
            click.echo(f'Cannot push {dtype}.{name}:{tag} to the cloud.')
            click.echo(f'The tag already exists there.')
            click.secho(
                'Warning! The contents of the local tag and the cloud tag differ '
                'Create a tag with a unique name and retry pushing',
                fg='red'
            )

@click.command()
@click.argument("slug", metavar="<slug>")
def pull(slug):
    """
    \b
    Pull a frozen minus80 dataset from the cloud.

    \b
    Positional Arguments:
    <slug> - The slug of the frozen minus80 dataset (e.g. Project.foo:v1)
    """
    cloud = m80.CloudData()
    try:
        cloud.user
    except UserNotLoggedInError as e:
        click.secho("Please log in to use this feature")
        return 1
    try:
        dtype,name,tag = minus80.Tools.parse_slug(slug) 
        if tag is None:
            raise TagInvalidError()
    except (TagInvalidError, FreezableNameInvalidError):
        click.echo(
            f'Please provide a valid tag in "{slug}"'
        )
        return 1
    # Pull the files and tag from the cloud
    try:
        # run the push method in an event loop
        asyncio.run(cloud.pull(dtype, name, tag))
    except TagExistsError as e:
        click.echo(
            f'The tag ({tag}) already exists from {dtype}.{name}'        
        )
        return 1
    except CloudDatasetDoesNotExistError as e:
        click.echo(
            f'The dataset "{dtype}.{name}" does not exist in the cloud'
        )
        return 1
    except CloudTagDoesNotExistError as e:
        click.echo(
            f'The tag "{tag}" does not exist in the cloud'
        )
        return 1
    except CloudPullFailedError as e:
        click.echo(f'Failed to pull all data for tag "{tag}". ')
        click.echo(f'This could be network issues, please try again later ')
        click.echo(f'or report error to https://github.com/LinkageIO/minus80/issues ')

        return 1

    # Let the user know
    click.echo(f'{dtype}.{name}:{tag} successfully pulled')


@click.command()
@click.argument("dtype", metavar="<dtype>")
@click.argument("name", metavar="<name>")
@click.option("--raw", is_flag=True, default=False, help="Flag to list raw data")
def remove(dtype, name, raw):
    """
    Delete a minus80 tag from the cloud.
    """
    pass

cloud.add_command(login)
cloud.add_command(list)
cloud.add_command(push)
cloud.add_command(pull)



@click.command(help='Additional information information')
def version():
    print(f'Version: {m80.__version__}')
    print(f'Installation Path: {m80.__file__}')

cli.add_command(version)
