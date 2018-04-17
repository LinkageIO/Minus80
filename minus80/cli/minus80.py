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
    click.echo(f'Debug mode is {debug}')

@cli.command(help='List the available minus80 datasets')
def available():
    m80.Tools.available()



#class minus80CLI(object):
#
#    def __init__(self):
#        parser = argparse.ArgumentParser(
#            description=(
#            ),
#            formatter_class=argparse.RawDescriptionHelpFormatter,
#            epilog= "\n".join([
#                'version: {}'.format(m80.__version__),
#                'src:{}'.format(m80.__file__),
#            ])
#        )
#        subparsers = parser.add_subparsers(help='sub-command help', dest='subparser_name')
#        parser_a = subparsers.add_parser('command_a', help='command_a help')
#
#        parser.add_argument('extra', nargs='*', help='Other commands')
#        args = parser.parse_args()
#        extra_namespaces = self.parse_extra(parser,args)
#        if not hasattr(self, args.command):
#            print(f'Unrecognized command.')
#            parser.print_help()
#            exit(1)
#        getattr(self, args.command)()
#
#
#    def parse_extra (parser, namespace):
#        namespaces = []
#        extra = namespace.extra
#        while extra:
#            n = parser.parse_args(extra)
#            extra = n.extra
#            namespaces.append(n)
#        return namespaces
#
#
#
#
if __name__ == '__main__':
    minus80CLI()
