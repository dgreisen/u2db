# Copyright 2011 Canonical Ltd.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 3, as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Command infrastructure for u1db"""

import argparse


class CommandGroup(object):
    """A collection of commands."""

    def __init__(self, description=None):
        self.commands = {}
        self.description = description

    def register(self, cmd):
        """Register a new command to be incorporated with this group."""
        self.commands[cmd.name] = cmd

    def make_argparser(self):
        """Create an argparse.ArgumentParser"""
        parser = argparse.ArgumentParser(description=self.description)
        subs = parser.add_subparsers(title='commands')
        for name, cmd in sorted(self.commands.iteritems()):
            sub = subs.add_parser(name, help=cmd.__doc__)
            sub.set_defaults(subcommand=cmd)
            cmd._populate_subparser(sub)
        return parser

    def run_argv(self, argv, stdin, stdout, stderr):
        """Run a command, from a sys.argv[1:] style input."""
        parser = self.make_argparser()
        args = parser.parse_args(argv)
        cmd = args.subcommand(stdin, stdout, stderr, args)
        cmd.run()


class Command(object):
    """Definition of a Command that can be run.

    :cvar name: The name of the command, so that you can run
        'u1db-client <name>'.
    """

    name = None
    _known_commands = {}

    def __init__(self, in_file, out_file, err_file, args):
        self.in_file = in_file
        self.out_file = out_file
        self.err_file = err_file
        self.args = args

    @classmethod
    def run_with_args(cls, stdin, stdout, stderr, **kwargs):
        args = argparse.Namespace(**kwargs)
        cmd = cls(stdin, stdout, stderr, args)
        return cmd.run()

    @classmethod
    def _populate_subparser(cls, parser):
        """Child classes should override this to provide their arguments."""
        raise NotImplementedError(cls._populate_subparser)

    def run(self):
        """This is where the magic happens.

        Subclasses should implement this, requesting their specific arguments.
        """
        raise NotImplementedError(run)



