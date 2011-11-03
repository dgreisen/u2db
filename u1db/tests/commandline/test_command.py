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

import cStringIO
import argparse

from u1db import (
    tests,
    )
from u1db.commandline import (
    command,
    )


class MyTestCommand(command.Command):
    """Help String"""

    name = 'mycmd'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('foo')
        parser.add_argument('--bar', type=int)

    def run(self):
        self.out_file.write('args: %s\n' % (self.args,))


def make_stdin_out_err():
    return cStringIO.StringIO(), cStringIO.StringIO(), cStringIO.StringIO()


class TestCommandGroup(tests.TestCase):

    def trap_system_exit(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SystemExit, e:
            self.fail('Got SystemExit trying to run: %s' % (func,))

    def parse_args(self, parser, args):
        return self.trap_system_exit(parser.parse_args, args)

    def test_register(self):
        group = command.CommandGroup()
        self.assertEqual({}, group.commands)
        group.register(MyTestCommand)
        self.assertEqual({'mycmd': MyTestCommand},
                         group.commands)

    def test_make_argparser(self):
        group = command.CommandGroup(description='test-foo')
        parser = group.make_argparser()
        self.assertIsInstance(parser, argparse.ArgumentParser)

    def test_make_argparser_with_command(self):
        group = command.CommandGroup(description='test-foo')
        group.register(MyTestCommand)
        parser = group.make_argparser()
        args = self.parse_args(parser, ['mycmd', 'foozizle', '--bar=10'])
        self.assertEqual('foozizle', args.foo)
        self.assertEqual(10, args.bar)
        self.assertEqual(MyTestCommand, args.subcommand)

    def test_run_argv(self):
        group = command.CommandGroup()
        group.register(MyTestCommand)
        stdin, stdout, stderr = make_stdin_out_err()
        self.trap_system_exit(group.run_argv,
            ['mycmd', 'foozizle', '--bar=10'],
            stdin, stdout, stderr)


class TestCommand(tests.TestCase):

    def make_command(self):
        stdin, stdout, stderr = make_stdin_out_err()
        return command.Command(stdin, stdout, stderr, None)

    def test__init__(self):
        cmd = self.make_command()
        self.assertIsNot(None, cmd.in_file)
        self.assertIsNot(None, cmd.out_file)
        self.assertIsNot(None, cmd.err_file)

    def test_run_with_args(self):
        stdin, stdout, stderr = make_stdin_out_err()
        res = MyTestCommand.run_with_args(stdin=stdin, stdout=stdout,
                                          stderr=stderr, foo='foozizle', bar=10)
