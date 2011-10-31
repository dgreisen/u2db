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

import errno
import os
import subprocess
import sys
import time

from u1db import tests


def safe_close(process, timeout=0.1):
    """Shutdown the process in the nicest fashion you can manage.

    :param process: A subprocess.Popen object.
    :param timeout: We'll try to send 'SIGTERM' but if the process is alive
        longer that 'timeout', we'll send SIGKILL.
    """
    if process.poll() is not None:
        return
    try:
        process.terminate()
    except OSError, e:
        if e.errno in (errno.ESRCH,):
            # Process has exited
            return
    tend = time.time() + timeout
    while time.time() < tend:
        if process.poll() is not None:
            return
        time.sleep(0.01)
    try:
        process.kill()
    except OSError, e:
        if e.errno in (errno.ESRCH,):
            # Process has exited
            return
    process.wait()


class TestU1DBServe(tests.TestCase):

    def _get_u1db_serve_path(self):
        from u1db import __path__ as u1db_path
        u1db_parent_dir = os.path.dirname(u1db_path[0])
        return os.path.join(u1db_parent_dir, 'u1db-serve')

    def startU1DBServe(self, args):
        command = [sys.executable, self._get_u1db_serve_path()]
        command.extend(args)
        p = subprocess.Popen(command, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(safe_close, p)
        return p

    def test_shows_help(self):
        p = self.startU1DBServe(['--help'])
        stdout, stderr = p.communicate()
        self.assertEqual('', stderr)
        self.assertEqual(0, p.returncode)
        self.assertIn('Run the U1DB server', stdout)
