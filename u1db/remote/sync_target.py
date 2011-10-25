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

""""""

import urlparse

from u1db import (
    SyncTarget,
    )


class RemoteSyncTarget(SyncTarget):
    """Implement the SyncTarget api to a remote server."""

    @staticmethod
    def connect(url):
        return RemoteSyncTarget(url)

    def __init__(self, url):
        self._url = urlparse.urlsplit(url)
        self._conn = None

    def _ensure_connection(self):
        pass
