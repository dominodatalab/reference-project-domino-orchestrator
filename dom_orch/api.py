# Author: Nikolay Manchev <nikolay.manchev@dominodatalab.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import os

from domino import Domino


class DominoAPISession(object):
    """Singleton that returns a connection to the target Domino instance.

    Note that environment variables like DOMINO_PROJECT_OWNER are automatically injected in 
    the session if the script is running in Domino. Otherwise, they need to be provided
    externally.

    Usage
    -----
    >>> from dom_orch.api import DominoAPISession
    >>> api = DominoAPISession.instance()
    >>> api.runs_start(...)   # standard Domino API call
    """

    _domino_api = None

    def __init__(self):
        raise RuntimeError("Call instance() instead")

    @classmethod
    def instance(cls):

        if not cls._domino_api:

            DOMINO_USER_API_KEY = os.environ["DOMINO_USER_API_KEY"]
            DOMINO_PROJECT_NAME = os.environ["DOMINO_PROJECT_NAME"]
            DOMINO_PROJECT_OWNER = os.environ["DOMINO_PROJECT_OWNER"]

            cls._domino_api = Domino(
                project=DOMINO_PROJECT_OWNER + "/" + DOMINO_PROJECT_NAME)
            cls._domino_api.authenticate(api_key=DOMINO_USER_API_KEY)

        return cls._domino_api
