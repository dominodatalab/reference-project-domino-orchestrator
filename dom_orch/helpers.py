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

from .api import DominoAPISession
from tzlocal import get_localzone


def get_hardware_tier_id(tier_name):
    """Gets a hardware tier id from its human-readable name

    Parameters
    ----------
    tier_name : str
             The human-readable name of the hardware tier, such as "Free", "Small", or "Medium". 

    Returns
    -------
    hw_tier_id : str
              Domino hardware tier ID (e.g. small-k8s, large-k8s, gpu-small-k8s, etc.)
    """
    hw_tier_id = None

    if tier_name:
        domino_api = DominoAPISession.instance()

        for hardware_tier in domino_api.hardware_tiers_list():
            if tier_name.lower() == hardware_tier["hardwareTier"]["name"].lower():
                hw_tier_id = hardware_tier["hardwareTier"]["id"]

    return hw_tier_id


def get_default_hardware_tier():
    """Gets the default HW tier for the project. The project ID is automatically 
    fetched from the authentication session.

    Returns
    -------
    hw_tier_id : str
              Domino hardware tier ID (e.g. small-k8s, large-k8s, gpu-small-k8s, etc.)
    """
    domino_api = DominoAPISession.instance()
    project_id = domino_api.project_id

    url = domino_api._routes.host + \
                "/v4/projects/" + project_id + "/hardwareTiers"

    result = domino_api.request_manager.get(url).json()
    
    # As a fallback, select the first HW tier available to the project
    default_tier_id = result[0]["hardwareTier"]["id"]

    for tier in result:
        if tier["hardwareTier"]["isDefault"]:
            # Found the default HW tier
            default_tier_id = tier["hardwareTier"]["id"]

    return default_tier_id

def get_local_timezone():
    # Returns the local timezone
    local_tz = get_localzone()
    return str(local_tz)
