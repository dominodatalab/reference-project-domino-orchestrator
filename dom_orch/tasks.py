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

import logging
import time

from .api import DominoAPISession
from .utils import get_hardware_tier_id
from abc import abstractmethod
from abc import ABCMeta




class DominoTask(metaclass=ABCMeta):
    """This is the base class for all tasks run by the orchestrator. 

    It translates the API states to four internal states - un-submitted, in progress, succeeded, and failed.
    It is also responsible for fetching the task status from the target Domino instance and for submitting
    the task for execution.

    Parameters
    ----------
    task_id : str
            Task name.
    """
    STAT_UNSUBMITTED = "Unsubmitted"
    STAT_SUCCEEDED = "Succeeded"
    STAT_INPROGRESS = "In-progress"
    STAT_FAILED = "Failed"

    VALID_STATES = [STAT_UNSUBMITTED,
                    STAT_SUCCEEDED, STAT_INPROGRESS, STAT_FAILED]

    def __init__(self, task_id):
        self.task_id = task_id
        self._status = self.STAT_UNSUBMITTED
        self.domino_api = DominoAPISession.instance()

    @abstractmethod
    def status(self):
        """Fetches the status of the specific task from the target Domino instance.
        """
        return

    @abstractmethod
    def submit(self):
        """Submits the task for execution on the target Domino instance.
        """
        return

    def set_status(self, status):
        """Sets the internal status of the task.
        """
        assert status in self.VALID_STATES, "%s status is an Invalid state" % status
        self._status = status

    def is_complete(self):
        """Checks the task status and returns true if it is STAT_SUCCEEDED
        """
        return self.status() == self.STAT_SUCCEEDED


class DominoSchedRun(DominoTask):

    def __init__(self, task_id, command, cron_string, title=None, tier=None):
        super(self.__class__, self).__init__(task_id)

        self.log = logging.getLogger(__name__)

        self.command = command
        self.tier = tier
        self.cron_string = cron_string

        # If no name is given for the scheduled job use the task_id instead
        if title:
            self.title = title
        else:
            self.title = task_id

    def set_status(self, status):
        """Sets the internal status of the task.
        """
        assert status in self.VALID_STATES, "%s status is an Invalid state" % status
        self._status = status

    def is_complete(self):
        """Checks the task status and returns true if it is STAT_SUCCEEDED
        """
        return self.status() == self.STAT_SUCCEEDED

    def status(self):
        return self._status

    def submit(self):   
        self.log.info("-- Submitting scheduled job {0} --".format(self.task_id))
        if self.tier:
            self.log.info("Tier        : {}".format(self.tier))
        #self.log.info("Environment : {}".format(self.environment_id))
        #self.log.info("Function    : {}".format(self.function_name))
        #self.log.info("File        : {}".format(self.file_name))
        #self.log.info("Model name  : {}".format(self.model_name))

        tier_id = get_hardware_tier_id(self.tier)
        print(tier_id)
        request = {
            "title": self.title,
            "command": self.command,
            "schedule": {
                "cronString": self.cron_string,
                "isCustom": True
            },
            "hardwareTierIdentifier": tier_id,
            "environmentRevisionSpec":"ActiveRevision",
            "notifyOnCompleteEmailAddresses":[],
            "isPaused":False,
            "timezoneId":"Europe/London",
            "publishAfterCompleted":False,
            "allowConcurrentExecution":False,
            "scheduledByUserId": "6141ccfd0f08e1652cfad376",
        }

        input("GGG")

class DominoRun(DominoTask):
    """
    self.command        # command to submit to API
    self.isDirect       # isDirect flag to submit to API
    self.max_retries    # maximum retries

    self.run_id         # ID of latest run attempt
    self.retries        # number of retries so far


    once submitted, it polls status, and retries (submits re-runs) up to max_retries
    """

    def __init__(self, task_id, command, isDirect=False, max_retries=0, tier=None):
        super(self.__class__, self).__init__(task_id)

        self.log = logging.getLogger(__name__)

        self.command = command
        self.isDirect = isDirect
        self.max_retries = max_retries
        self.tier = tier
        self.run_id = None
        self.retries = 0

    def status(self):
        if self.run_id:
            # Task has been submitted
            # Update status
            api_status = self.domino_api.runs_status(
                self.run_id)["status"].lower()  # needs error handling?
            if api_status == "succeeded":
                self.set_status(self.STAT_SUCCEEDED)
            elif api_status in ("error", "failed"):
                self.set_status(self.STAT_FAILED)
            elif api_status in ("preparing", "running", "pending", "finishing"):
                self.set_status(self.STAT_INPROGRESS)
            else:
                raise RuntimeError("Unknown DominoRun status:", api_status)

        return self._status

    def submit(self):
        self.log.info("-- Submitting run {0} --".format(self.task_id))
        self.log.info("Direct task   : {}".format(self.isDirect))
        self.log.info("Command       : {}".format(self.command))
        if self.tier:
            # Catch errors (e.g. invalid hw tier etc.)
            response_json = self.domino_api.runs_start(
                self.command, isDirect=self.isDirect, tier=self.tier)
            self.log.info("Tier override : {}".format(self.tier))
        else:
            response_json = self.domino_api.runs_start(
                self.command, isDirect=self.isDirect)

        self.run_id = response_json["runId"]
        self._status = DominoTask.STAT_INPROGRESS
        self.log.info(20*"-")
        return response_json


class DominoModel(DominoTask):
    """
    self.task_id            # name of task
    self.environment_id     # id of compute env used for building the model image
    file_name               # file containing the scoring function
    function_name           # scoring function
    model_name              # model name
    description             # model description
    model_id                # model ID (if updating an existing model)
    """

    def __init__(self, task_id, file_name, function_name, model_name, description="", model_id=None, environment_id=None):
        super(self.__class__, self).__init__(task_id)

        self.log = logging.getLogger(__name__)

        if environment_id == None:
            # No environment provided for the model. Pick the first global environment
            self.environment_id = self.get_global_envs()[0].get("id")
            self.log.warn("No environment provided for model {0}. Automatically selecting the first Global environment: {1}".format(
                self, task_id, self.environment_id))
        else:
            self.environment_id = environment_id

        self.file_name = file_name
        self.function_name = function_name
        self.model_name = model_name
        self.description = description
        self.model_id = model_id
        self.version_id = None

    def status(self):
        if (self._status != DominoTask.STAT_UNSUBMITTED):
            assert self.model_id != None, "This shouldn't happen. Task is marked as submitted but has no model_id?"
            # If the task has been submitted, update its status
            url = self.domino_api._routes._build_models_v4_url() + "/" + self.model_id + \
                "/" + self.version_id + "/getBuildStatus"

            api_status = self.domino_api.request_manager.get(url).json()[
                "status"].lower()
            #print("DominoModel status:", api_status)

            if api_status == "building":
                status = self.STAT_INPROGRESS
            elif api_status == "complete":
                # This is only the build status. If we want to make sure that the model is up and running we need to check
                # /v4/models/model_id/version_id/getModelDeploymentStatus for "status":"running"
                status = self.STAT_SUCCEEDED
            else:
                status = self.STAT_INPROGRESS
            self.set_status(status)

        return self._status

    def get_global_envs(self):
        # Get all globally available environments
        all_available_environments = self.domino_api.environments_list()
        global_environments = list(
            filter(
                lambda x: x.get(
                    "visibility") == "Global", all_available_environments["data"]
            )
        )

        return global_environments

    def get_versions(self):
        url = self.domino_api._routes._build_models_url() + "/" + \
            self.model_id + "/versions"
        response_json = self.domino_api.request_manager.get(url).json()
        return response_json.get("data", {})

    def submit(self):
        self.log.info("-- Submitting model {0} --".format(self.task_id))
        self.log.info("Environment : {}".format(self.environment_id))
        self.log.info("Function    : {}".format(self.function_name))
        self.log.info("File        : {}".format(self.file_name))
        self.log.info("Model name  : {}".format(self.model_name))
        if self.model_id:
            self.log.info("Model ID    : {}".format(self.model_id))

        if self.model_id:
            # model_id provided, we'll have to update instead of deploying a new model
            self.log.info(
                "This is an existing model. We need to build a new version instead of deploying a new model.")

            response_json = self.domino_api.model_version_publish(model_id=self.model_id, file=self.file_name, function=self.function_name,
                                                                  environment_id=self.environment_id, description=self.description)
        else:
            # no model_id, this is a new model deply
            response_json = self.domino_api.model_publish(file=self.file_name, function=self.function_name, environment_id=self.environment_id,
                                                          name=self.model_name, description=self.description)
            # Set the model_id and version_id
            self.model_id = response_json.get("data", {}).get("_id")

        versions = self.get_versions()
        self.version_id = versions[0].get("_id")

        self.log.info("Created a model with model_id {0} and model_version {1}".format(
            self.model_id, self.version_id))
        #print("model_id:", self.model_id)
        #print("version_id:", self.version_id)

        self._status = DominoTask.STAT_INPROGRESS
        self.log.info(20*"-")

        return response_json


class DominoApp(DominoTask):
    """
    self.command        # command to submit to API
    self.isDirect       # isDirect flag to submit to API
    self.max_retries    # maximum retries

    self.run_id         # ID of latest run attempt
    self.retries        # number of retries so far


    once submitted, it polls status, and retries (submits re-runs) up to max_retries
    """

    def __init__(self, task_id, app_name, tier=None):
        super(self.__class__, self).__init__(task_id)

        self.log = logging.getLogger(__name__)

        self.app_name = app_name
        self.tier = tier

    def status(self):
        if (self._status != DominoTask.STAT_UNSUBMITTED):
            assert self.app_id != None, "This shouldn't happen. Task is marked as submitted but has no app_id?"
            # If the task has been submitted, update its status

            url = self.domino_api._routes.app_get(self.app_id)
            response = self.domino_api.request_manager.get(url).json()
            api_status = response.get("status", None).lower()

            #api_status = self.domino_api.request_manager.get(url).json()["status"]
            #log.info("DominoApp status: {}".format(api_status))
            #print("DominoApp status: {}".format(api_status))

            if api_status == "running":
                self.set_status(self.STAT_SUCCEEDED)
            elif api_status in ("error", "failed"):
                self.set_status(self.STAT_FAILED)
            elif api_status in ("preparing", "pending", "finishing"):
                self.set_status(self.STAT_INPROGRESS)
            else:
                raise RuntimeError("Unknown DominoModel status:", api_status)

        return self._status

    def submit(self):
        self.log.info("-- Submitting app {0} --".format(self.task_id))
        self.log.info("Name          : {}".format(self.app_name))
        if self.tier:
            self.log.info("Hardware tier : {}".format(self.tier))

        try:
            self.log.info("Unpublishing running apps...")
            self.domino_api.app_unpublish()
            self.log.info("Creating new app...")
            self.app_id = self._create_app()
            self.log.info(
                "Starting application with app_id: {}".format(self.app_id))
            self._start_app()
            self.set_status(self.STAT_INPROGRESS)

        except Exception as e:
            self.set_status(self.STAT_FAILED)
            self.log.error(e)

        self.log.info(20*"-")

    def _start_app(self):
        url = self.domino_api._routes.app_start(self.app_id)

        if self.tier:
            # HW tier set. We need to get its ID
            hw_tier_id = get_hardware_tier_id(self.tier)
            if not hw_tier_id:
                raise RuntimeError(
                    "Hardware tier ID for tier name {} cannot be fetched. Misspelled tier name?".format(self.tier))

            request = {"hardwareTierId": hw_tier_id}

        else:
            request = {}

        response_json = self.domino_api.request_manager.post(
            url, json=request).json()
        return response_json

    def _create_app(self):
        # project_id = self.domino_api._project_id # check what version uses _project_id and which has a property instead
        project_id = self.domino_api.project_id
        url = self.domino_api._routes.app_create()

        request_payload = {
            "modelProductType": "APP",
            "projectId": project_id,
            "name": self.app_name,
            "owner": "",
            "created": time.time_ns(),
            "lastUpdated": time.time_ns(),
            "status": "",
            "media": [],
            "openUrl": "",
            "tags": [],
            "stats": {"usageCount": 0},
            "appExtension": {"appType": ""},
            "id": "000000000000000000000000",
            "permissionsData": {
                "visibility": "GRANT_BASED",
                "accessRequestStatuses": {},
                "pendingInvitations": [],
                "discoverable": True,
                "appAccessStatus": "ALLOWED",
            }
        }

        response_json = self.domino_api.request_manager.post(
            url, json=request_payload).json()

        key = "id"
        if key in response_json.keys():
            app_id = response_json[key]
            self.log.info(
                "Succesfully created application with app_id: {}".format(app_id))
        else:
            raise RuntimeError(
                "Cannot create application. task_id {}".format(self.task_id))

        return app_id
