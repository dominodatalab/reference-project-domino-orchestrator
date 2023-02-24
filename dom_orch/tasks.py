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
from .helpers import get_hardware_tier_id, get_local_timezone
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
            Task name (unique id).
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

        # By default all tasks are attempted only once
        self.max_retries = 0
        self.retries = 0

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

        Parameters
        ----------
        status : {STAT_UNSUBMITTED, STAT_SUCCEEDED, STAT_INPROGRESS, STAT_FAILED}
              Status of the current task.
        """
        assert status in self.VALID_STATES, "%s status is an Invalid state" % status
        self._status = status

    def is_complete(self):
        """Checks if the task has completed successfully

        Returns
        -------
        True : if the task's internal status is STAT_SUCCEEDED
        """
        return self.status() == self.STAT_SUCCEEDED


class DominoSchedRun(DominoTask):
    """Handles Scheduled Jobs.

    This class implements a task for scheduling Domino jobs. 
    Schedule Jobs are used to set up the execution of a script in advance, and set them to execute on a regular cadence. 
    These can be useful when you have a data source that is updated regularly.

    Parameters
    ----------
    task_id : str
            Task name (unique id).
    command : str
            The command to execution as an array of strings where members of the array represent arguments of the command. For example: ["main.py", "hi mom"].
    cron_string : str
            crontab representation of the execution schedule

            # ┌───────────── minute (0 - 59)
            # │ ┌───────────── hour (0 - 23)
            # │ │ ┌───────────── day of the month (1 - 31)
            # │ │ │ ┌───────────── month (1 - 12)
            # │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
            # │ │ │ │ │                                   7 is also Sunday on some systems)
            # │ │ │ │ │
            # │ │ │ │ │
            # * * * * * <command to execute>
    title : str, default=task_id
            A title for the execution.
    tier : str
            The hardware tier to use for the execution. This is the human-readable name of the hardware tier, such as "Free", "Small", or "Medium". 
            If not provided, the project's default tier is used.
    environment_id : str
            The environment ID with which to launch the job. If not provided, the project's default environment is used.

    See Also
    --------
    DominoTask : The base class for all orchestrator tasks

    The `Scheduled Jobs <https://docs.dominodatalab.com/en/latest/user_guide/5dce1f/scheduled-jobs/>`_ section in the Domino Documentation.
    """

    def __init__(self, task_id, command, cron_string, title=None, tier=None, environment_id=None):
        super(self.__class__, self).__init__(task_id)

        self.log = logging.getLogger(__name__)

        # the API expects a string here (scheduled jobs are always direct jobs)
        self.command = command[0]
        self.tier = tier
        self.cron_string = cron_string
        self.environment_id = environment_id

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
        # This is a blocking call - no need to poll for status.
        return self._status

    def submit(self):
        response_json = None

        self.log.info(
            "-- Submitting scheduled job {0} --".format(self.task_id))
        self.log.info("Title       : {}".format(self.title))
        self.log.info("Command     : {}".format(self.command))
        if self.tier:
            self.log.info("Tier        : {}".format(self.tier))
        if self.environment_id:
            self.log.info("Environment : {}".format(self.environment_id))
        self.log.info("Cron string : {}".format(self.cron_string))

        tier_id = get_hardware_tier_id(self.tier)
        project_id = self.domino_api.project_id

        # Get the scheduling user's id
        username = self.domino_api._routes._owner_username
        user_id = self.domino_api.get_user_id(username)

        # We need the local TZ for scheduling
        local_tz = get_local_timezone()

        request = {
            "title": self.title,
            "command": self.command,
            "schedule": {
                "cronString": self.cron_string,
                "isCustom": True
            },
            "hardwareTierIdentifier": tier_id,
            # Always use the active revision of the CE
            "environmentRevisionSpec": "ActiveRevision",
            "notifyOnCompleteEmailAddresses": [],
            "isPaused": False,
            "timezoneId": local_tz,  # "Europe/London",
            "publishAfterCompleted": False,
            "allowConcurrentExecution": False,
            "scheduledByUserId": user_id,
            "overrideEnvironmentId": self.environment_id
        }

        try:
            url = self.domino_api._routes.host + \
                "/v4/projects/" + project_id + "/scheduledjobs"
            response_json = self.domino_api.request_manager.post(
                url, json=request).json()
            job_id = response_json["id"]
            self.log.info("Submission of scheduled job {} succeeded. Job id is {}".format(
                self.task_id, job_id))
            self.set_status(self.STAT_SUCCEEDED)
        except:
            self.set_status(self.STAT_FAILED)
            self.log.error(
                "Submission of scheduled job {} failed.".format(self.task_id))

        return response_json


class DominoRun(DominoTask):
    """Handles Job executions.

    This class implements a task for running Domino jobs. 
    Jobs are a type of execution where an executor machine is assigned to execute a specified command in its OS shell. 
    You can use Jobs to run Python, R, or Bash scripts

    Parameters
    ----------
    task_id : str
            Task name (unique id).
    command : str
            The command to execution as an array of strings where members of the array represent arguments of the command. For example: ["main.py", "hi mom"].
    is_Direct : bool, default=False
            Whether this command should be passed directly to a shell
    mas_retries : int, default=0
            Number of maximum retries if the original run fails. Once max_retries is reached, the entire task is placed in a failed state.
    tier : str
            The hardware tier to use for the execution. This is the human-readable name of the hardware tier, such as "Free", "Small", or "Medium". 
            If not provided, the project's default tier is used.

    See Also
    --------
    DominoTask : The base class for all orchestrator tasks

    The `Jobs <https://docs.dominodatalab.com/en/latest/user_guide/942549/jobs/>`_ section in the Domino Documentation.
    """

    def __init__(self, task_id, command, isDirect=False, max_retries=0, tier=None, title=None):
        super(self.__class__, self).__init__(task_id)

        self.log = logging.getLogger(__name__)

        self.command = command
        self.isDirect = isDirect
        self.max_retries = max_retries
        self.tier = tier
        self.title = title
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
        response_json = None

        self.log.info("-- Submitting run {0} --".format(self.task_id))
        self.log.info("Direct task   : {}".format(self.isDirect))
        self.log.info("Command       : {}".format(self.command))

        try:
            if self.tier:
                # Catch errors (e.g. invalid hw tier etc.)
                response_json = self.domino_api.runs_start(
                    self.command, isDirect=self.isDirect, tier=self.tier, title=self.title)
                self.log.info("Tier override : {}".format(self.tier))
            else:
                response_json = self.domino_api.runs_start(
                    self.command, isDirect=self.isDirect)
            self.run_id = response_json["runId"]
            self._status = DominoTask.STAT_INPROGRESS
        except Exception as e:
            self._status = DominoTask.STAT_FAILED
            self.log.error(
                "Submission of task {} failed.".format(self.task_id))
            self.log.exception(e)

        return response_json


class DominoModel(DominoTask):
    """Handles Model API deployments.

    This class implements a task for building and deploying Domino APIs. 
    Domino Model APIs are REST API endpoints that run your Domino code. 
    These endpoints are automatically served and scaled by Domino to provide programmatic access to your R and Python data science code. 

    Parameters
    ----------
    task_id : str
            Task name (unique id).
    file_name : str
            Path to the file containing the model code.
    function_name : str
            The function to be called when the model handles a request
    model_name: str
            Name for the model (this is the name showed in the Model APIs view of the Domino UI)
    description : str, default=""
            Description of the model or summary of the changes in the new version.
    model_id : str, default=None
            If set, triggers the deployment of a new version of an existing Model API. The existing model is identified using the model_id value.
            If not set, builds and deploys a brand new Model API.
    environment_id : str, default=None
            The unique id of the environment for the model to use. 
            TODO: If no environment id is provided it defaults to the first globally available environment. This needs to be fixed. 
                    It is better to fall back to the default project environment instead.

    See Also
    --------
    DominoTask : The base class for all orchestrator tasks

    The `Model APIs <https://docs.dominodatalab.com/en/latest/user_guide/8dbc91/model-apis/>`_ section in the Domino Documentation.
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
        """Fetches all globally available compute environments.

        Returns
        -------
        list of str : List of all globally available compute environments
        """
        all_available_environments = self.domino_api.environments_list()
        global_environments = list(
            filter(
                lambda x: x.get(
                    "visibility") == "Global", all_available_environments["data"]
            )
        )

        return global_environments

    def get_versions(self):
        """Gets all versions of a specific model. The id of the queried model is fetched from self.model_id.

        Returns
        -------
        list of str : List of ids (most recent first) of all model versions for a specific Model API
        """
        assert self.model_id is not None, "Trying to get Model API versions but model_id is None. This should not happen."

        url = self.domino_api._routes._build_models_url() + "/" + \
            self.model_id + "/versions"
        response_json = self.domino_api.request_manager.get(url).json()
        return response_json.get("data", {})

    def submit(self):
        response_json = None

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
            # no model_id, this is a new model deploy
            response_json = self.domino_api.model_publish(file=self.file_name, function=self.function_name, environment_id=self.environment_id,
                                                          name=self.model_name, description=self.description)
            # Set the model_id and version_id
            self.model_id = response_json.get("data", {}).get("_id")

        versions = self.get_versions()
        self.version_id = versions[0].get("_id")

        self.log.info("Created a model with model_id {0} and model_version {1}".format(
            self.model_id, self.version_id))

        self._status = DominoTask.STAT_INPROGRESS
        self.log.info(20*"-")

        return response_json


class DominoApp(DominoTask):
    """Handles Domino App deployments.

    This class implements a task for building and deploying a Domino App. 
    Domino Apps host web applications and dashboards with the same elastic infrastructure that powers Jobs and Workspace sessions.
    Typical Apps are built with frameworks like Flask, Shiny, and Dash.

    Note, that Domino allows for only one Domino App per project. If the project that the task operates on already has an App up and running
    the task will automatically unpublish it before replacing it with the new App.

    Parameters
    ----------
    task_id : str
            Task name (unique id).
    app_name : str
            Application name.
    tier : str
            The hardware tier to use for the deployed application. This is the human-readable name of the hardware tier, such as "Free", "Small", or "Medium". 
            If not provided, the project's default tier is used.

    See Also
    --------
    DominoTask : The base class for all orchestrator tasks

    The `Domino Apps <https://docs.dominodatalab.com/en/latest/user_guide/8b094b/domino-apps/>`_ section in the Domino Documentation.
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
        response_json = None

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
                "Successfully created application with app_id: {}".format(app_id))
        else:
            raise RuntimeError(
                "Cannot create application. task_id {}".format(self.task_id))

        return app_id
