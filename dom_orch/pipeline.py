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

import json
import logging
import time
import configparser

from .tasks import DominoApp, DominoModel, DominoRun, DominoSchedRun, DominoTask

class Dag:
    """Dependency graph class.

    Contains the workflow structure, including all tasks and their dependencies.

    Parameters
    ----------
    tasks : list of DominoRun instances
            Task name (unique id)
    dependency_graph : a dictionary of {'task_id_1': [dependencies], 'task_id_2': [dependencies],...}
            Contains all task ids (from the control file) as keys, and their relevant dependencies (as task ids). The list from the test control
            file produces a dependency graph that looks like this:
            ``{'job_1': [], 'job_2': [], 'job_3': [], 'sched_job_1': [], 'model_1': ['job_3'], 'app_1': ['model_1']}``
    allow_partial_failure : bool, default=False
        If partial failures are allowed, the execution continues even if individual tasks fail.
    """
    DAG_FAILED = "Failed"
    DAG_RUNNING = "Running"
    DAG_SUCCEEDED = "Succeeded"
 
    def __init__(self, tasks, dependency_graph, allow_partial_failure=False):
        self.tasks = tasks
        self.dependency_graph = dependency_graph
        self.allow_partial_failure = allow_partial_failure
 
        self.ready_tasks = []
        self.failed_tasks = []

        self.log = logging.getLogger(__name__)
 
    def get_dependency_statuses(self, task_id):
        """
        Returns a list of statuses for all dependencies for a given task_id.
        This enables us to check if all prerequisites for a task have been completed so
        the task itself can be scheduled. 
        """
        dependency_statuses = []
        deps = self.dependency_graph[task_id]
        for dep in deps:
            dependency_statuses += [self.tasks[dep].status()]
        return dependency_statuses
 
    def are_task_dependencies_complete(self, task_id):
        """Check if all task dependencies have completed successfully.

        Parameters
        ----------
        task_id : str
            Task name (unique id).

        Returns
        -------
        bool : True if all dependent tasks have completed with DominoTask.STAT_SUCCEEDED status.
    
        See Also
        --------
        get_dependency_statuses : list of statuses for all dependencies
        """
        dependency_statuses = self.get_dependency_statuses(task_id)
        if dependency_statuses:
            all_deps_succeeded = all(status == DominoTask.STAT_SUCCEEDED for status in dependency_statuses)
        else:
            all_deps_succeeded = True
        return all_deps_succeeded
 
    def get_ready_tasks(self):
        """Returns a list of tasks that are ready for execution.
        """
        return self.ready_tasks
 
    def get_failed_tasks(self):
        """Returns a list of tasks that have failed.
        """
        return self.failed_tasks
 
    def update_tasks_states(self):
        """Updates the status of all tasks in the DAG.

        Returns
        -------
        failed_tasks : a list of DominoTask
            Tasks that have failed
        ready_tasks : a list of DominoTask
            Tasks that are ready for execution

        See Also
        --------
        See the :DominoTask:'dom_orch.tasks.DominoTask' class.
        """        
        self.failed_tasks = []
        self.ready_tasks = []
        all_tasks = self.tasks
        all_states = {}
 
        for task_id, task in all_tasks.items():
            all_states[task_id] = task.status()
 
        for task_id in all_states:
            status = all_states[task_id]
            task = all_tasks[task_id]
            self.log.info("task_id: {0:15} status:{1}".format(task_id, status))
    
            # Check for failed tasks
            if status == DominoTask.STAT_FAILED and task.retries >= task.max_retries:
                self.failed_tasks.append(task)
 
            # Check for ready tasks
            deps_complete = self.are_task_dependencies_complete(task_id)
            task_status_ready = (status == DominoTask.STAT_FAILED and task.retries < task.max_retries) or (status == DominoTask.STAT_UNSUBMITTED)
            if deps_complete and task_status_ready:
                self.ready_tasks.append(task)
 
        return self.failed_tasks, self.ready_tasks            
 
    def pipeline_status(self):
        """Get the pipeline status.

        Returns
        -------

        str: {DAG_RUNNING, DAG_FAILED, DAG_SUCCEEDED}
            DAG_RUNNING - the pipeline is running
            DAG_FAILED - the pipeline has failed
            DAG_SUCCEEDED - the pipeline has completed successfully
        """
        status = self.DAG_RUNNING
 
        if len(self.get_failed_tasks()) > 0 and self.allow_partial_failure == False:
            status = self.DAG_FAILED
        elif all(task.is_complete() for task_id, task in self.tasks.items()):
            status = self.DAG_SUCCEEDED
        return status
    
    def validate_dag(self):
        """
        TODO: Implement graph validation. Make sure that:
              * The graph is not cyclic
              * All task names are unique
        """
        pass
  
    def get_tasks(self):
        """Get a list of all tasks in the graph

        Returns
        -------
        list of DominoTask:
            All tasks in the DAG

        See Also
        --------
        See the :DominoTask:'dom_orch.tasks.DominoTask' class.
        """
        return self.tasks
 
    def __str__(self):
        dict_3 = {**self.tasks, **self.dependency_graph}
        for key, value in dict_3.items():
            if key in self.tasks and key in self.dependency_graph:
               dict_3[key] = [self.tasks[key].__class__.__name__, value]
 
        return json.dumps(dict_3, indent=2)

class PipelineRunner:
    """Responsible for running a pipeline based on the DAG structure.

    Parameters
    ----------
    dag : Dag
        The execution graph.
    tick_freq : int, default=15
        Number of seconds to wait between checking the status of the execution graph.
        The default is 15 seconds.

    See Also
    --------
    See the :Dag:'dom_orch.pipeline.Dag' class.
    """
    def __init__(self, dag, tick_freq=15):
        self.dag = dag
        self.tick_freq = tick_freq

        self.log = logging.getLogger(__name__)
 
    def run(self):
        """Starts and operates the graph execution cycle
        """
        # Loop until failure or until everything has been executed
        self.log.info("Starting the pipeline...")
 
        while True:
            
            failed_tasks, ready_tasks = self.dag.update_tasks_states()
            pipeline_status = self.dag.pipeline_status()
 
            if pipeline_status == Dag.DAG_SUCCEEDED:
                # Pipeline completed successfully
                break
            elif pipeline_status == Dag.DAG_FAILED:
                #for task in failed_tasks:
                #    self.log.error("Failed task detected. task_id: {0}\t status: {1}".format(task.task_id, task.status()))
                raise RuntimeError("Pipeline Execution Failed")
            
            # Check for tasks
            if len(ready_tasks) == 0:
                self.log.info("Waiting for executions or new tasks...")
            else:
                self.log.info("Task(s) ready for submission: {0}".format(", ".join([task.task_id for task in ready_tasks])))
 
            # Submit all tasks that are ready
            for task in ready_tasks:
                response_json = task.submit()
                #print(json.dumps(response_json, indent=2))
                #print(20*"-")
 
            time.sleep(self.tick_freq)
 
        #print("Pipeline completed. Status: {}".format(pipeline_status))
 

class DagBuilder:
    """Builds a dag from a control file.

    Parameters
    ----------
    control_file : str
        A control file containing an execution graph. The structure of the control file follows a dictionary/attributes paradigm.
        Each task has a unique id (str), which is used for describing relationships (dependencies). The valid task types
        are {run, model, app}

    Returns
    -------
    dag : Dag
        The execution graph.

    See Also
    --------
    * The configuration file parser - `configparser <https://docs.python.org/3/library/configparser.html>`_.

    * The :Dag:'dom_orch.pipeline.Dag' class.

    Examples
    --------
    A simple configuration file with 2 jobs, 1 scheduled task, 1 model, and 1 app could look like this:

    ```
        [job_1]
        type: run
        command: hello.py job_1
        
        [job_2]
        type: run
        command: hello.py job_2
        tier: Large
                
        [sched_job_1]
        type: run
        cron_string: * 0/20 0 ? * SUN,MON,TUE,WED,THU,FRI *
        command: hello.py job_1
        tier: small

        [model_1]
        type: model
        name: Hello Model
        description: Hello Test model
        file: model.py
        function: return_hello
        depends: job_2
        
        [app_1]
        type: app
        name: TestApp1
        tier: Large
        depends: model_1
    ```

    Note, that model_1 depends on job_2, and app_1 depends on model_1. There is no dependency between job_1, job_2, and sched_job_1, so these
    three will be executed in parallel.
    """
    def __init__(self, control_file):
        self.control_file = control_file

    def build_dag(self):
 
        # Valid task types
        task_types = ["run", "app", "model"]
    
        # Parse the configuration file
        c = configparser.ConfigParser(allow_no_value=True)
        c.read(self.control_file)
    
        # Build dependency graph
        tasks = {}
        dependency_graph = {}
        task_ids = c.sections()
    
        for task_id in task_ids:
    
            # Check for task dependencies
            if c.has_option(task_id, "depends"):
                dependencies_str = c.get(task_id, "depends")
                dependencies = dependencies_str.split()
            else:
                dependencies = []
            dependency_graph[task_id] = dependencies
    
            # HW tier set?
            domino_run_kwargs = {}
            if c.has_option(task_id, "tier"):
                tier = c.get(task_id, "tier")
                domino_run_kwargs["tier"] = tier
    
            # Task type set?
            if c.has_option(task_id, "type"):
                task_type = c.get(task_id, "type").lower()
            else:
                # If no task type is set we assume it's a job
                task_type = "run"
            
            if task_type == "run":
                # Task is a job
                # Direct command?
                if c.has_option(task_id, "direct"):
                    isDirect = c.get(task_id, "direct").lower() == "true"
                else:
                    isDirect = False
                
                command_str = c.get(task_id, "command")
                if isDirect or c.has_option(task_id, "cron_string"): 
                    command = [command_str]
                else:
                    command = command_str.split()

                # Retries required?
                if c.has_option(task_id, "max_retries"):
                    max_retries = c.get(task_id, "max_retries")
                    domino_run_kwargs["max_retries"] = max_retries

                if c.has_option(task_id, "title"):
                    title = c.get(task_id, "title")
                    domino_run_kwargs["title"] = title

                # Is it a scheduled job?
                if c.has_option(task_id, "cron_string"):
                    cron_string = c.get(task_id, "cron_string")

                    # Check for user override
                    if c.has_option(task_id, "user"):
                         domino_run_kwargs["username"] = c.get(task_id, "user")

                    tasks[task_id] = DominoSchedRun(task_id, command, cron_string, **domino_run_kwargs)

                else:
                    tasks[task_id] = DominoRun(task_id, command, isDirect=isDirect, **domino_run_kwargs)
    
            elif task_type == "model":
                # Task is a model deployment
                if c.has_option(task_id, "name"):
                    model_name = c.get(task_id, "name")
                else:
                    # if there is no model name just use the task_id
                    model_name = task_id
    
                model_id = None
                if c.has_option(task_id, "model_id"):
                    model_id = c.get(task_id, "model_id")
    
                environment = None
                if c.has_option(task_id, "environment"):
                    environment = c.get(task_id, "environment")
    
                deploy_by_name = False
                if c.has_option(task_id, "deploy_by_name"):
                    deploy_by_name = bool(c.get(task_id, "deploy_by_name"))

                model_description = None
                if c.has_option(task_id, "description"):
                    model_description = c.get(task_id, "description")
    
                if (c.has_option(task_id, "file") and c.has_option(task_id, "function")):
                        file_name = c.get(task_id, "file")
                        function = c.get(task_id, "function")
                else:
                    raise ValueError("File and function are mandatory fields for a Model API task.")

                tasks[task_id] = DominoModel(task_id, file_name, function, model_name, model_description, model_id, environment, deploy_by_name)
                
            elif task_type == "app":
                # Task is an app deployment
                domino_run_kwargs = {}
                
                app_name = task_id # Use task_id as the defualt app name
                if c.has_option(task_id, "name"):
                    app_name = c.get(task_id, "name")
    
                if c.has_option(task_id, "tier"):
                    tier = c.get(task_id, "tier")
                    domino_run_kwargs["tier"] = tier
    
                tasks[task_id] = DominoApp(task_id, app_name, **domino_run_kwargs)
    
            else:
                raise ValueError("{0} is not a valid task type. Must be one of {1}".format(task_type, task_types))
    
        return Dag(tasks, dependency_graph)        