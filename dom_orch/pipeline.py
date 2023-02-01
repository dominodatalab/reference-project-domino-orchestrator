import json
import logging
import time
import configparser

from .tasks import DominoApp, DominoModel, DominoRun, DominoSchedRun, DominoTask

class Dag:
    """
    self.tasks              # dictionary of task_ids -> DominoRun objects
    self.dependency_graph   # dictionary of task_ids -> list of dependency task_ids
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
        dependency_statuses = []
        deps = self.dependency_graph[task_id]
        for dep in deps:
            dependency_statuses += [self.tasks[dep].status()]
        return dependency_statuses
 
    def are_task_dependencies_complete(self, task_id):
        dependency_statuses = self.get_dependency_statuses(task_id)
        if dependency_statuses:
            all_deps_succeeded = all(status == DominoTask.STAT_SUCCEEDED for status in dependency_statuses)
        else:
            all_deps_succeeded = True
        return all_deps_succeeded
 
    def get_ready_tasks(self):
        return self.ready_tasks
 
    def get_failed_tasks(self):
        return self.failed_tasks
 
    def update_tasks_states(self):
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
        status = self.DAG_RUNNING
 
        if len(self.get_failed_tasks()) > 0 and self.allow_partial_failure == False:
            status = self.DAG_FAILED
        elif all(task.is_complete() for task_id, task in self.tasks.items()):
            status = self.DAG_SUCCEEDED
        #         elif len(self.get_ready_tasks()) == 0: # infinite loop if not Succeeded
        #             status = 'Failed'
        return status
        # inspect graph, run statuses, allow_partial_failure to determine whether pipeline should continue
 
    def validate_dag(self):
        pass
 
    def validate_run_command(self):
        pass
 
    def get_tasks(self):
        return self.tasks
 
    def __str__(self):
 
        dict_3 = {**self.tasks, **self.dependency_graph}
        for key, value in dict_3.items():
            if key in self.tasks and key in self.dependency_graph:
               dict_3[key] = [self.tasks[key].__class__.__name__, value]
 
        #return pprint.pformat(dict_3, width=1)
        return json.dumps(dict_3, indent=2)

class PipelineRunner:
    '''
    should this be stateless or stateful?
    - needs to be stateful to track run IDs and states (for retry logic) of various tasks
    - use Dag object to store state
    '''
 
    def __init__(self, dag, tick_freq=15):
        """
        Initialises a pipline runner object
        :param Dag dag: The execution graph
        :param int tick_freq: Seconds to wait between checking the status of the graph. Default is 15 seconds.
        """
        self.dag = dag
        self.tick_freq = tick_freq

        self.log = logging.getLogger(__name__)
 
    def run(self):
        # Loop until failure or until everything has been executed
        self.log.info("Starting the pipeline...")
 
        while True:
            
            failed_tasks, ready_tasks = self.dag.update_tasks_states()
            pipeline_status = self.dag.pipeline_status()
 
            if pipeline_status == Dag.DAG_SUCCEEDED:
                # Pipeline completed successfully
                break
            elif pipeline_status == Dag.DAG_FAILED:
                #for task_id, task in self.dag.get_tasks().items():
                for task_id, task in failed_tasks:
                    self.log.error("task_id: {0}\t status: {1}".format(task_id, task.status()))
                raise RuntimeError("Pipeline Execution Failed")
            
            # Check for tasks
            #ready_tasks = dag.get_ready_tasks()
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

    def __init__(self, control_file):
        self.control_file = control_file

    def build_dag(self):
 
        # Valid task types
        task_types = ["run", "app", "model"]
    
        # Parse the configuration file
        c = configparser.ConfigParser(allow_no_value=True)
        c.read(self.control_file)
    
        # Build dependancy graph
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
                if isDirect:
                    command = [command_str]
                else:
                    command = command_str.split()             

                # Retries required?
                if c.has_option(task_id, "max_retries"):
                    max_retries = c.get(task_id, "max_retries")
                    domino_run_kwargs["max_retries"] = max_retries
            
                # Is it a scheduled job?
                if c.has_option(task_id, "cron_string"):
                    cron_string = c.get(task_id, "cron_string")
                    
                    if c.has_option(task_id, "title"):
                        title = c.get(task_id, "title")
                        domino_run_kwargs["title"] = title

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
    
    
                model_description = None
                if c.has_option(task_id, "description"):
                    model_description = c.get(task_id, "description")
    
                if (c.has_option(task_id, "file") and c.has_option(task_id, "function")):
                        file_name = c.get(task_id, "file")
                        function = c.get(task_id, "function")
                else:
                    raise ValueError("File and function are mandatory fields for a Model API task.")
    
                tasks[task_id] = DominoModel(task_id, file_name, function, model_name, model_description, model_id, environment)
                
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