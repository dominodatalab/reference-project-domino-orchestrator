# Domino Reference Project: Workflow orchestration
A lightweight framework for executing tasks via the Domino API

This provides a simple orchestrator, which uses the [Domino API](https://docs.dominodatalab.com/en/4.2/api_guide/f35c19/api-guide/) to run a number of tasks (e.g. jobs, models, apps) in the [Domino Enterprise MLOps Platform](https://www.dominodatalab.com/product/domino-enterprise-mlops-platform). Four different type of tasks are supported:

* Run --- This is a standard [Domino Job](https://docs.dominodatalab.com/en/latest/user_guide/942549/jobs/), which is a type of execution where an executor machine is assigned to execute a specified command in its OS shell. A Job can be used to also run Python, R, or Bash scripts.
* Scheduled Job --- This is a job, which is [scheduled for execution](https://docs.dominodatalab.com/en/latest/user_guide/5dce1f/scheduled-jobs/) in advance and set to execute on a regular cadence.
* Model API --- A Domino [Model API](https://docs.dominodatalab.com/en/latest/user_guide/0e1396/model-apis/), a Python or R function available as a REST API endpoint.
* App --- [Domino Apps](https://docs.dominodatalab.com/en/latest/user_guide/8b094b/domino-apps/), hosted web applications written in popular frameworks like Flask, Shiny, or Dash

This framework provides a mechanism for executing such tasks remotely or within a running Domino instance.

## Task definition

The tasks are defined using a control file. The control file syntax follows [configparser](https://docs.python.org/3/library/configparser.html). Each task is a separate entry and must conform to the following format:

```
[task_id]
type = task_type
task_attribute_1 = value_1
task_attribute_2 = value_2
...
```

where `task_id` is a string, uniquely identifying the task (within the namespace of the control file), `type` denotes the task type, and `task_attribute_1,2,...` are task-dependent attributes that provide additional information for the task. Note, that not all task attributes are mandatory. 

There are three type of tasks: `run`, `model`, and `app`. The most simple job run can be expressed like this:

```
[simple_job_1]
type=run
command: hello.py
```

This task executes the hello.py script, which must be present in your Domino project. Note, that `command` is a mandatory attribute for tasks of type `run`. There are optional attributes for overriding the default hardware tier. Including a value for the optional attribute `cron_string`, turn the run into a scheduled job. Notice, that the value of `cron_string` follows the crontab syntax as follows:

```
            # ┌───────────── minute (0 - 59)
            # │ ┌───────────── hour (0 - 23)
            # │ │ ┌───────────── day of the month (1 - 31)
            # │ │ │ ┌───────────── month (1 - 12)
            # │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
            # │ │ │ │ │                                   7 is also Sunday on some systems)
            # │ │ │ │ │
            # │ │ │ │ │
            # * * * * * <command to execute>
```

For example, the following entry defines a scheduled job named `sched_job_1`, which executes the `hello.py` Python script every 20 minutes, on every day of the week except Saturday:

```
[sched_job_1]
type: run
cron_string: * 0/20 0 ? * SUN,MON,TUE,WED,THU,FRI *
command: hello.py
tier: small
```

Feel free to study the [test_deploy.cfg](https://github.com/dominodatalab/reference-project-domino-orchestrator/raw/main/test_deploy.cfg) control file, which provides sample entries for all supported task types.

## Maintaining dependencies

Each task in the control file has an optional attribute `depends`, which takes a list of task names (space separated) that the current task depends on. Using this mechanism enables us to build an acyclic execution graph, which defines a dependency structure. Tasks in the graph will only be scheduled for execution once all of the tasks they depend on (i.e. listed in the `depends` attribute) have been successfully completed.

For example, the demo control file [test_deploy.cfg](https://github.com/dominodatalab/reference-project-domino-orchestrator/raw/main/test_deploy.cfg) defines the following dependency graph:

![dependency graph](https://github.com/dominodatalab/reference-project-domino-orchestrator/raw/main/images/dep_graph.png)

Here you see that the three runs (`job_1`, `job_2`, and `job_3`) have no dependencies, so they will be executed in parallel. `model_1`, however has a dependency on `job_3`, so the orchestrator will wait for `job_3` to complete before executing the `model_1` task. Similarly, `app_1` depends on `model_1`, which needs to be built successfully before the application is deployed.

## Authentication
The [test_deploy.py](https://raw.githubusercontent.com/dominodatalab/reference-project-domino-orchestrator/main/test_deploy.py) and the [DominoAPISession](https://github.com/dominodatalab/reference-project-domino-orchestrator/raw/main/dom_orch/api.py) singleton expect that the following environment variables are present during execution of the test script:

* DOMINO_USER_API_KEY - Name of the running project
* DOMINO_PROJECT_NAME - User API key
* DOMINO_PROJECT_OWNER - Username of the owner of the running project

These are used for identifying the Domino instance URL, current project, and authentication key. They need to be present in the environment running the `test_deploy.py` (or any script using `dom_orch` for that matter).

## Example usage

The [test_deploy.py](https://raw.githubusercontent.com/dominodatalab/reference-project-domino-orchestrator/main/test_deploy.py) files includes sample code for parsing, building the execution graph, and running the tasks defined in the demo control file ([test_deploy.cfg](https://github.com/dominodatalab/reference-project-domino-orchestrator/raw/main/test_deploy.cfg)).

This is the expected output (some parts have been omitted for brevity):

```console
$ python test_deploy.py
INFO:dom_orch.pipeline:Starting the pipeline...
INFO:dom_orch.pipeline:task_id: job_1           status:Unsubmitted
INFO:dom_orch.pipeline:task_id: job_2           status:Unsubmitted
INFO:dom_orch.pipeline:task_id: job_3           status:Unsubmitted
INFO:dom_orch.pipeline:task_id: sched_job_1     status:Unsubmitted
INFO:dom_orch.pipeline:task_id: model_1         status:Unsubmitted
INFO:dom_orch.pipeline:task_id: app_1           status:Unsubmitted
INFO:dom_orch.pipeline:Task(s) ready for submission: job_1, job_2, job_3, sched_job_1
INFO:dom_orch.tasks:-- Submitting run job_1 --
INFO:dom_orch.tasks:Direct task   : False
INFO:dom_orch.tasks:Command       : ['hello.py', 'job_1']
INFO:dom_orch.tasks:-- Submitting run job_2 --
INFO:dom_orch.tasks:Direct task   : False
INFO:dom_orch.tasks:Command       : ['hello.py', 'job_2']
INFO:dom_orch.tasks:Tier override : Large
INFO:dom_orch.tasks:-- Submitting run job_3 --
INFO:dom_orch.tasks:Direct task   : False
INFO:dom_orch.tasks:Command       : ['hello.py', 'job_3']
INFO:dom_orch.tasks:-- Submitting scheduled job sched_job_1 --
INFO:dom_orch.tasks:Title       : sched_job_1
INFO:dom_orch.tasks:Command     : hello.py job_1
INFO:dom_orch.tasks:Tier        : small
INFO:dom_orch.tasks:Cron string : * 0/20 0 ? * SUN,MON,TUE,WED,THU,FRI *
INFO:dom_orch.tasks:Submission of scheduled job sched_job_1 succeeded. Job id is fn72VoLNeGxS4SfSQVGtAurLtAaj7h68500e1PwT
...
INFO:dom_orch.pipeline:Waiting for executions or new tasks...
INFO:dom_orch.pipeline:task_id: job_1           status:Succeeded
INFO:dom_orch.pipeline:task_id: job_2           status:In-progress
INFO:dom_orch.pipeline:task_id: job_3           status:Succeeded
INFO:dom_orch.pipeline:task_id: sched_job_1     status:Succeeded
INFO:dom_orch.pipeline:task_id: model_1         status:Unsubmitted
INFO:dom_orch.pipeline:task_id: app_1           status:Unsubmitted
INFO:dom_orch.pipeline:Task(s) ready for submission: model_1
INFO:dom_orch.tasks:-- Submitting model model_1 --
INFO:dom_orch.tasks:Environment : 635873f8393c357ed5b9a23b
INFO:dom_orch.tasks:Function    : return_hello
INFO:dom_orch.tasks:File        : model.py
INFO:dom_orch.tasks:Model name  : Hello Model9
INFO:dom_orch.tasks:Model ID    : 63d2b772a111ce3c7f7cb41e
INFO:dom_orch.tasks:This is an existing model. We need to build a new version instead of deploying a new model.
INFO:dom_orch.tasks:Created a model with model_id 63d2b772a111ce3c7f7cb41e and model_version 63e3b36da111ce3c7f7cb94a
INFO:dom_orch.tasks:--------------------
...
INFO:dom_orch.pipeline:Waiting for executions or new tasks...
INFO:dom_orch.pipeline:task_id: job_1           status:Succeeded
INFO:dom_orch.pipeline:task_id: job_2           status:In-progress
INFO:dom_orch.pipeline:task_id: job_3           status:Succeeded
INFO:dom_orch.pipeline:task_id: sched_job_1     status:Succeeded
INFO:dom_orch.pipeline:task_id: model_1         status:Succeeded
INFO:dom_orch.pipeline:task_id: app_1           status:Unsubmitted
INFO:dom_orch.pipeline:Task(s) ready for submission: app_1
INFO:dom_orch.tasks:-- Submitting app app_1 --
INFO:dom_orch.tasks:Name          : TestApp3
INFO:dom_orch.tasks:Hardware tier : Large
INFO:dom_orch.tasks:Unpublishing running apps...
INFO:dom_orch.tasks:Creating new app...
INFO:dom_orch.tasks:Successfully created application with app_id: 63d77bdfa111ce3c7f7cb4a8
INFO:dom_orch.tasks:Starting application with app_id: 63d77bdfa111ce3c7f7cb4a8
INFO:dom_orch.tasks:--------------------
INFO:dom_orch.pipeline:task_id: job_1           status:Succeeded
INFO:dom_orch.pipeline:task_id: job_2           status:In-progress
INFO:dom_orch.pipeline:task_id: job_3           status:Succeeded
INFO:dom_orch.pipeline:task_id: sched_job_1     status:Succeeded
INFO:dom_orch.pipeline:task_id: model_1         status:Succeeded
INFO:dom_orch.pipeline:task_id: app_1           status:In-progress
INFO:dom_orch.pipeline:Waiting for executions or new tasks...
...
INFO:dom_orch.pipeline:Waiting for executions or new tasks...
INFO:dom_orch.pipeline:task_id: job_1           status:Succeeded
INFO:dom_orch.pipeline:task_id: job_2           status:Succeeded
INFO:dom_orch.pipeline:task_id: job_3           status:Succeeded
INFO:dom_orch.pipeline:task_id: sched_job_1     status:Succeeded
INFO:dom_orch.pipeline:task_id: model_1         status:Succeeded
INFO:dom_orch.pipeline:task_id: app_1           status:Succeeded
INFO:__main__:Pipeline completed successfully.
$ 
```

## Notes

This package has been tested and certified using [python-domino](https://github.com/dominodatalab/python-domino) version 1.2.2. Trying to use it with other versions of the `python-domino` package may lead to unstable behaviour.