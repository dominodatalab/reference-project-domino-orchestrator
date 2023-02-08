# reference-project-domino-orchestrator
A lightweight framework for executing tasks via the Domino API




## Example usage



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