import os
import logging

from dom_orch.api import DominoAPISession
from dom_orch.pipeline import Dag, DagBuilder
from dom_orch.pipeline import PipelineRunner



def main():
    # Set up logging
    DOMINO_LOG_LEVEL = os.getenv("DOMINO_LOG_LEVEL", "INFO").upper()
    logging_level = logging.getLevelName(DOMINO_LOG_LEVEL)
    logging.basicConfig(level=logging_level)
    log = logging.getLogger(__name__)

    # Connect to Domino
    api = DominoAPISession.instance()
    
    # Parse config and build DAG
    dag_builder = DagBuilder("test_deploy.cfg")
    #dag_builder = DagBuilder("test_scheduled.cfg")
    dag = dag_builder.build_dag()
 
    # Show the DAG
    log.info(dag)

    # Run DAG
    pipeline_runner = PipelineRunner(dag)
    try:
      pipeline_runner.run()
      dag_status = dag.pipeline_status()
 
      if dag_status == Dag.DAG_SUCCEEDED:
          log.info("Pipeline completed successfully.")
      else:
          log.warn("Pipeline terminated in {} state.".format(dag_status))
 
    except RuntimeError as e:
      # TODO: Proper error handling
      log.error("Pipeline execution failed.")
      raise

    """
    
    project_id = api.project_id
    url = api._routes.host + "/v4/projects/" + project_id + "/scheduledjobs"
    url = "https://market4186.marketing-sandbox.domino.tech/v4/projects/63bc098ba111ce3c7f7ca892/scheduledjobs"

    request = {
        "title": "Test",
        "command": "hello.py",
        "schedule": {
            "cronString": "0 0/20 0 ? * * *",
            "isCustom": True
        },
        "hardwareTierIdentifier": "small-k8s",
        "environmentRevisionSpec":"ActiveRevision",
        "notifyOnCompleteEmailAddresses":[],
        "isPaused":False,
        "timezoneId":"Europe/London",
        "publishAfterCompleted":False,
        "allowConcurrentExecution":False,
        "scheduledByUserId": "6141ccfd0f08e1652cfad376",
    }

    response = api.request_manager.post(url, json=request)
    print(response)
    #return response.json()

    """

    """
    request = {
        "projectId":"63bc098ba111ce3c7f7ca892",
        "title":"Test",
        "command":"test.py",
        "schedule":{"cronString":"0 0 12 * * ?",
        "isCustom":False,
        "humanReadableCronString":"every day at 12:00 GMT"},
        "timezoneId":"Europe/London",
        "isPaused":False,
        "publishAfterCompleted":False,
        "allowConcurrentExecution":False,
        "hardwareTierIdentifier":"small-k8s",
        "hardwareTierName":"Small",
        "overrideEnvironmentId":"613f748afd9e5a517530348a",
        "environmentRevisionSpec":"ActiveRevision",
        "scheduledByUserId": "6141ccfd0f08e1652cfad376",
        "scheduledByUserName": "nmanchev",
        "notifyOnCompleteEmailAddresses":[],
        "volumeSpecificationOverride":{"size":{"value":10,"unit":"GiB"}
    }
    """


if __name__ == "__main__":
    main()