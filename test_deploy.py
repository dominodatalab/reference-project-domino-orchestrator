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
import logging

from dom_orch.api import DominoAPISession
from dom_orch.pipeline import Dag, DagBuilder
from dom_orch.pipeline import PipelineRunner

def main():
    """Simple demo on how to use the orchestrator with the sample control file.

    Note, that this assumes the following environment variables are set:

    DOMINO_USER_API_KEY - Name of the running project
    DOMINO_PROJECT_NAME - User API key
    DOMINO_PROJECT_OWNER - Username of the owner of the running project
    """

    # Set up logging
    DOMINO_LOG_LEVEL = os.getenv("DOMINO_LOG_LEVEL", "INFO").upper()
    logging_level = logging.getLevelName(DOMINO_LOG_LEVEL)
    logging.basicConfig(level=logging_level)
    log = logging.getLogger(__name__)

    # Connect to Domino
    api = DominoAPISession.instance()
    
    # Parse config and build DAG
    dag_builder = DagBuilder("test_deploy.cfg")
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

if __name__ == "__main__":
    main()