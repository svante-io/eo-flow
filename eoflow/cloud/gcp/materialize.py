import json
import logging
import os

from cloudpathlib import AnyPath
from dagster_pipes import PipesMappingParamsLoader, open_dagster_pipes
from google.cloud.storage import Client

from eoflow.cloud.gcp.pipes import PipesCloudStorageMessageWriter
from eoflow.core.materialize import materialize_tile
from eoflow.models import Tile


def eager():
    """the entrypoint for a cloud run job that materializes a single tile"""

    RUN_STORE = os.environ["RUN_STORE"]

    # basic logging to cloud logs
    logging.info(f"RUN_STORE: {RUN_STORE}")
    logging.info(f"Task index: {os.environ.get('CLOUD_RUN_TASK_INDEX')}")

    logging.info(AnyPath(RUN_STORE + "/tiles.json").read_text())
    logging.info(AnyPath(RUN_STORE + "/revisits.json").read_text())
    logging.info(AnyPath(RUN_STORE + "/dataspec.json").read_text())

    tiles = json.loads(AnyPath(RUN_STORE + "/tiles.json").read_text())
    tile = tiles[int(os.environ.get("CLOUD_RUN_TASK_INDEX"))]

    revisits = json.loads(AnyPath(RUN_STORE + "/revisits.json").read_text())
    revisits = [t for t in revisits if t["tile"] == tile]

    dataspec = json.loads(AnyPath(RUN_STORE + "/dataspec.json").read_text())

    data = {
        "tile": tile,
        "revisits": revisits,
        "dataspec": dataspec,
    }

    with open_dagster_pipes(
        params_loader=PipesMappingParamsLoader(data),
        message_writer=PipesCloudStorageMessageWriter(client=Client()),
    ) as pipes:

        tile = Tile(tile=data["tile"])
        revisits = pipes.params["revisits"]
        dataspec = pipes.params["dataspec"]

        return materialize_tile(tile, revisits, dataspec, logger=pipes.log)
