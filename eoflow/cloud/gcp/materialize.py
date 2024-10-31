import json
import sys

from dagster_pipes import PipesMappingParamsLoader, open_dagster_pipes
from google.cloud.storage import Client

from eoflow.cloud.gcp.pipes import PipesCloudStorageMessageWriter
from eoflow.core.materialize import materialize_tile
from eoflow.models import Tile


def eager():
    """the entrypoint for a cloud run job that materializes a single tile"""

    data = json.loads(sys.argv[1])

    with open_dagster_pipes(
        params_loader=PipesMappingParamsLoader(data),
        message_writer=PipesCloudStorageMessageWriter(client=Client()),
    ) as pipes:

        tile = Tile(tile=pipes.asset_key)
        revisits = pipes.params["revisits"]
        dataspec = pipes.params["dataspec"]

        return materialize_tile(tile, revisits, dataspec, logger=pipes.log)