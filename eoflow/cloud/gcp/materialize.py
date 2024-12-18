import json
import logging
import os

from cloudpathlib import AnyPath
from dagster_pipes import (
    DAGSTER_PIPES_CONTEXT_ENV_VAR,
    DAGSTER_PIPES_MESSAGES_ENV_VAR,
    PipesMappingParamsLoader,
    open_dagster_pipes,
)
from google.cloud.storage import Client

from eoflow.cloud.gcp.pipes import PipesCloudStorageMessageWriter
from eoflow.core.materialize import materialize_tile
from eoflow.models import DataSpec, S2IndexItem, Tile


def eager():
    """the entrypoint for a cloud run job that materializes a single tile"""

    RUN_STORE = os.environ["RUN_STORE"]
    run_id = os.path.split(RUN_STORE)[-1]
    TASK_INDEX = os.environ.get("CLOUD_RUN_TASK_INDEX")

    # basic logging to cloud logs
    logging.info(f"RUN_STORE: {RUN_STORE}")
    logging.info(f"Task index: {os.environ.get('CLOUD_RUN_TASK_INDEX')}")
    logging.info(f"Run ID: {run_id}")

    logging.info(AnyPath(RUN_STORE + "/tiles.json").read_text())
    logging.info(AnyPath(RUN_STORE + "/revisits.json").read_text())
    logging.info(AnyPath(RUN_STORE + "/dataspec.json").read_text())

    tiles = json.loads(AnyPath(RUN_STORE + "/tiles.json").read_text())
    tile = tiles[int(os.environ.get("CLOUD_RUN_TASK_INDEX"))]

    revisits = json.loads(AnyPath(RUN_STORE + "/revisits.json").read_text())
    revisits = [S2IndexItem(**t) for t in revisits if t["mgrs_tile"] == tile]

    dataspec = DataSpec(**json.loads(AnyPath(RUN_STORE + "/dataspec.json").read_text()))

    data = {
        "tile": tile,
        "revisits": revisits,
        "dataspec": dataspec,
    }

    # move to env vars - point these to the cloud storage files
    ctx = {
        DAGSTER_PIPES_CONTEXT_ENV_VAR: os.environ.get(DAGSTER_PIPES_CONTEXT_ENV_VAR),
        DAGSTER_PIPES_MESSAGES_ENV_VAR: os.environ.get(DAGSTER_PIPES_MESSAGES_ENV_VAR),
    }

    with open_dagster_pipes(
        params_loader=PipesMappingParamsLoader(ctx),
        message_writer=PipesCloudStorageMessageWriter(
            client=Client(),
            bucket=AnyPath(RUN_STORE).bucket,
            prefix=AnyPath(RUN_STORE).blob,
            task_index=TASK_INDEX,
        ),
    ) as pipes:

        # transfers logs back to the dagster daemon
        pipes.log.info(f"RUN_STORE: {RUN_STORE}")
        pipes.log.info(f"Task index: {TASK_INDEX}")
        pipes.log.info("tile: {}".format(data["tile"]))

        tile = Tile(tile=data["tile"])
        revisits = data["revisits"]
        dataspec = data["dataspec"]

        pipes.report_custom_message(f"staring materaliazation {tile.tile}")

        idx_blob = materialize_tile(
            tile, revisits, dataspec, logger=pipes.log, run_id=run_id
        )

        pipes.log.info(f"Materialized {tile.tile}")
        AnyPath(RUN_STORE + f"/{tile.tile}-index.json").write_text(
            idx_blob.model_dump_json()
        )

        return 200, "success"
