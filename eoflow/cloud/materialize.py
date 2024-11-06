import json
import os

from cloudpathlib import AnyPath
from dagster import In, OpExecutionContext, Out, op

from eoflow.core import settings
from eoflow.models import DagsterS2IndexDF, DataSpec, S2IndexDF, S2IndexDFtoItems

if settings.CLOUD == "gcp":
    from eoflow.cloud.gcp.materialize import eager
    from eoflow.cloud.gcp.pipes import (
        PipesCloudStorageMessageReader,
        PipesEagerJobClient,
    )

    @op(ins={"df_revisits": In(DagsterS2IndexDF)}, out=Out())
    def op_materialize_tile_eager(
        context: OpExecutionContext,
        df_revisits: S2IndexDF,
        pipes_run_job_client: PipesEagerJobClient,
        config: DataSpec,
    ):
        """Deploy cloud run jobs to materialise the dataset."""

        AnyPath(config.dataset_store + "/tiles.json").write_text(
            json.dumps(df_revisits["mgrs_tile"].values.tolist())
        )
        AnyPath(config.dataset_store + "/revisits.json").write_text(
            json.dumps(
                [
                    json.loads(item.model_dump_json())
                    for item in S2IndexDFtoItems(df_revisits)
                ]
            )
        )
        AnyPath(config.dataset_store + "/dataspec.json").write_text(
            json.dumps(json.loads(config.model_dump_json()))
        )

        return pipes_run_job_client.run(
            context=context,
            function_name=os.environ.get("GCP_MATERIALIZE_EAGER_RUN_JOB_NAME"),
            data=dict(RUN_STORE=config.dataset_store),
        ).get_materialize_result()

    __all__ = [
        "PipesCloudStorageMessageReader",
        "eager",
    ]

else:

    def op_materialize_tile_eager():
        raise ValueError(
            "ENV CLOUD={} not supported or not set.".format(os.getenv("CLOUD"))
        )
