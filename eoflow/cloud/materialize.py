import json
import os

from dagster import AssetExecutionContext, In, Out, op

from eoflow.core import settings
from eoflow.models import DagsterS2IndexDF, DataSpec, S2IndexDF, S2IndexDFtoItems

if settings.CLOUD == "gcp":
    from eoflow.cloud.gcp.pipes import PipesEagerJobClient

    @op(ins={"df_revisit_slice": In(DagsterS2IndexDF)}, out=Out())
    def op_materialize_tile_eager(
        context: AssetExecutionContext,
        df_revisit_slice: S2IndexDF,
        pipes_run_job_client: PipesEagerJobClient,
        config: DataSpec,
    ):
        """Deploy cloud run jobs to materialise the dataset."""

        data = {
            "tile": df_revisit_slice["mgrs_tile"].values[0],
            "revisits": [
                json.loads(item.model_dump_json)
                for item in S2IndexDFtoItems(df_revisit_slice)
            ],
            "dataspec": json.loads(config.model_dump_json),
        }

        return pipes_run_job_client.run(
            context=context,
            function_url=os.environ.get(""),
            data=data,
        ).get_materialize_result()

else:

    def op_materialize_tile_eager():
        raise ValueError(
            "ENV CLOUD={} not supported or not set.".format(os.getenv("CLOUD"))
        )
