import os

from google.cloud import run_v2

from eoflow.cloud.common.pipes import (
    PipesCloudStorageMessageReader,
    PipesCloudStorageMessageWriter,
    PipesEagerJobClient,
)

# TODO: overwrite GCP specific pipes


def invoke_cloud_run_job(data):
    """Invoke a cloud run job to materialize a tile"""

    client = run_v2.JobsClient()

    request = run_v2.RunJobRequest(
        name=os.environ["CLOUD_RUN_JOB_NAME"],
        overrides=dict(envs=[dict(name="DATA", value=data)]),
    )

    # Make the request
    operation = client.run_job(request=request)

    response = operation.result()

    return response


__all__ = [
    "invoke_cloud_run_job",
    "PipesCloudStorageMessageWriter",
    "PipesCloudStorageMessageReader",
    "PipesEagerJobClient",
]
