"""
With gratitude and kudos to JasperHG90
https://github.com/JasperHG90/dagster-pipes-gcp/blob/main/dagster/dg_pipes.py
"""

import json
import os
from contextlib import contextmanager
from typing import IO, Any, Iterator, Mapping, Optional, Sequence

import dagster._check as check
import google.cloud.logging
import google.cloud.storage
from dagster import PipesClient  # type: ignore
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesMessageReader,
)
from dagster._core.pipes.context import PipesMessageHandler
from dagster._core.pipes.utils import (
    PipesBlobStoreMessageReader,
    PipesEnvContextInjector,
    PipesLogReader,
    extract_message_or_forward_to_stdout,
    open_pipes_session,
)
from dagster_pipes import (  # _assert_opt_env_param_type
    PipesBlobStoreMessageWriter,
    PipesBlobStoreMessageWriterChannel,
    PipesDefaultMessageWriter,
    PipesMessage,
    PipesMessageWriter,
    PipesMessageWriterChannel,
    PipesParams,
)
from google.cloud import run_v2

from eoflow.cloud.common.utils import get_execution_logs

# TODO: make this cloud-agnostic, move cloud-specific to cloud/<cloud>/pipes.py


logging_client = google.cloud.logging.Client()


def invoke_cloud_run_job(data: dict):
    """Invoke a cloud run job to materialize a tile"""

    client = run_v2.JobsClient()

    request = run_v2.RunJobRequest(
        name=os.environ["GCP_MATERIALIZE_EAGER_RUN_JOB_NAME"],
        overrides=dict(
            container_overrides=[
                dict(env=[dict(name=k, value=v) for k, v in data.items()])
            ]
        ),
    )

    # Make the request
    operation = client.run_job(request=request)

    response = operation.result()

    return response


class PipesCloudStorageMessageWriter(PipesBlobStoreMessageWriter):
    """Message writer that writes messages by periodically writing message chunks to a GCS bucket.

    Args:
        client (google.cloud.storage.Client): A google.cloud.storage client object
        interval (float): interval in seconds between upload chunk uploads
    """

    def __init__(
        self,
        client: google.cloud.storage.Client,
        bucket: str,
        prefix: str,
        task_index: int,
        *,
        interval: float = 10,
    ):
        super().__init__(interval=interval)
        self._client = client
        self.bucket = bucket
        self.prefix = prefix
        self.task_index = task_index

    def make_channel(
        self,
        params: PipesParams,
    ) -> "PipesCloudStorageMessageWriterChannel":

        return PipesCloudStorageMessageWriterChannel(
            client=self._client,
            bucket=self.bucket,
            key_prefix=self.prefix,
            interval=self.interval,
        )


class PipesCloudStorageMessageWriterChannel(PipesBlobStoreMessageWriterChannel):
    """Message writer channel for writing messages by periodically writing message chunks to a GCS bucket.

    Args:
        client (google.cloud.storage.Client): A google.cloud.storage client object
        bucket (str): The name of the GCS bucket to write to.
        key_prefix (Optional[str]): An optional prefix to use for the keys of written blobs.
        interval (float): interval in seconds between upload chunk uploads
    """

    def __init__(
        self,
        client: google.cloud.storage.Client,
        bucket: str,
        key_prefix: Optional[str],
        *,
        interval: float = 10,
    ):
        super().__init__(interval=interval)
        self._client = client
        self._bucket = client.bucket(bucket)
        self._key_prefix = key_prefix

    def upload_messages_chunk(self, payload: IO, index: int) -> None:
        print("uploadng message")
        print(payload)
        key = (
            f"{self._key_prefix}/{index}.json" if self._key_prefix else f"{index}.json"
        )
        print("key", key)
        blob = self._bucket.blob(key)
        blob.upload_from_string(payload.read())


class PipesCloudLoggerMessageWriterChannel(PipesMessageWriterChannel):

    def __init__(self, trace: str):
        self._trace = trace
        self._logger = logging_client.logger("dagster-pipes-utils")

    def write_message(self, message: PipesMessage):
        self._logger.log_struct(
            {
                "severity": "INFO",
                "message": json.dumps(message),
                "trace": self._trace,
            }
        )


class PipesCloudLoggerMessageWriter(PipesMessageWriter):

    def __init__(self, trace: str) -> None:
        super().__init__()
        self._trace = trace

    @contextmanager
    def open(
        self, params: PipesParams
    ) -> Iterator[PipesCloudLoggerMessageWriterChannel]:
        yield PipesCloudLoggerMessageWriterChannel(self._trace)


class PipesCloudStorageMessageReader(PipesBlobStoreMessageReader):
    """Message reader that reads messages by periodically reading message chunks from a specified GCS
    bucket.

    If `log_readers` is passed, this reader will also start the passed readers
    when the first message is received from the external process.

    Args:
        interval (float): interval in seconds between attempts to download a chunk
        bucket (str): The GCS bucket to read from.
        client (google.cloud.storage.Client): A google.cloud.storage client object
        log_readers (Optional[Sequence[PipesLogReader]]): A set of readers for logs on GCS.
    """

    def __init__(
        self,
        *,
        interval: float = 10,
        bucket: str,
        prefix: Optional[str],
        client: google.cloud.storage.Client,
        log_readers: Optional[Sequence[PipesLogReader]] = None,
    ):
        super().__init__(
            interval=interval,
            log_readers=log_readers,
        )
        self.key_prefix: Optional[str] = prefix
        self.bucket = check.str_param(bucket, "bucket")
        self.client = client

    @contextmanager
    def get_params(self) -> Iterator[PipesParams]:
        # key_prefix = context.run.run_id

        # key_prefix = "".join(random.choices(string.ascii_letters, k=30))  # nosec
        yield {"bucket": self.bucket, "key_prefix": self.key_prefix}

    def download_messages_chunk(self, index: int, params: PipesParams) -> Optional[str]:
        key = f"{params['key_prefix']}/{index}.json"
        bucket = self.client.bucket(self.bucket)
        blob = bucket.blob(key)
        if blob.exists():
            return blob.download_as_text()
        else:
            return None

    def no_messages_debug_text(self) -> str:
        return (
            f"Attempted to read messages from GCS path {self.bucket}/{self.key_prefix}. Expected"
            " PipesCloudStorageMessageReader to be explicitly passed to open_dagster_pipes in the external"
            " process."
        )

    def messages_are_readable(self, params: PipesParams) -> bool:
        key_prefix = params.get("key_prefix")
        if key_prefix is None:
            return False
        bucket = self.client.bucket(self.bucket)
        blobs = list(bucket.list_blobs(prefix=key_prefix))
        return bool(blobs)


class PipesCloudLoggerMessageReader(PipesMessageReader):
    """Message reader that consumes logs from Google Cloud Logging. This means messages
    emitted during the computation will only be processed once the cloud function completes.

    Adapted from: https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-aws/dagster_aws/pipes.py
    """

    @contextmanager
    def read_messages(
        self,
        handler: PipesMessageHandler,
    ) -> Iterator[PipesParams]:
        self._handler = handler
        try:
            # use buffered stdio to shift the pipes messages to the tail of logs
            yield {
                PipesDefaultMessageWriter.BUFFERED_STDIO_KEY: PipesDefaultMessageWriter.STDERR
            }
        finally:
            self._handler = None

    def consume_cloud_function_logs(self, trace_id: str) -> None:
        handler = check.not_none(
            self._handler, "Can only consume logs within context manager scope."
        )

        # Get logs
        log_result = get_execution_logs(trace_id)

        for log_line in log_result:
            extract_message_or_forward_to_stdout(handler, log_line)

    def no_messages_debug_text(self) -> str:
        return "Attempted to read messages by extracting them from the tail of cloud function logs directly."


class PipesCloudFunctionEventContextInjector(PipesEnvContextInjector):
    def no_messages_debug_text(self) -> str:
        return "Attempted to inject context via the cloud function event input."


class PipesEagerJobClient(PipesClient, TreatAsResourceParam):
    """A pipes client for invoking Google Cloud Function.

    By default context is injected via the GCF API call and logs are extracted using google
    cloud logging client.
    """

    def __init__(
        self,
        message_reader: Optional[PipesMessageReader] = None,
    ):
        self._message_reader = message_reader or PipesCloudLoggerMessageReader()
        self._context_injector = PipesCloudFunctionEventContextInjector()

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def run(
        self,
        *,
        function_name: str,
        data: Mapping[str, Any],
        context: OpExecutionContext,
    ):
        """Synchronously invoke a cloud function function, enriched with the pipes protocol.

        Args:
            function_url (str): The HTTP url that triggers the cloud function.
            event (Mapping[str, Any]): A JSON serializable object to pass as input to the cloud function.
            context (OpExecutionContext): The context of the currently executing Dagster op or asset.
        """
        with open_pipes_session(
            context=context,
            message_reader=self._message_reader,
            context_injector=self._context_injector,
        ) as session:

            print("SESSONS")
            print(session)
            print(dir(session))
            print(type(session))

            if isinstance(
                self._context_injector, PipesCloudFunctionEventContextInjector
            ):
                payload_data = {
                    **data,
                    **session.get_bootstrap_env_vars(),
                }
            else:
                payload_data: Mapping[str, Any] = data  # type: ignore

            pipes_messages = session.get_bootstrap_params().get(
                "DAGSTER_PIPES_MESSAGES"
            )
            if (
                "bucket" in pipes_messages.keys()
                and "key_prefix" in pipes_messages.keys()
            ):
                context.log.debug(
                    f"Execution logs are stored in gs://{pipes_messages['bucket']}/{pipes_messages['key_prefix']}/"
                )

            response = invoke_cloud_run_job(  # noqa: F821
                data=payload_data,
            )

            print("response:", response)
            print(type(response))
            print(response.succeeded_count, type(response.succeeded_count))
            print(response.task_count, type(response.task_count))

            success = int(response.succeeded_count) == int(response.task_count)

            context.log.debug(f"Response status: {success}")
            if not success:
                context.log.error(
                    f"Failed to invoke cloud function {function_name}. Returned success {success}."
                )
                context.log.error(response)
                raise ValueError(
                    f"Failed to invoke cloud function {function_name} with status code {success}"
                )

            context.log.info("Cloud function invoked successfully. Waiting for logs...")
            if isinstance(self._message_reader, PipesCloudLoggerMessageReader):
                trace_id = response.headers.get("X-Cloud-Trace-Context") or "local/0"
                trace_id = trace_id.split(";")[0]
                self._message_reader.consume_cloud_function_logs(trace_id)

        return PipesClientCompletedInvocation(session)
