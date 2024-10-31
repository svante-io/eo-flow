"""
With gratitude and kudos to JasperHG90
https://github.com/JasperHG90/dagster-pipes-gcp/blob/main/dagster/dg_pipes.py
"""

import random
import string
from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Optional, Sequence

import dagster._check as check
import google.cloud.storage
from dagster import PipesClient  # type: ignore
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesMessageReader,
    PipesParams,
)
from dagster._core.pipes.context import PipesMessageHandler
from dagster._core.pipes.utils import (
    PipesBlobStoreMessageReader,
    PipesEnvContextInjector,
    PipesLogReader,
    extract_message_or_forward_to_stdout,
    open_pipes_session,
)
from dagster_pipes import PipesDefaultMessageWriter
from dg_utils import get_execution_logs

# TODO: make this cloud-agnostic;


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
        client: google.cloud.storage.Client,
        log_readers: Optional[Sequence[PipesLogReader]] = None,
    ):
        super().__init__(
            interval=interval,
            log_readers=log_readers,
        )
        self.key_prefix: Optional[str] = None
        self.bucket = check.str_param(bucket, "bucket")
        self.client = client

    @contextmanager
    def get_params(self) -> Iterator[PipesParams]:
        key_prefix = "".join(random.choices(string.ascii_letters, k=30))  # nosec
        yield {"bucket": self.bucket, "key_prefix": key_prefix}

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
            f"Attempted to read messages from GCS bucket {self.bucket}. Expected"
            " PipesCloudStorageMessageReader to be explicitly passed to open_dagster_pipes in the external"
            " process."
        )


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
        function_url: str,
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
                url=function_url,
                data=payload_data,
            )

            context.log.debug(f"Response status code: {response.status_code}")
            if response.status_code != 200:
                context.log.error(
                    f"Failed to invoke cloud function {function_url}. Returned status code {response.status_code}."
                )
                context.log.error(response.text)
                raise ValueError(
                    f"Failed to invoke cloud function {function_url} with status code {response.status_code}"
                )

            context.log.info("Cloud function invoked successfully. Waiting for logs...")
            if isinstance(self._message_reader, PipesCloudLoggerMessageReader):
                trace_id = response.headers.get("X-Cloud-Trace-Context") or "local/0"
                trace_id = trace_id.split(";")[0]
                self._message_reader.consume_cloud_function_logs(trace_id)

        return PipesClientCompletedInvocation(session)
