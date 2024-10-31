import json
import logging
import typing

import google.cloud.logging
import google.oauth2.id_token
import google.oauth2.service_account
import httpx
import requests

# import tenacity
from cachetools import TTLCache, cached
from google.auth.transport.requests import AuthorizedSession, Request

logger = logging.getLogger("dagster.utils")


class NotAllLogsReceivedException(Exception): ...  # noqa: E701


class NoLogsException(Exception): ...  # noqa: E701


@cached(cache=TTLCache(ttl=30 * 60, maxsize=100))
def _get_id_token(url: str) -> str:
    auth_req = Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
    return id_token


def _with_id_token(url: str, timeout: int, data: str) -> httpx.Response:
    """Use credentials set on GOOGLE_APPLICATION_CREDENTIALS environment variable to invoke google cloud function"""
    id_token = _get_id_token(url)
    headers = {
        "Authorization": "bearer " + id_token,
        "Content-Type": "application/json",
    }
    resp = httpx.post(url=url, headers=headers, timeout=timeout, data=data)
    return resp


def _with_service_account(
    url: str, service_account_file: str, timeout: int, data: str
) -> requests.models.Response:
    """Use service account credentials from a specific file to invoke the cloud function"""
    creds = google.oauth2.service_account.IDTokenCredentials.from_service_account_file(
        service_account_file, target_audience=url
    )
    authed_session = AuthorizedSession(creds)
    return authed_session.get(url, timeout=timeout, data=data)


def invoke_cloud_function(
    url: str,
    data: typing.Mapping[str, typing.Any],
    service_account_file: typing.Optional[str] = None,
    timeout: int = 120,
) -> typing.Union[httpx.Response, requests.models.Response]:
    """Invoke a Google Cloud Function"""
    _data = json.dumps(data)
    if service_account_file is not None:
        resp = _with_service_account(url, service_account_file, timeout, _data)
    else:  # Fallback. Use GOOGLE_APPLICATION_CREDENTIALS
        resp = _with_id_token(url, timeout, _data)
    return resp


# @tenacity.retry(
#     stop=tenacity.stop_after_attempt(5), wait=tenacity.wait_exponential_jitter(initial=1)
# )
def get_execution_logs(trace_id: str):
    """Lists the most recent entries for a given logger."""
    logging_client = google.cloud.logging.Client()

    out = []
    for entry in logging_client.list_entries(
        filter_=f'jsonPayload.trace="projects/eo-dev-438207/traces/{trace_id}" AND logName="projects/eo-dev-438207/logs/dagster-pipes-utils"'
    ):
        if entry.payload is not None:
            if entry.payload.get("message") is not None:
                out.append(entry.payload["message"])
    if len(out) == 0:
        raise NoLogsException(f"No logs found for trace id={trace_id}")
    elif json.loads(out[-1])["method"] != "closed":
        raise NotAllLogsReceivedException(
            f"Logs not yet complete for trace id={trace_id}"
        )
    else:
        return out
