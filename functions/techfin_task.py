from urllib3.util.retry import Retry
import requests
import os
from requests.adapters import HTTPAdapter

def retry_session(retries=5, session=None, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504, 524),
                   method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE'])):

    """
    Static method used to handle retries between calls.

    Args:

        retries: `int` , default `5`
            Number of retries for the API calls
        session: Session object dealt `None`
            It allows you to persist certain parameters across requests.
        backoff_factor: `float` , default `0.5`
            Backoff factor to apply between  attempts. It will sleep for:
                    {backoff factor} * (2 ^ ({retries} - 1)) seconds
        status_forcelist: `iterable` , default (500, 502, 503, 504, 524).
            A set of integer HTTP status codes that we should force a retry on.
            A retry is initiated if the request method is in method_whitelist and the response status code is in
            status_forcelist.
        method_whitelist: `iterable` , default frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE']))
            Set of uppercased HTTP method verbs that we should retry on.

    Returns:
        :class:`requests.Section`
    """

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        method_whitelist=method_whitelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def add_pubsub(tenant):

    tenant = tenant[6:]
    uuid_tenant = tenant[:8] + '-' + tenant[8:12] + '-' + tenant[12:16] + '-' + tenant[16:20] + '-' + tenant[20:]
    bearer_token = os.environ['TOKEN_TECHFIN']
    api = 'https://cashflow.totvs.app/carol-sync/api/v1/subscription/subscribe'
    header = {"Authorization": f"Bearer {bearer_token}", 'accept': '/' ,"content-type": 'application/json'}
    payload = {"tenantIds": [uuid_tenant], "defaultMaxInFlight": 1,"defaultMaxBatchSize": 100,
               "defaultStartAt": 0, "clearDelayedSubscriptions":True, "pause":False}

    session = retry_session(method_whitelist=frozenset(['POST']), status_forcelist=frozenset([504]),)
    r = session.post(url=api, json=payload, headers=header, )

    return r
