import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


def create_resilient_session():
    session = requests.Session()

    retries = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"],
        raise_on_status=True,
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    return session


http_session = create_resilient_session()
