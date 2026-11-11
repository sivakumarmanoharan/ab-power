"""AESOClient: retries, failure handling and request building, with HTTP mocked."""

from datetime import date
import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

pytest = importlib.import_module("pytest")
requests = importlib.import_module("requests")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import via importlib so the runtime path update is explicit and static analyzers
# do not flag the project root as an unresolved module.
aeso_api_client = importlib.import_module("aeso_api_client")
AESOClient = aeso_api_client.AESOClient


def response(status, payload=None, text=""):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload or {}
    r.text = text
    return r


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Backoff waits would make tests slow; record them instead."""
    waits = []
    monkeypatch.setattr(aeso_api_client.time, "sleep", waits.append)
    return waits


@pytest.fixture
def client():
    return AESOClient("test-key", max_retries=3)


def test_missing_api_key_is_rejected():
    with pytest.raises(ValueError):
        AESOClient("")


def test_api_key_sent_as_header(client):
    assert client.session.headers["API-KEY"] == "test-key"


def test_success_returns_json(client):
    client.session.get = MagicMock(return_value=response(200, {"return": []}))
    assert client.fetch("assets") == {"return": []}


def test_rate_limit_is_retried_then_succeeds(client, no_sleep):
    client.session.get = MagicMock(side_effect=[response(429), response(200, {"ok": True})])
    assert client.fetch("assets") == {"ok": True}
    assert client.session.get.call_count == 2
    assert no_sleep == [2]  # exponential backoff: 2s before the second attempt


def test_server_errors_exhaust_retries(client):
    client.session.get = MagicMock(return_value=response(503, text="unavailable"))
    with pytest.raises(RuntimeError, match="503"):
        client.fetch("assets")
    assert client.session.get.call_count == 3


def test_client_error_fails_immediately_without_retry(client):
    """A 404 or 401 will not fix itself: fail on the first attempt."""
    client.session.get = MagicMock(return_value=response(404, text="not found"))
    with pytest.raises(RuntimeError, match="404"):
        client.fetch("assets")
    assert client.session.get.call_count == 1


def test_network_error_is_retried(client):
    client.session.get = MagicMock(
        side_effect=[requests.ConnectionError("down"), response(200, {"ok": True})]
    )
    assert client.fetch("assets") == {"ok": True}


def test_range_feed_sends_start_and_end_dates(client):
    client.session.get = MagicMock(return_value=response(200, {}))
    client.fetch("pool_price", date(2026, 11, 1), date(2026, 11, 2))
    params = client.session.get.call_args.kwargs["params"]
    assert params == {"startDate": "2026-11-01", "endDate": "2026-11-02"}


def test_range_feed_requires_start_date(client):
    with pytest.raises(ValueError):
        client.fetch("pool_price")


def test_snapshot_feed_sends_no_date_params(client):
    """'current' feeds must not return None or send dates (the bug fixed in review)."""
    client.session.get = MagicMock(return_value=response(200, {"return": []}))
    assert client.fetch("generation") is not None
    assert client.session.get.call_args.kwargs["params"] is None


def test_unknown_dataset_is_rejected(client):
    with pytest.raises(KeyError):
        client.fetch("not_a_feed")