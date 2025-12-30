import asyncio
from unittest.mock import MagicMock, patch

import aiohttp
import pytest
from aioresponses import aioresponses

from src.core.aws.spot.spot_termination_watcher import (
    check_spot_termination_notice,
    get_metadata_token,
    signal_handler,
    watch_spot_termination,
)


@pytest.fixture
def mock_aiohttp_session():
    """Fixture for mocking aiohttp client session."""
    with aioresponses() as m:
        yield m


@pytest.fixture
def shutdown_event():
    """Fixture for creating an asyncio Event."""
    return asyncio.Event()


def test_signal_handler():
    """
    Tests if the signal_handler correctly sets the shutdown event.
    """
    mock_event = MagicMock(spec=asyncio.Event)
    signal_handler(2, mock_event)
    mock_event.set.assert_called_once()


def test_signal_handler_no_event():
    """
    Tests if the signal_handler runs without error when the event is None.
    """
    signal_handler(2, None)


@pytest.mark.asyncio
async def testget_metadata_token_success(mock_aiohttp_session):
    """
    Tests successful retrieval of the EC2 metadata token.
    """
    token_url = "http://169.254.169.254/latest/api/token"
    with patch("os.getenv", return_value=token_url):
        mock_aiohttp_session.put(token_url, status=200, body="test-token")
        async with aiohttp.ClientSession() as session:
            token = await get_metadata_token(session)
            assert token == "test-token"


@pytest.mark.asyncio
async def testget_metadata_token_failure(mock_aiohttp_session):
    """
    Tests failure to retrieve the EC2 metadata token.
    """
    token_url = "http://169.254.169.254/latest/api/token"
    with patch("os.getenv", return_value=token_url):
        mock_aiohttp_session.put(token_url, status=400)
        async with aiohttp.ClientSession() as session:
            token = await get_metadata_token(session)
            assert token is None


@pytest.mark.asyncio
async def testget_metadata_token_exception(mock_aiohttp_session):
    """
    Tests exception handling during metadata token retrieval.
    """
    token_url = "http://169.254.169.254/latest/api/token"
    with patch("os.getenv", return_value=token_url):
        mock_aiohttp_session.put(token_url, exception=asyncio.TimeoutError())
        async with aiohttp.ClientSession() as session:
            token = await get_metadata_token(session)
            assert token is None


@pytest.mark.asyncio
async def testcheck_spot_termination_notice_found(mock_aiohttp_session, shutdown_event):
    """
    Tests that the shutdown event is set when a termination notice is found.
    """
    token_url = "http://token-url"
    metadata_url = "http://metadata-url"
    with patch("os.getenv", side_effect=[metadata_url, token_url]):
        mock_aiohttp_session.put(token_url, status=200, body="test-token")
        mock_aiohttp_session.get(
            metadata_url,
            status=200,
            payload={"action": "terminate", "time": "2023-12-15T10:45:00Z"},
        )

        async with aiohttp.ClientSession() as session:
            await check_spot_termination_notice(session, shutdown_event)
            assert shutdown_event.is_set()


@pytest.mark.asyncio
async def testcheck_spot_termination_notice_not_found(
    mock_aiohttp_session, shutdown_event
):
    """
    Tests that the shutdown event is not set when no termination notice is found.
    """
    token_url = "http://token-url"
    metadata_url = "http://metadata-url"
    with patch("os.getenv", side_effect=[token_url, metadata_url]):
        mock_aiohttp_session.put(token_url, status=200, body="test-token")
        mock_aiohttp_session.get(metadata_url, status=404)

        async with aiohttp.ClientSession() as session:
            await check_spot_termination_notice(session, shutdown_event)
            assert not shutdown_event.is_set()


@pytest.mark.asyncio
async def test_check_spot_termination_no_token(mock_aiohttp_session, shutdown_event):
    """
    Tests that the check is aborted if no metadata token is retrieved.
    """
    token_url = "http://token-url"
    metadata_url = "http://metadata-url"
    with patch("os.getenv", side_effect=[token_url, metadata_url]):
        mock_aiohttp_session.put(token_url, status=400)

        async with aiohttp.ClientSession() as session:
            await check_spot_termination_notice(session, shutdown_event)
            assert not shutdown_event.is_set()
            assert len(mock_aiohttp_session.requests) == 1  # Only token request


@pytest.mark.asyncio
async def test_check_spot_termination_invalid_json(
    mock_aiohttp_session, shutdown_event
):
    """
    Tests handling of invalid JSON in the metadata response.
    """
    token_url = "http://token-url"
    metadata_url = "http://metadata-url"
    with patch("os.getenv", side_effect=[token_url, metadata_url]):
        mock_aiohttp_session.put(token_url, status=200, body="test-token")
        mock_aiohttp_session.get(metadata_url, status=200, body="not-json")

        async with aiohttp.ClientSession() as session:
            await check_spot_termination_notice(session, shutdown_event)
            assert not shutdown_event.is_set()


@pytest.mark.asyncio
async def test_watch_spot_termination_stops_on_event(shutdown_event):
    """
    Tests that the watcher loop terminates when the shutdown event is set.
    """
    with patch(
        "src.core.aws.spot.spot_termination_watcher.check_spot_termination_notice"
    ) as mock_check:

        def side_effect(*args):
            """Sets the shutdown event after two calls."""
            if mock_check.call_count >= 2:
                event = next(
                    (arg for arg in args if isinstance(arg, asyncio.Event)), None
                )
                if event:
                    event.set()

        mock_check.side_effect = side_effect

        try:
            await asyncio.wait_for(
                watch_spot_termination(shutdown_event, 0.1), timeout=0.5
            )
        except asyncio.TimeoutError:
            pytest.fail("Watcher did not terminate as expected.")

        assert mock_check.call_count >= 2
        assert shutdown_event.is_set()


@pytest.mark.asyncio
async def test_watch_spot_termination_cancelled():
    """
    Tests that the watcher correctly handles cancellation.
    """
    shutdown_event = asyncio.Event()
    task = asyncio.create_task(watch_spot_termination(shutdown_event, 0.1))

    await asyncio.sleep(0.05)  # Let it run a few times
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert not shutdown_event.is_set()
