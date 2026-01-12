from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.core.worker.base_worker import (
    handle_stolen_message,
    generic_worker,
    run_worker_pool,
)


class TestHandleStolenMessage:
    """Tests for handle_stolen_message function."""

    @pytest.mark.asyncio
    async def test_handle_stolen_message_with_completed_fetch(self):
        """Test handling a message when fetch task completed successfully."""
        worker_id = 1
        message = Mock()
        message.change_visibility = Mock()

        fetch_task = asyncio.create_task(asyncio.sleep(0))
        await fetch_task

        # Mock fetch_task.result() to return messages
        fetch_task.result = Mock(return_value=[message])

        with patch(
            "src.core.worker.base_worker.asyncio.to_thread", new_callable=AsyncMock
        ) as mock_thread:
            mock_thread.return_value = None

            await handle_stolen_message(worker_id, fetch_task)

            mock_thread.assert_called_once()
            call_args = mock_thread.call_args
            assert call_args[0][0] == message.change_visibility
            assert call_args[1]["VisibilityTimeout"] == 120

    @pytest.mark.asyncio
    async def test_handle_stolen_message_with_no_messages(self):
        """Test handling when fetch task returns empty list."""
        worker_id = 1

        fetch_task = asyncio.create_task(asyncio.sleep(0))
        await fetch_task

        # Mock fetch_task.result() to return empty list
        fetch_task.result = Mock(return_value=[])

        with patch(
            "src.core.worker.base_worker.asyncio.to_thread", new_callable=AsyncMock
        ) as mock_thread:
            await handle_stolen_message(worker_id, fetch_task)

            mock_thread.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_stolen_message_with_cancelled_task(self):  # NOSONAR
        """Test handling a canceled fetch task."""
        worker_id = 1

        fetch_task = asyncio.create_task(asyncio.sleep(0))
        fetch_task.cancel()

        try:
            await fetch_task
        except asyncio.CancelledError:  # NOSONAR
            pass

        with patch(
            "src.core.worker.base_worker.asyncio.to_thread", new_callable=AsyncMock
        ) as mock_thread:
            await handle_stolen_message(worker_id, fetch_task)

            mock_thread.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_stolen_message_with_exception(self):
        """Test handling when message release fails."""
        worker_id = 1
        message = Mock()
        message.change_visibility = Mock()

        fetch_task = asyncio.create_task(asyncio.sleep(0))
        await fetch_task

        fetch_task.result = Mock(return_value=[message])

        with patch(
            "src.core.worker.base_worker.asyncio.to_thread", new_callable=AsyncMock
        ) as mock_thread:
            mock_thread.side_effect = Exception("Release failed")

            # Should not raise exception, just log warning
            await handle_stolen_message(worker_id, fetch_task)

            mock_thread.assert_called_once()


class TestGenericWorker:
    """Tests for generic_worker function."""

    @pytest.mark.asyncio
    async def test_generic_worker_processes_message(self):
        """Test that worker successfully processes a message."""
        worker_id = 1
        queue = Mock()
        shutdown_event = asyncio.Event()
        message = Mock()
        handler_called = []

        async def message_handler(msg):  # NOSONAR
            handler_called.append(msg)
            shutdown_event.set()

        with patch("src.core.worker.base_worker.receive_messages") as mock_receive:
            mock_receive.return_value = [message]

            await generic_worker(
                worker_id=worker_id,
                queue=queue,
                shutdown_event=shutdown_event,
                message_handler=message_handler,
                max_messages=1,
                wait_time=20,
            )

            assert len(handler_called) == 1
            assert handler_called[0] == message
            mock_receive.assert_called_once_with(queue, 1, 20)

    @pytest.mark.asyncio
    async def test_generic_worker_handles_empty_queue(self):
        """Test worker behavior when queue returns no messages."""
        worker_id = 1
        queue = Mock()
        shutdown_event = asyncio.Event()
        handler = AsyncMock()

        poll_count = [0]

        def mock_receive(*args, **kwargs):
            poll_count[0] += 1
            if poll_count[0] >= 2:
                shutdown_event.set()
            return []

        with patch(
            "src.core.worker.base_worker.receive_messages", side_effect=mock_receive
        ):
            await generic_worker(
                worker_id=worker_id,
                queue=queue,
                shutdown_event=shutdown_event,
                message_handler=handler,
                max_messages=1,
                wait_time=20,
            )

            handler.assert_not_called()
            assert poll_count[0] >= 2

    @pytest.mark.asyncio
    async def test_generic_worker_graceful_shutdown_while_waiting(self):
        """Test worker shuts down gracefully when signal received while waiting."""
        worker_id = 1
        queue = Mock()
        shutdown_event = asyncio.Event()
        handler = AsyncMock()

        async def delayed_shutdown():
            await asyncio.sleep(0.1)
            shutdown_event.set()

        async def slow_receive(*args, **kwargs):
            await asyncio.sleep(1)
            return []

        shutdown_task = asyncio.create_task(delayed_shutdown())

        with patch(
            "src.core.worker.base_worker.receive_messages", side_effect=slow_receive
        ):
            await generic_worker(
                worker_id=worker_id,
                queue=queue,
                shutdown_event=shutdown_event,
                message_handler=handler,
                max_messages=1,
                wait_time=20,
            )

            await shutdown_task
            handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_generic_worker_with_shutdown_after_message(self):
        """Test worker processes message then exits gracefully on shutdown."""
        worker_id = 1
        queue = Mock()
        shutdown_event = asyncio.Event()
        handler = AsyncMock()
        message = Mock()

        receive_count = [0]

        def mock_receive(*args, **kwargs):
            receive_count[0] += 1
            if receive_count[0] == 1:
                return [message]
            # After first message, trigger shutdown
            shutdown_event.set()
            return []

        with patch(
            "src.core.worker.base_worker.receive_messages", side_effect=mock_receive
        ):
            await generic_worker(
                worker_id=worker_id,
                queue=queue,
                shutdown_event=shutdown_event,
                message_handler=handler,
                max_messages=1,
                wait_time=20,
            )

            # Worker should process the message and then exit
            handler.assert_called_once_with(message)
            assert receive_count[0] >= 1

    @pytest.mark.asyncio
    async def test_generic_worker_handles_message_handler_exception(self):
        """Test worker continues after message handler raises exception."""
        worker_id = 1
        queue = Mock()
        shutdown_event = asyncio.Event()
        messages_processed = []

        async def failing_handler(msg):  # NOSONAR
            messages_processed.append(msg)
            if len(messages_processed) == 1:
                raise RuntimeError("Handler failed")
            shutdown_event.set()

        message1 = Mock()
        message1.body = "message1"
        message2 = Mock()
        message2.body = "message2"

        receive_calls = [0]

        def mock_receive(*args, **kwargs):
            receive_calls[0] += 1
            if receive_calls[0] == 1:
                return [message1]
            elif receive_calls[0] == 2:
                return [message2]
            return []

        with patch(
            "src.core.worker.base_worker.receive_messages", side_effect=mock_receive
        ):
            await generic_worker(
                worker_id=worker_id,
                queue=queue,
                shutdown_event=shutdown_event,
                message_handler=failing_handler,
                max_messages=1,
                wait_time=20,
            )

            assert len(messages_processed) == 2

    @pytest.mark.asyncio
    async def test_generic_worker_with_custom_params(self):
        """Test worker with custom max_messages and wait_time parameters."""
        worker_id = 42
        queue = Mock()
        shutdown_event = asyncio.Event()
        message = Mock()
        handler = AsyncMock()

        async def handler_with_shutdown(msg):
            await handler(msg)
            shutdown_event.set()

        with patch("src.core.worker.base_worker.receive_messages") as mock_receive:
            mock_receive.return_value = [message]

            await generic_worker(
                worker_id=worker_id,
                queue=queue,
                shutdown_event=shutdown_event,
                message_handler=handler_with_shutdown,
                max_messages=5,
                wait_time=10,
            )

            handler.assert_called_once_with(message)
            mock_receive.assert_called_once_with(queue, 5, 10)


class TestRunWorkerPool:
    """Tests for run_worker_pool function."""

    @pytest.mark.asyncio
    async def test_run_worker_pool_basic_execution(self):
        """Test worker pool starts and stops workers correctly."""
        shutdown_event = asyncio.Event()
        worker_calls = []

        async def mock_worker_factory(worker_id: int):
            worker_calls.append(worker_id)
            await asyncio.sleep(0.1)

        async def trigger_shutdown():
            await asyncio.sleep(0.05)
            shutdown_event.set()

        shutdown_task = asyncio.create_task(trigger_shutdown())

        await run_worker_pool(
            n_workers=3,
            shutdown_event=shutdown_event,
            worker_factory=mock_worker_factory,
            shutdown_timeout=5.0,
        )

        await shutdown_task

        assert len(worker_calls) == 3
        assert worker_calls == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_run_worker_pool_graceful_shutdown(self):
        """Test worker pool waits for workers to finish gracefully."""
        shutdown_event = asyncio.Event()
        worker_finished = []

        async def mock_worker_factory(worker_id: int):
            await shutdown_event.wait()
            await asyncio.sleep(0.1)
            worker_finished.append(worker_id)

        async def trigger_shutdown():
            await asyncio.sleep(0.05)
            shutdown_event.set()

        shutdown_task = asyncio.create_task(trigger_shutdown())

        await run_worker_pool(
            n_workers=2,
            shutdown_event=shutdown_event,
            worker_factory=mock_worker_factory,
            shutdown_timeout=5.0,
        )

        await shutdown_task

        assert len(worker_finished) == 2
        assert set(worker_finished) == {0, 1}

    @pytest.mark.asyncio
    async def test_run_worker_pool_timeout_cancellation(self):
        """Test worker pool cancels workers on timeout."""
        shutdown_event = asyncio.Event()
        worker_cancelled = []

        async def slow_worker_factory(worker_id: int):
            try:
                await shutdown_event.wait()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                worker_cancelled.append(worker_id)
                raise

        async def trigger_shutdown():
            await asyncio.sleep(0.05)
            shutdown_event.set()

        shutdown_task = asyncio.create_task(trigger_shutdown())

        await run_worker_pool(
            n_workers=2,
            shutdown_event=shutdown_event,
            worker_factory=slow_worker_factory,
            shutdown_timeout=0.2,
        )

        await shutdown_task

        assert len(worker_cancelled) == 2
        assert set(worker_cancelled) == {0, 1}

    @pytest.mark.asyncio
    async def test_run_worker_pool_worker_exception(self):
        """Test worker pool handles worker exceptions gracefully."""
        shutdown_event = asyncio.Event()
        worker_calls = []

        async def failing_worker_factory(worker_id: int):
            worker_calls.append(worker_id)
            if worker_id == 1:
                raise RuntimeError(f"Worker {worker_id} failed")
            await shutdown_event.wait()

        async def trigger_shutdown():
            await asyncio.sleep(0.1)
            shutdown_event.set()

        shutdown_task = asyncio.create_task(trigger_shutdown())

        await run_worker_pool(
            n_workers=3,
            shutdown_event=shutdown_event,
            worker_factory=failing_worker_factory,
            shutdown_timeout=5.0,
        )

        await shutdown_task

        assert len(worker_calls) == 3

    @pytest.mark.asyncio
    async def test_run_worker_pool_signal_handler_setup(self):
        """Test worker pool sets up signal handlers correctly."""
        shutdown_event = asyncio.Event()

        async def mock_worker_factory(worker_id: int):
            await asyncio.sleep(0.05)

        async def trigger_shutdown():
            await asyncio.sleep(0.02)
            shutdown_event.set()

        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop_instance = Mock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.add_signal_handler = Mock()

            shutdown_task = asyncio.create_task(trigger_shutdown())

            await run_worker_pool(
                n_workers=1,
                shutdown_event=shutdown_event,
                worker_factory=mock_worker_factory,
                shutdown_timeout=5.0,
            )

            await shutdown_task

            # Verify signal handlers were added
            assert mock_loop_instance.add_signal_handler.call_count == 2

    @pytest.mark.asyncio
    async def test_run_worker_pool_windows_signal_fallback(self):
        """Test worker pool handles Windows signal setup fallback."""
        shutdown_event = asyncio.Event()

        async def mock_worker_factory(worker_id: int):
            await asyncio.sleep(0.05)

        async def trigger_shutdown():
            await asyncio.sleep(0.02)
            shutdown_event.set()

        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop_instance = Mock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.add_signal_handler = Mock(
                side_effect=NotImplementedError("Windows")
            )

            with patch("signal.signal") as mock_signal:
                shutdown_task = asyncio.create_task(trigger_shutdown())

                await run_worker_pool(
                    n_workers=1,
                    shutdown_event=shutdown_event,
                    worker_factory=mock_worker_factory,
                    shutdown_timeout=5.0,
                )

                await shutdown_task

                # Verify fallback to signal.signal was used
                assert mock_signal.call_count == 2

    @pytest.mark.asyncio
    async def test_run_worker_pool_gather_exception_during_drain(self):
        """Test worker pool handles exceptions during worker drain."""
        shutdown_event = asyncio.Event()

        async def mock_worker_factory(worker_id: int):
            await shutdown_event.wait()
            if worker_id == 0:
                raise ValueError("Unexpected error during drain")

        async def trigger_shutdown():
            await asyncio.sleep(0.05)
            shutdown_event.set()

        shutdown_task = asyncio.create_task(trigger_shutdown())

        # Should not raise, just log error
        await run_worker_pool(
            n_workers=2,
            shutdown_event=shutdown_event,
            worker_factory=mock_worker_factory,
            shutdown_timeout=5.0,
        )

        await shutdown_task
