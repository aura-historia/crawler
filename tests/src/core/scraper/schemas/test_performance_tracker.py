import time
import pytest

from core.scraper.schemas.perfomance_tracker import PerformanceStats


class TestPerformanceStatsDuration:
    """Tests for duration calculation methods."""

    def test_duration_seconds_returns_elapsed_time(self):
        """Verify duration_seconds returns correct elapsed time."""
        stats = PerformanceStats()
        time.sleep(0.1)

        duration = stats.duration_seconds()

        assert duration >= 0.1
        assert duration < 0.5  # Should be fast

    def test_duration_minutes_converts_correctly(self):
        """Verify duration_minutes converts seconds to minutes."""
        stats = PerformanceStats()
        # Mock start_time to be 120 seconds ago
        stats.start_time = time.time() - 120

        duration = stats.duration_minutes()

        assert 1.9 < duration < 2.1  # ~2 minutes


class TestPerformanceStatsSafeDivide:
    """Tests for safe_divide helper method."""

    def test_safe_divide_normal_division(self):
        """Verify safe_divide performs normal division."""
        stats = PerformanceStats()

        result = stats.safe_divide(10, 2)

        assert result == pytest.approx(5.0)

    def test_safe_divide_by_zero_returns_zero(self):
        """Verify safe_divide returns 0.0 when denominator is zero."""
        stats = PerformanceStats()

        result = stats.safe_divide(10, 0)

        assert result == pytest.approx(0.0)

    def test_safe_divide_with_floats(self):
        """Verify safe_divide works with float arguments."""
        stats = PerformanceStats()

        result = stats.safe_divide(5.5, 2.0)

        assert result == pytest.approx(2.75)


class TestPerformanceStatsRates:
    """Tests for success_rate, error_rate, and related calculations."""

    def test_success_rate_calculation(self):
        """Verify success_rate calculates percentage correctly."""
        stats = PerformanceStats(
            extracted_successfully=80,
            n_unchanged_urls=20,
        )

        # (80 / (100 - 20)) * 100 = 100%
        rate = stats.success_rate(n_urls=100)

        assert rate == pytest.approx(100.0)

    def test_success_rate_with_partial_success(self):
        """Verify success_rate with partial success."""
        stats = PerformanceStats(
            extracted_successfully=40,
            n_unchanged_urls=0,
        )

        # (40 / 100) * 100 = 40%
        rate = stats.success_rate(n_urls=100)

        assert rate == pytest.approx(40.0)

    def test_success_rate_zero_processed_returns_zero(self):
        """Verify success_rate returns 0 when no URLs processed."""
        stats = PerformanceStats(
            extracted_successfully=0,
            n_unchanged_urls=100,  # All skipped
        )

        rate = stats.success_rate(n_urls=100)

        assert rate == pytest.approx(0.0)

    def test_error_rate_calculation(self):
        """Verify error_rate sums validation and system errors."""
        stats = PerformanceStats(
            validation_errors=10,
            system_errors=5,
            n_unchanged_urls=0,
        )

        # ((10 + 5) / 100) * 100 = 15%
        rate = stats.error_rate(n_urls=100)

        assert rate == pytest.approx(15.0)

    def test_error_rate_excludes_unchanged(self):
        """Verify error_rate excludes unchanged URLs from denominator."""
        stats = PerformanceStats(
            validation_errors=8,
            system_errors=2,
            n_unchanged_urls=50,
        )

        # ((8 + 2) / (100 - 50)) * 100 = 20%
        rate = stats.error_rate(n_urls=100)

        assert rate == pytest.approx(20.0)


class TestPerformanceStatsTimePerPage:
    """Tests for time_per_page calculation."""

    def test_time_per_page_calculation(self):
        """Verify time_per_page returns seconds per processed URL."""
        stats = PerformanceStats(n_unchanged_urls=0)
        stats.start_time = time.time() - 100  # 100 seconds ago

        time_pp = stats.time_per_page(n_urls=50)

        assert 1.9 < time_pp < 2.1  # ~2 seconds per page

    def test_time_per_page_excludes_unchanged(self):
        """Verify time_per_page excludes unchanged URLs."""
        stats = PerformanceStats(n_unchanged_urls=25)
        stats.start_time = time.time() - 100  # 100 seconds ago

        time_pp = stats.time_per_page(n_urls=50)

        # 100 / (50 - 25) = 4 seconds per page
        assert 3.9 < time_pp < 4.1

    def test_time_per_page_all_unchanged_returns_zero(self):
        """Verify time_per_page returns 0 when all URLs unchanged."""
        stats = PerformanceStats(n_unchanged_urls=100)

        time_pp = stats.time_per_page(n_urls=100)

        assert time_pp == pytest.approx(0.0)


class TestPerformanceStatsProgress:
    """Tests for progress calculation."""

    def test_progress_calculation(self):
        """Verify progress calculates correct percentage."""
        stats = PerformanceStats(
            total_urls=200,
            processed_urls=100,
            n_unchanged_urls=0,
        )

        progress = stats.progress()

        assert progress == pytest.approx(50.0)

    def test_progress_excludes_unchanged(self):
        """Verify progress excludes unchanged from numerator."""
        stats = PerformanceStats(
            total_urls=100,
            processed_urls=50,
            n_unchanged_urls=25,
        )

        # ((50 - 25) / 100) * 100 = 25%
        progress = stats.progress()

        assert progress == pytest.approx(25.0)

    def test_progress_zero_total_returns_zero(self):
        """Verify progress returns 0 when total_urls is zero."""
        stats = PerformanceStats(
            total_urls=0,
            processed_urls=0,
        )

        progress = stats.progress()

        assert progress == pytest.approx(0.0)


class TestPerformanceStatsReport:
    """Tests for report output."""

    def test_report_current_mode_uses_processed_urls(self, capsys):
        """Verify report with 'current' mode uses processed_urls."""
        stats = PerformanceStats(
            total_urls=200,
            processed_urls=50,
            domains_processed="test.com",
            extracted_successfully=40,
            n_unchanged_urls=5,
            validation_errors=3,
            system_errors=2,
            token_limit_errors=1,
            filtered_non_products=4,
        )
        stats.start_time = time.time() - 60  # 1 minute ago

        stats.report(mode="current")

        captured = capsys.readouterr()
        assert "CURRENT PERFORMANCE EVALUATION" in captured.out
        assert "Domains Processed:    test.com" in captured.out
        assert "Total URLs Number:    50" in captured.out
        assert "Success:" in captured.out
        assert "Validation Fails:" in captured.out

    def test_report_final_mode_uses_total_urls(self, capsys):
        """Verify report with 'final' mode uses total_urls."""
        stats = PerformanceStats(
            total_urls=200,
            processed_urls=200,
            domains_processed="final.com",
            extracted_successfully=180,
        )

        stats.report(mode="final")

        captured = capsys.readouterr()
        assert "FINAL PERFORMANCE EVALUATION" in captured.out
        assert "Total URLs Number:    200" in captured.out

    def test_report_prints_all_fields(self, capsys):
        """Verify report includes all expected fields."""
        stats = PerformanceStats(
            domains_processed="all-fields.com",
            extracted_successfully=10,
            filtered_non_products=5,
            n_unchanged_urls=3,
            system_errors=2,
            validation_errors=1,
            token_limit_errors=1,
        )

        stats.report(mode="current")

        captured = capsys.readouterr()
        assert "Progress:" in captured.out
        assert "Duration:" in captured.out
        assert "Throughput:" in captured.out
        assert "Success:" in captured.out
        assert "Success Rate:" in captured.out
        assert "Filtered (Non-Prd):" in captured.out
        assert "Skipped (No Change):" in captured.out
        assert "System/Net Errors:" in captured.out
        assert "Validation Fails:" in captured.out
        assert "Error Rate:" in captured.out
        assert "Truncated Tokens:" in captured.out


class TestPerformanceStatsDataclass:
    """Tests for dataclass initialization and defaults."""

    def test_default_initialization(self):
        """Verify default values are set correctly."""
        stats = PerformanceStats()

        assert stats.total_urls == 0
        assert stats.processed_urls == 0
        assert stats.domains_processed == ""
        assert stats.extracted_successfully == 0
        assert stats.n_unchanged_urls == 0
        assert stats.validation_errors == 0
        assert stats.system_errors == 0
        assert stats.token_limit_errors == 0
        assert stats.filtered_non_products == 0
        assert isinstance(stats.start_time, float)

    def test_custom_initialization(self):
        """Verify custom values are set correctly."""
        stats = PerformanceStats(
            total_urls=100,
            processed_urls=50,
            domains_processed="custom.com",
            extracted_successfully=45,
            n_unchanged_urls=5,
            validation_errors=2,
            system_errors=1,
            token_limit_errors=1,
            filtered_non_products=3,
        )

        assert stats.total_urls == 100
        assert stats.processed_urls == 50
        assert stats.domains_processed == "custom.com"
        assert stats.extracted_successfully == 45
        assert stats.n_unchanged_urls == 5
        assert stats.validation_errors == 2
        assert stats.system_errors == 1
        assert stats.token_limit_errors == 1
        assert stats.filtered_non_products == 3
