import time
from dataclasses import dataclass, field


@dataclass
class PerformanceStats:
    start_time: float = field(default_factory=time.time)
    total_urls: int = 0
    processed_urls: int = 0
    domains_processed: str = ""
    extracted_successfully: int = 0
    n_unchanged_urls: int = 0  # URLs that were skipped because they were unchanged
    validation_errors: int = 0  # JSON was valid, but Pydantic failed
    system_errors: int = 0  # Network, vLLM 500s, or crashes
    token_limit_errors: int = 0  # LengthFinishReason (Truncated)
    filtered_non_products: int = 0  # LLM correctly identified as "Not a Product"

    def duration_seconds(self) -> float:
        """Return duration since start in seconds."""
        return time.time() - self.start_time

    def duration_minutes(self) -> float:
        """Return duration since start in minutes."""
        return self.duration_seconds() / 60

    def safe_divide(self, numerator: float, denominator: float) -> float:
        return numerator / denominator if denominator else 0.0

    def success_rate(self, n_urls: int) -> float:
        return (
            self.safe_divide(
                self.extracted_successfully, n_urls - self.n_unchanged_urls
            )
            * 100
        )

    def error_rate(self, n_urls: int) -> float:
        sum_errors = self.validation_errors + self.system_errors
        return self.safe_divide(sum_errors, n_urls - self.n_unchanged_urls) * 100

    def time_per_page(self, n_urls: int) -> float:
        processed = n_urls - self.n_unchanged_urls
        return self.safe_divide(self.duration_seconds(), processed)

    def progress(self) -> float:
        return (
            self.safe_divide(
                self.processed_urls - self.n_unchanged_urls, self.total_urls
            )
            * 100
        )

    def report(self, mode="current"):
        if mode == "current":
            total_urls = self.processed_urls
            title = "CURRENT PERFORMANCE EVALUATION"
        else:
            total_urls = self.total_urls
            title = "FINAL PERFORMANCE EVALUATION"

        print("\n" + "═" * 40)
        print(title)
        print("═" * 40)
        print(f"Domains Processed:    {self.domains_processed}")
        print(f"Total URLs Number:    {total_urls}")
        print(f"Progress:             {self.progress():.1f}%")
        print(f"Duration:             {self.duration_minutes():.2f} min")
        print(f"Throughput:           {self.time_per_page(total_urls):.2f} sec/page")
        print(f"Success:              {self.extracted_successfully}")
        print(f"Success Rate:         {self.success_rate(total_urls):.1f}%")
        print(f"Filtered (Non-Prd):   {self.filtered_non_products}")
        print(f"Skipped (No Change):  {self.n_unchanged_urls}")
        print(f"System/Net Errors:    {self.system_errors}")
        print(f"Validation Fails:     {self.validation_errors}")
        print(f"Error Rate:           {self.error_rate(total_urls):.1f}%")
        print(f"Truncated Tokens:     {self.token_limit_errors}")
        print("═" * 40 + "\n")
