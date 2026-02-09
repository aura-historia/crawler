import time
from dataclasses import dataclass, field


@dataclass
class PerformanceStats:
    start_time: float = field(default_factory=time.time)
    total_urls: int = 0
    processed_urls: int = 0
    domains_processed: str = ""
    extracted_successfully: int = 0
    unchanged_urls: int = 0  # URLs that were skipped because they were unchanged
    validation_errors: int = 0  # JSON was valid, but Pydantic failed
    system_errors: int = 0  # Network, vLLM 500s, or crashes
    token_limit_errors: int = 0  # LengthFinishReason (Truncated)
    filtered_non_products: int = 0  # LLM correctly identified as "Not a Product"

    def success_rate(self, n_urls) -> float:
        if self.extracted_successfully == 0 or n_urls == 0:
            return 0.0

        return (self.extracted_successfully / n_urls) * 100

    def duration(self) -> float:
        return time.time() - self.start_time

    def pps(self, n_urls) -> float:
        return n_urls / self.duration()

    def error_rate(self, n_urls) -> float:
        sum_errors = self.validation_errors + self.system_errors
        return (sum_errors / n_urls) * 100

    def progress(self) -> float:
        return self.processed_urls / self.total_urls * 100

    def report(self, type="current"):
        if type == "current":
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
        print(f"Duration:             {self.duration():.2f}s")
        print(f"Throughput:           {self.pps(total_urls):.2f} pages/sec")
        print(f"Success:              {self.extracted_successfully}")
        print(f"Success Rate:         {self.success_rate(total_urls):.1f}%")
        print(f"Filtered (Non-Prd):   {self.filtered_non_products}")
        print(f"Skipped (No Change):  {self.unchanged_urls}")
        print(f"System/Net Errors:    {self.system_errors}")
        print(f"Validation Fails:     {self.validation_errors}")
        print(f"Error Rate:           {self.error_rate(total_urls):.1f}%")
        print(f"Truncated Tokens:     {self.token_limit_errors}")
        print("═" * 40 + "\n")
