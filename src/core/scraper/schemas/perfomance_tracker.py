import time
from dataclasses import dataclass, field


@dataclass
class PerformanceStats:
    start_time: float = field(default_factory=time.time)
    total_urls: int = 0
    extracted_successfully: int = 0
    validation_errors: int = 0  # JSON was valid, but Pydantic failed
    system_errors: int = 0  # Network, vLLM 500s, or crashes
    token_limit_errors: int = 0  # LengthFinishReason (Truncated)
    filtered_non_products: int = 0  # LLM correctly identified as "Not a Product"

    def report(self):
        duration = time.time() - self.start_time
        pps = self.total_urls / duration if duration > 0 else 0

        # Success = we got a structured product out
        success_rate = (
            (self.extracted_successfully / self.total_urls * 100)
            if self.total_urls > 0
            else 0
        )
        # Error Rate = we lost data we probably wanted
        error_rate = (
            ((self.validation_errors + self.system_errors) / self.total_urls * 100)
            if self.total_urls > 0
            else 0
        )

        print("\n" + "═" * 40)
        print("FINAL PERFORMANCE EVALUATION")
        print("═" * 40)
        print(f"Duration:           {duration:.2f}s")
        print(f"Throughput:         {pps:.2f} pages/sec")
        print(f"Success Rate:       {success_rate:.1f}%")
        print(f"Filtered (Non-Prd): {self.filtered_non_products}")
        print(f"System/Net Errors:  {self.system_errors}")
        print(f"Validation Fails:   {self.validation_errors}")
        print(f"Error Rate:         {error_rate:.1f}%")
        print(f"Truncated Tokens:   {self.token_limit_errors}")
        print("═" * 40 + "\n")
