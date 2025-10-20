import os
from typing import Optional
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

_client: Optional[Groq] = None


def get_client() -> Groq:
    """Get or create the Groq client instance.

    Lazy initialization ensures the client is only created when needed,
    preventing errors during test collection when API key is not set.

    Returns:
        Groq client instance

    Raises:
        GroqError: If GROQ_API_KEY environment variable is not set
    """
    global _client
    if _client is None:
        api_key = os.environ.get("GROQ_API_KEY")
        _client = Groq(api_key=api_key)
    return _client


# For backwards compatibility, expose as 'client'
# This will work in most cases, but won't fail on import
class _ClientProxy:
    """Proxy that provides lazy access to the Groq client."""

    def __getattr__(self, name):
        return getattr(get_client(), name)


client = _ClientProxy()
