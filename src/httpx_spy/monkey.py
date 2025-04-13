"""
Things that we use to monkey-patch httpx
"""

import asyncio

import httpx


class MonkeyClient(httpx.Client.__base__):
    """
    Monkey-patched intermediate base class for Client and AsyncClient
    """

    def __getattribute__(self, item):
        """
        We force-inject our hooks into the _event_hooks of httpx
        """

        out = super().__getattribute__(item)

        if item == "_event_hooks" and not getattr(self, "_no_monkey", False):
            if hasattr(self, "_processor"):
                processor = self._processor
            else:
                from .processor import get_processor

                processor = get_processor()

            is_async = asyncio.iscoroutinefunction(self.get)

            out["request"].append(
                processor.async_handle_request
                if is_async
                else processor.sync_handle_request
            )
            out["response"].append(
                processor.sync_handle_response
                if is_async
                else processor.async_handle_response
            )

        return out
