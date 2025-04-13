"""
Things that we use to monkey-patch httpx
"""

import asyncio

import httpx


class MonkeyClient(httpx.Client.__base__):
    """
    Monkey-patched intermediate base class for Client and AsyncClient
    """

    def __init__(self, *args, **kwargs):
        if "_no_hook" in kwargs:
            del kwargs["_no_hook"]
            no_hook = True
        else:
            no_hook = False

        if "_processor" in kwargs:
            processor = kwargs.pop("processor")
        else:
            from .processor import get_processor

            processor = get_processor()

        super().__init__(*args, **kwargs)

        if not no_hook:
            is_async = asyncio.iscoroutinefunction(self.get)

            self._event_hooks["request"].append(
                processor.async_handle_request
                if is_async
                else processor.sync_handle_request
            )
            self._event_hooks["response"].append(
                processor.sync_handle_response
                if is_async
                else processor.async_handle_response
            )
