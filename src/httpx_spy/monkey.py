"""
Things that we use to monkey-patch httpx
"""

import asyncio

import httpx


class _MonkeyProxy:
    """
    Proxies the hooks so that we know that's our hooks and not something from
    somewhere else
    """

    def __init__(self, obj: object):
        self.obj = obj

    def __call__(self, *args, **kwargs):
        # noinspection PyCallingNonCallable
        return self.obj(*args, **kwargs)


def _ensure_in_list(lst: list, obj: object) -> None:
    if not any(isinstance(x, _MonkeyProxy) for x in lst):
        lst.append(_MonkeyProxy(obj))


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

            _ensure_in_list(
                out["request"],
                (
                    processor.async_handle_request
                    if is_async
                    else processor.sync_handle_request
                ),
            )
            _ensure_in_list(
                out["response"],
                (
                    processor.async_handle_response
                    if is_async
                    else processor.sync_handle_response
                ),
            )

        return out
