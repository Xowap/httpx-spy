"""
Utilities common for Django-related integrations
"""

import threading

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

_init_done = False
_init_done_lock = threading.Lock()


def django_init():
    """Ensures that HTTPX Spy is started"""

    global _init_done

    if _init_done:
        return

    with _init_done_lock:
        if _init_done:
            return

        # Somehow not doing this results in a deadlock when some parts of the
        # code import httpcore? No idea why, but it fixes the issue ¯\_(ツ)_/¯
        import httpcore  # noqa: F401

        from httpx_spy.processor import get_processor

        processor = get_processor()

        if moesif_config := getattr(settings, "MOESIF_MIDDLEWARE", None):
            from httpx_spy.moesif import MoesifHandler

            if moesif_config.get("CAPTURE_OUTGOING_REQUESTS"):
                try:
                    mh = MoesifHandler(moesif_config["APPLICATION_ID"])
                except KeyError:
                    msg = (
                        "APPLICATION_ID not found in django.settings.MOESIF_MIDDLEWARE"
                    )
                    raise ImproperlyConfigured(msg) from None
                else:
                    processor.add_handler(mh)

        if processor.handlers:
            processor.start()

        _init_done = True
