"""
Our Django app for automatic Django integration
"""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class HttpxSpyAppConfig(AppConfig):
    """Tells Django how to deal with this app"""

    verbose_name = _("HTTPX Spy")
    name = "httpx_spy.django"
    label = "httpx_spy"

    def ready(self):
        """
        Starts the processor and monkey patches httpx
        """

        from .common import django_init

        django_init()
