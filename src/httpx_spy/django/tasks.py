"""Hooking up to Celery"""

try:
    from celery.signals import worker_init, worker_process_init

    @worker_process_init.connect
    @worker_init.connect
    def init_httpx_spy(*_, **__) -> None:
        """Makes sure that workers also get their share of spying"""

        from .common import django_init

        django_init()
except ImportError:
    pass
