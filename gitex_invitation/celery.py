import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gitex_invitation.settings")

app = Celery("gitex_invitation")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
