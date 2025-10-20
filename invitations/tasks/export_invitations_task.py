# tasks.py
from celery import shared_task
from django.conf import settings
from accounts.models import User
from invitations.models import Invitation
from invitations.helpers.exporters import CSVExporter, ExcelExporter, PDFExporter
from invitations.utils.redis_utils import get_redis
from reportlab.lib.units import mm


@shared_task
def export_invitations_task(user_id, export_format, job_id):
    r = get_redis()
    user = User.objects.get(id=user_id)
    invitations = Invitation.objects.filter(user=user)

    exporters = {
        "csv": CSVExporter,
        "xlsx": ExcelExporter,
        "pdf": PDFExporter,
    }

    exporter_cls = exporters.get(export_format)
    if not exporter_cls:
        r.set(job_id, "ERROR: Unknown format")
        return

    exporter = exporter_cls(invitations)
    file_path, file_url = exporter.export(job_id)

    r.set(job_id, file_url)
    r.expire(job_id, 600)  # auto-expire after 10 mins
    return file_url







