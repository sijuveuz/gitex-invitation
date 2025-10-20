from celery import shared_task
from django.template.loader import render_to_string
from django.core.mail import EmailMultiAlternatives
from django.conf import settings
from invitations.models import Invitation

@shared_task(bind=True, max_retries=5, default_retry_delay=60)
def send_personal_invitation_email(self, invitation_id):
    try:
        inv = Invitation.objects.get(id=invitation_id)
    except Invitation.DoesNotExist:
        # nothing to do
        return 

    # render templates (html/plain)
    subject = f"You're invited to GITEX — {inv.ticket_type.name}"
    context = {
        "guest_name": inv.guest_name,
        "invitation_url": inv.invitation_url,
        "personal_message": inv.personal_message,
        "expire_date": inv.expire_date,
        "ticket_type": inv.ticket_type.name,
    }
    text_body = render_to_string("emails/invite_personal.txt", context)
    html_body = render_to_string("emails/invite_personal.html", context)

    msg = EmailMultiAlternatives(subject, text_body, settings.DEFAULT_FROM_EMAIL, [inv.guest_email])
    msg.attach_alternative(html_body, "text/html")

    try:
        msg.send(fail_silently=False)
    except Exception as exc:
        # retry with exponential backoff
        raise self.retry(exc=exc)
