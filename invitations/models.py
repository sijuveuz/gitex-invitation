from django.utils import timezone
from django.conf import settings
from django.db import models
import uuid


class TicketType(models.Model):
    """
    Represents available ticket types for invitations (Visitor, VIP, etc.)
    """
    name = models.CharField(max_length=50, unique=True, db_index=True)
    description = models.TextField(blank=True, null=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name


class InvitationStats(models.Model):
    """
    Tracks invitation metrics for each registered user.
    """
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="invitation_stats"
    )
    allocated_invitations = models.PositiveIntegerField(default=50000)
    generated_invitations = models.PositiveIntegerField(default=0)
    remaining_invitations = models.PositiveIntegerField(default=50000)
    registered_visitors = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f"Invitation stats for {self.user.email}"

    def update_remaining(self):
        self.remaining_invitations = self.allocated_invitations - self.generated_invitations
        self.save()

    class Meta:
        indexes = [
            models.Index(fields=["user"]),
            models.Index(fields=["remaining_invitations"]),
        ]


class Invitation(models.Model):
    """
    Represents any invitation record — personal, bulk, or generated link-based.
    Belongs to a specific exhibitor (user).
    """
    SOURCE_CHOICES = [
        ("personal", "Personalized"),
        ("bulk", "Bulk Upload"),
        ("link", "Invitation Link"),
    ]

    STATUS_CHOICES = [
        ("active", "Active"),
        ("expired", "Expired"),
        ("pending", "Pending"),
    ]

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="invitations"
    )

    # Core guest info
    guest_name = models.CharField(max_length=255)
    guest_email = models.EmailField(blank=True, null=True)
    company_name = models.CharField(max_length=255, blank=True, null=True)
    personal_message = models.TextField(blank=True, null=True)

    # Invitation configuration
    ticket_type = models.ForeignKey("invitations.TicketType", on_delete=models.PROTECT)
    expire_date = models.DateField()
    source_type = models.CharField(max_length=20, choices=SOURCE_CHOICES)
    link_code = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    usage_limit = models.PositiveIntegerField(default=1)  # For link-based invites
    usage_count = models.PositiveIntegerField(default=0)
    invitation_url = models.URLField(blank=True, null=True, max_length=500)
    # System fields
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="active")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    registered_at = models.DateTimeField(blank=True, null=True)
    registered = models.BooleanField(default=False)
    link_is_active = models.BooleanField(default=True)
    link_limit_reached = models.BooleanField(default=True)
    class Meta:
        ordering = ["-created_at"]

        constraints = [
            models.UniqueConstraint(
                fields=["user", "guest_email", "ticket_type"],
                name="unique_invite_per_guest_per_ticket",
                condition=~models.Q(guest_email__isnull=True),
            ),
        ]

        indexes = [
            models.Index(fields=["user", "guest_email"]),
            models.Index(fields=["user", "source_type"]),
            models.Index(fields=["link_code"]),
            models.Index(fields=["status"]),
            models.Index(fields=["expire_date"]),
        ]

    def __str__(self):
        return f"{self.guest_name} ({self.guest_email}) via {self.source_type}"

    @property
    def is_expired(self):
        return timezone.now().date() > self.expire_date or self.status == "expired"

    @property
    def remaining_uses(self):
        return max(self.usage_limit - self.usage_count, 0)

    def mark_as_expired(self):
        """Expire the invitation."""
        self.status = "expired"
        self.save(update_fields=["status", "updated_at"])


class InvitationLinkUsage(models.Model):
    """
    Tracks each individual registration that happened via a generated invitation link.
    """

    link = models.ForeignKey(
        "invitations.Invitation",
        on_delete=models.CASCADE,
        related_name="usages"
    )

    guest_name = models.CharField(max_length=255)
    guest_email = models.EmailField()
    company_name = models.CharField(max_length=255, blank=True, null=True)
    registered_at = models.DateTimeField(auto_now_add=True)
    registered = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        unique_together = ("link", "guest_email")

    def __str__(self):
        return f"{self.guest_name} ({self.guest_email}) via {self.link.link_code}"



class BulkUploadJob(models.Model):
    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_PREVIEW_READY = "preview_ready"
    STATUS_CONFIRMED = "confirmed"
    STATUS_SENDING = "sending"
    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"
    STATUS_CLEARED = "cleared_data"

    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_PROCESSING, "Processing"),
        (STATUS_PREVIEW_READY, "Preview Ready"),
        (STATUS_CONFIRMED, "Confirmed"),
        (STATUS_SENDING, "Sending"),
        (STATUS_COMPLETED, "Completed"),
        (STATUS_FAILED, "Failed"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="bulk_jobs")
    uploaded_file = models.FileField(upload_to="bulk_uploads/")
    file_name = models.CharField(max_length=255, blank=True)
    status = models.CharField(max_length=30, choices=STATUS_CHOICES, default=STATUS_PENDING)

    total_count = models.PositiveIntegerField(default=0)
    valid_count = models.PositiveIntegerField(default=0)
    invalid_count = models.PositiveIntegerField(default=0)

    # small preview cached (first N rows) to show immediately in UI
    preview_data = models.JSONField(default=list, blank=True)
    sample_errors = models.JSONField(default=list, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    # expire_date = models.DateTimeField(null=True, blank=True)
    # default_personal_message = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"BulkUploadJob({self.id}) by {self.user.email}"

