from django.db import models
from django.core.exceptions import ValidationError

from invitations.models import BulkUploadJob
from accounts.models import User 

class TicketType(models.Model):
    """
    Represents available ticket types for invitations (Visitor, VIP, etc.)
    """
    name = models.CharField(max_length=50, unique=True, db_index=True)
    description = models.TextField(blank=True, null=True)
    is_active = models.BooleanField(default=True)
    enforce_unique_email = models.BooleanField(default=False)  # ticket-level uniqueness
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return  f"{self.name} Unique: {self.enforce_unique_email}"
    

class InvitationSettings(models.Model):
    enforce_global_unique = models.BooleanField(default=False)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        # Enforce only one instance
        if not self.pk and InvitationSettings.objects.exists():
            raise ValidationError("Only one InvitationSettings instance is allowed.")

        # Get original value before save
        old_value = None
        if self.pk:
            old_value = InvitationSettings.objects.get(pk=self.pk).enforce_global_unique

        super().save(*args, **kwargs)

        # If enforce_global_unique is turned ON
        if old_value is False and self.enforce_global_unique is True:
            TicketType.objects.update(enforce_unique_email=True)

    def __str__(self):
        return f"Global Unique: {self.enforce_global_unique}"





class DuplicateRecord(models.Model):
    """
    Stores information about duplicate invitations detected
    during bulk upload or send operation.
    """

    DETECTION_SOURCE_CHOICES = [
        ("dedup_service", "Deduplication Service"),
        ("db_check", "Database Check"),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="duplicate_records")
    job = models.ForeignKey(
        BulkUploadJob, on_delete=models.CASCADE, related_name="duplicate_records"
    )
    ticket_type = models.ForeignKey(
        TicketType, on_delete=models.SET_NULL, null=True, blank=True, related_name="duplicate_records"
    )

    guest_email = models.EmailField(db_index=True)
    detection_source = models.CharField(max_length=50, choices=DETECTION_SOURCE_CHOICES)
    reason = models.TextField(blank=True, null=True)  # optional debugging reason
    scope = models.CharField(max_length=20, blank=True, null=True)  # "ticket" / "global"
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["guest_email", "scope"]),
        ]
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.guest_email} (Job: {self.job_id}, Scope: {self.scope})"
