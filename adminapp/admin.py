from django.contrib import admin

from .models import *


admin.site.register(TicketType)
admin.site.register(InvitationSettings)
admin.site.register(DuplicateRecord)

