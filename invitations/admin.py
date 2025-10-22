from django.contrib import admin

from .models import * 

admin.site.register(InvitationStats)
admin.site.register(Invitation)
admin.site.register(InvitationLinkUsage)   
admin.site.register(BulkUploadJob)

