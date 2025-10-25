from django.urls import path
from . import views

urlpatterns = [
    #Auth User Inviation stats
    path("stats/", views.InvitationStatsView.as_view(), name="invitation-stats"),
    path("tickets/", views.TicketTypeListView.as_view(), name="invitation-tickets"),

    #personal inviation
    path("send/", views.SendPersonalInvitationView.as_view(), name="send-personal-invitation"),

    #Bulk Invitations
    path("bulk/upload/", views.BulkUploadView.as_view(), name="bulk-upload"), 
    path("bulk/<uuid:job_id>/rows/", views.BulkRowsView.as_view(), name="bulk-rows"),
    path("bulk/<uuid:job_id>/row/<int:row_number>/", views.BulkRowPatchView.as_view(), name="bulk-row-patch"),
    path("bulk/<uuid:job_id>/confirm/", views.BulkConfirmView.as_view(), name="bulk-confirm"),
    path("bulk/<uuid:job_id>/delete/row/<int:row_number>/", views.BulkRowDeleteView.as_view(), name="bulk-delete-row"),
    path("bulk/<uuid:job_id>/rows/clear/", views.BulkClearPreviewView.as_view(), name="bulk-clear-preview"),
    path("bulk/<uuid:job_id>/rows/add/", views.BulkAddRowView.as_view(), name="bulk-add-row"), 

    #List Inviations 
    path("list/", views.InvitationListView.as_view(), name="invitation-list"),
 
    #Inviation actions
    path("dash/<int:pk>/", views.InvitationDetailByIdView.as_view(), name="invitation-detail-by-id"),
    path("<int:pk>/delete/", views.InvitationDeleteView.as_view(), name="invitation-delete"),
    path("<int:pk>/edit/", views.InvitationEditView.as_view(), name="invitation-edit"),
 
    #Inviation(link) Confirmations
    path("<uuid:link_code>/", views.InvitationDetailView.as_view(), name="invitation-detail"),
    path("<uuid:link_code>/confirm/", views.InvitationConfirmView.as_view(), name="invitation-confirm"), 

    #Generate Inviation Link
    path("generate-link/", views.GenerateInvitationLinkView.as_view(), name="generate-invitation-link"),
    path("register-from-link/", views.RegisterFromLinkView.as_view(), name="register-from-link"), 
    path("link/<uuid:link_code>/", views.GenerateInvitationLinkDetailsView.as_view(), name="link-invitation-details"),

    #Brodcasting 
    path("broadcast/", views.BroadcastInvitationView.as_view(), name="invitation-broadcast"),

    #Export to file
    path("exports/request/", views.InvitationExportStartView.as_view(), name="export-request"),
    path("exports/<uuid:job_id>/", views.InvitationExportStatusView.as_view(), name="export-status"),
    path("export/download/<str:filename>/", views.InvitationExportDownloadView.as_view()),

    path("jobs/", views.BulkUploadJobListView.as_view(), name="bulk-job-list"),
    path("jobs/<uuid:job_id>/status/", views.BulkUploadJobStatusView.as_view(), name="bulk-job-status"),]   
 


