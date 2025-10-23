from django.urls import path
from adminapp.views import DuplicateRecordListView

urlpatterns = [
    path("duplicates/", DuplicateRecordListView.as_view(), name="duplicate-records"),
]