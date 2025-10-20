from django.urls import path
from . import views as view

urlpatterns = [
    path("register/",view. UserRegistrationView.as_view(), name="register"),
    path("login/", view.UserLoginView.as_view(), name="login"),
]
