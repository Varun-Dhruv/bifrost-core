from django.urls import path
from user.views import user_login, user_signup

urlpatterns = [
    path("login", user_login, name="login"),
    path("signup", user_signup, name="signup"),
]
