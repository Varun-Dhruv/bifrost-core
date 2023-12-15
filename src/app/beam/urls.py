from beam.views import run
from django.urls import path

urlpatterns = [
    path("run", run, name="run"),
]
