from core.views import getHealth
from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/health/", getHealth, name="health"),
    path("api-auth/", include("rest_framework.urls")),
    path("api/user/", include("user.urls")),
    path("api/beam/", include("beam.urls")),
    path("api/visualizer/", include("visualizer.urls")),
]
