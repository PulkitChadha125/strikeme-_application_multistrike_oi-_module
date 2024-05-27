from django.urls import path
from multistrikeoi.views import UserSelectedAPI,GetNearestStrikeAPI

urlpatterns = [
    path("user_selected_price", UserSelectedAPI.as_view()),
    path("get_nearest_strike", GetNearestStrikeAPI.as_view()),
]
