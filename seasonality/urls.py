from django.urls import path
from seasonality.views import SeasonalityScreener, StockSeasonalityAPI


urlpatterns = [
    path("stocks", StockSeasonalityAPI.as_view()),
    path("screener", SeasonalityScreener.as_view()),
]
