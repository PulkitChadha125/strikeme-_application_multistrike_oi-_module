import json
from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework import permissions
from rest_framework.request import Request
from seasonality.seializers import (
    ScreenerSeasonalitySerializer,
    StockSeasonalitySerializer,
)
from seasonality.stock_seasonality import (
    Symbol_Seasonality_Individual as seasonality_individual,
)
from seasonality.screener_seasonality import (
    Symbol_Seasonality_Screener as seasonality_screener,
)
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from datetime import date
from datetime import time
from seasonality.stock_data import HistoricalData as h
from datetime import datetime
from rest_framework.response import Response
from rest_framework import status
import pandas as pd
from django_redis import get_redis_connection
from django.core.cache import cache
from seasonality.screener_data import ScreenerData as s_d


class StockSeasonalityAPI(APIView):
    # permission_classes = [permissions.AllowAny]
    # authentication_classes = []

    def post(self, request: Request):
        serializer = StockSeasonalitySerializer(data=request.data)
        # key = "diffusion*"
        # data = cache.keys(key)
        # print(data)

        custom = None
        if serializer.is_valid():
            if "custom" in serializer.validated_data:
                custom = serializer.validated_data["custom"]
                custom_period_start = serializer.validated_data["custom_period_start"]
                custom_period_end = serializer.validated_data["custom_period_end"]

            symbol = serializer.validated_data["symbol"]
            start = serializer.validated_data["start"]
            end = serializer.validated_data["end"]
            period_start = serializer.validated_data["period_start"]
            period_end = serializer.validated_data["period_end"]

            df = h.ic_historical_data(
                symbol=symbol,
                start=start,
                end=end,
                tf="D",
                indices_id="",
                securities="",
            )

            sym_obj = seasonality_individual(symbol=symbol, price_data=df)
            if custom is None:
                sym_obj.update_horizon(start=period_start, end=period_end)
                annual_seasonality = sym_obj.annual_seasonality.to_json(
                    orient="records", date_format="iso"
                )
                detrend_annual_seasonality = (
                    sym_obj.detrended_annual_seasonality.to_json(
                        orient="records", date_format="iso"
                    )
                )
                best_performance_window = json.dumps(sym_obj.best_performance_window)
                best_performance_metrics = json.dumps(sym_obj.best_performance_metrics)
                worst_performance_window = json.dumps(sym_obj.worst_performance_window)
                worst_performance_metrics = json.dumps(
                    sym_obj.worst_performance_metrics
                )
                best_performance_basket = sym_obj.best_performance_backtest.to_json(
                    orient="records", date_format="iso"
                )
                worst_performance_basket = sym_obj.worst_performance_backtest.to_json(
                    orient="records", date_format="iso"
                )

                data = {
                    "annual_seasonality": json.loads(annual_seasonality),
                    "detrend_annual_seasonality": json.loads(
                        detrend_annual_seasonality
                    ),
                    "best_performance_window": json.loads(best_performance_window),
                    "best_performance_metrics": json.loads(best_performance_metrics),
                    "worst_performance_window": json.loads(worst_performance_window),
                    "worst_performance_metrics": json.loads(worst_performance_metrics),
                    "best_performance_basket": json.loads(best_performance_basket),
                    "worst_performance_basket": json.loads(worst_performance_basket),
                }
                return Response({"data": data}, status=status.HTTP_200_OK)
            else:
                sym_obj.update_horizon(start=period_start, end=period_end)
                sym_obj.update_custom_backtest(
                    start_day=custom_period_start.day,
                    start_month=custom_period_start.month,
                    end_day=custom_period_end.day,
                    end_month=custom_period_end.month,
                )
                custom_window = json.dumps(sym_obj.custom_window)
                custom_performance_metrics = json.dumps(sym_obj.custom_backtest_metrics)
                custom_performance_basket = sym_obj.custom_backtest.to_json(
                    orient="records", date_format="iso"
                )
                data = {
                    "custom_window": json.loads(custom_window),
                    "custom_performance_metrics": json.loads(
                        custom_performance_metrics
                    ),
                    "custom_performance_basket": json.loads(custom_performance_basket),
                }
            return Response({"data": data}, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class SeasonalityScreener(APIView):
    # permission_classes = [permissions.AllowAny]
    # authentication_classes = []

    def post(self, request: Request):
        serializer = ScreenerSeasonalitySerializer(data=request.data)
        if serializer.is_valid():
            if "indices_id" in serializer.validated_data:
                indices_id = serializer.validated_data["indices_id"]
            else:
                indices_id = None

            date = serializer.validated_data["start_date"]

            if "all_indices" in serializer.validated_data:
                all = serializer.validated_data["all_indices"]
            else:
                all = False

            if "time_period" in serializer.validated_data:
                time_period = serializer.validated_data["time_period"]
            else:
                time_period = 5

            if "exam_period" in serializer.validated_data:
                exam_period = serializer.validated_data["exam_period"]
            else:
                exam_period = 10

            if "fno_stocks" in serializer.validated_data:
                fno = serializer.validated_data["fno_stocks"]
            else:
                fno = False

            if "security_codes" in serializer.validated_data:
                security_codes = serializer.validated_data["security_codes"]
            else:
                security_codes = None

            df = s_d.get_data(
                date=date,
                indices_id=indices_id,
                all=all,
                fno=fno,
                securities=security_codes,
                time_period=time_period,
                exam_period=exam_period,
            )

            data = {
                "screener_data": json.loads(
                    df.to_json(orient="records", date_format="iso")
                ),
            }

            return Response({"data": data}, status=status.HTTP_200_OK)
        return Response({serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
