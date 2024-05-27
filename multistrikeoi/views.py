import json
from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework import permissions
from rest_framework.request import Request
from multistrikeoi import user_selected_price as usp
from multistrikeoi.serializers import UserSelectedSerializer,GetNearestStrikeSerializer
from rest_framework.response import Response
from rest_framework import status
from django.db import connection
import pandas as pd

from multistrikeoi import get_nearest_strike as gns



class UserSelectedAPI(APIView):
    permission_classes = [permissions.AllowAny]
    authentication_classes = []

    def post(self, request: Request):
        serializer = UserSelectedSerializer(data=request.data)
        if serializer.is_valid():
            symbol = request.data["symbol"]
            instrument = request.data["instrument"]
            strikeprice = request.data["strikeprice"]
            option_type = request.data["option_type"]
            expiery = request.data["expiery"]

            df_selected_strike = usp.UserSelectedPrice.combined_oi_calculation(
                symbol=symbol,
                instrument=instrument,
                strikeprice=strikeprice,
                option_type=option_type,
                expiery=expiery,
            )
            df_eod_data = usp.UserSelectedPrice.get_historical_eod_data(
                symbol=symbol,
                instrument=instrument,
                strikeprice=strikeprice,
                option_type=option_type,
                expiery=expiery,
            )

            merged_df = df_selected_strike.merge(
                df_eod_data[["expiry_date", "open_interest", "Combined OI EOD"]],
                on="expiry_date",
                how="left",
            )
            merged_df.rename(
                columns={"open_interest": "Yesterday Eod OI"}, inplace=True
            )
            merged_df["Change In Oi"] = (
                merged_df["open_interest_x"] - merged_df["Yesterday Eod OI"]
            )
            merged_df["Change In Combined Oi"] = (
                merged_df["Combined OI"] - merged_df["Combined OI EOD"]
            )

            merged_df_json = merged_df.to_json(orient="records")

            return Response(
                {"data": json.loads(merged_df_json)}, status=status.HTTP_200_OK
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class  GetNearestStrikeAPI(APIView):
    permission_classes = [permissions.AllowAny]
    authentication_classes = []

    def post(self, request: Request):
        serializer = GetNearestStrikeSerializer(data=request.data)
        engine = connection
        if serializer.is_valid():
            symbol = request.data["symbol"]
            monthly_exp = request.data["monthly_exp"]
            multiplier =  request.data["multiplier"]
            instrument =  request.data["instrument"]

            selected_exp = request.data["selected_exp"]

            gns.GetNearestStrike.get_nearest_strike(symbol=symbol,monthly_exp=monthly_exp,multiplier=multiplier,instrument=instrument,selected_exp=selected_exp)


