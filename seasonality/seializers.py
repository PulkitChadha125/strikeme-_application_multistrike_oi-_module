from rest_framework import serializers


class StockSeasonalitySerializer(serializers.Serializer):
    start = serializers.DateField(required=False)
    end = serializers.DateField(required=False)
    period_start = serializers.DateField(required=False)
    period_end = serializers.DateField(required=False)
    symbol = serializers.CharField(required=False)
    custom = serializers.BooleanField(required=False)
    custom_period_start = serializers.DateField(required=False)
    custom_period_end = serializers.DateField(required=False)


class ScreenerSeasonalitySerializer(serializers.Serializer):
    start_date = serializers.DateField(required=False)
    end_date = serializers.DateField(required=False)
    indices_id = serializers.ListField(child=serializers.CharField(), required=False)
    security_codes = serializers.ListField(
        child=serializers.CharField(), required=False
    )
    time_period = serializers.IntegerField(required=False)
    exam_period = serializers.IntegerField(required=False)
    all_indices = serializers.BooleanField(required=False)
    fno_stocks = serializers.BooleanField(required=False)
