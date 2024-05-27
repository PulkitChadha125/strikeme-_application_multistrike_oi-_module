from rest_framework import serializers


class UserSelectedSerializer(serializers.Serializer):
    symbol = (serializers.CharField(required=True),)
    instrument = (serializers.CharField(required=True),)
    strikeprice = serializers.IntegerField(required=True)
    option_type = (serializers.CharField(required=True),)
    expiery = (serializers.DateField(required=True),)



class GetNearestStrikeSerializer(serializers.Serializer):

    symbol = (serializers.CharField(required=True),)
    monthly_exp = (serializers.DateField(required=True),)
    multiplier=(serializers.IntegerField(required=True),)
    instrument = (serializers.CharField(required=True),)

    option_type = (serializers.CharField(required=True),)
    selected_exp = (serializers.DateField(required=True),)
