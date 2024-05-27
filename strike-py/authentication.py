from rest_framework.exceptions import APIException
from rest_framework import status
from rest_framework import authentication
import jwt
import time as t
from datetime import datetime


class NoAuthToken(APIException):
    status_code = status.HTTP_401_UNAUTHORIZED
    default_detail = "No authentication token provided"
    default_code = "no_auth_token"


class InvalidAuthToken(APIException):
    status_code = status.HTTP_401_UNAUTHORIZED
    default_detail = "Invalid authentication token provided"
    default_code = "invalid_token"
    
    
class JwtAuthentication(authentication.TokenAuthentication):
    def authenticate(self, request):
        auth_header = request.META.get("HTTP_AUTHORIZATION")
        if not auth_header:
            raise NoAuthToken("No auth token provided")
        id_token = auth_header.split(" ").pop()
        decoded_token = None
        jwt_secret_key = "5152fa850c02dc222631cca898ed1485821a70912a6e3649c49076912daa3b62182ba013315915d64f40cddfbb8b58eb5bd11ba225336a6af45bbae07ca873f3"
        try:
            decoded_token = jwt.decode(id_token, jwt_secret_key, algorithms=['HS256'])
            for key, value in decoded_token.items():
                if(key == 'sub'):
                    user = value
                if(key == 'iat'):
                    dt = datetime.fromtimestamp(value)
                    print(dt)
                if(key == 'exp'):
                    dt = datetime.fromtimestamp(value)
                    print(dt)
                    if(~(dt <= datetime.now())):
                        print('valid')
                        return (user,True)
                    else:
                        raise InvalidAuthToken("Invalid auth token")
        except Exception:
            raise InvalidAuthToken("Invalid auth token")
        return (user,True)
        
        