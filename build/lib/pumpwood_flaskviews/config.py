"""Fetch enviroment variables used on pumpwood flaskviews."""
import os

INFO_CACHE_TIMEOUT = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__INFO_CACHE_TIMEOUT', 600))
SERIALIZER_FK_CACHE_TIMEOUT = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__SERIALIZER_FK_CACHE_TIMEOUT', 300))
MICROSERVICE_URL = os.getenv('MICROSERVICE_URL')
