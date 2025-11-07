"""Fetch enviroment variables used on pumpwood flaskviews."""
import os

INFO_CACHE_TIMEOUT = int(
    os.getenv('INFO_CACHE_TIMEOUT', 600))
SERIALIZER_FK_CACHE_TIMEOUT = int(
    os.getenv('SERIALIZER_FK_CACHE_TIMEOUT', 600))
