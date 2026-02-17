"""Fetch enviroment variables used on pumpwood flaskviews."""
import os

INFO_CACHE_TIMEOUT = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__INFO_CACHE_TIMEOUT', 600))
"""Config variable to ser cache associated with information data, such as
   options and points."""

SERIALIZER_FK_CACHE_TIMEOUT = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__SERIALIZER_FK_CACHE_TIMEOUT', 300))
"""Config variable to ser cache associated with foreign key data fetch."""


AUTHORIZATION_CACHE_TIMEOUT = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__AUTHORIZATION_CACHE_TIMEOUT', 60))
"""Config variable to ser cache associated with autorization and row
   permission cache."""

MICROSERVICE_URL = os.getenv('MICROSERVICE_URL')
