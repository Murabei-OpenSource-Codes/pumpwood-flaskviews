"""Fetch environment variables used on pumpwood flaskviews."""
import os

INFO_CACHE_EXPIRE = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__INFO_CACHE_EXPIRE', 600))
"""Config variable to set cache associated with information data, such as
   options and points."""

SERIALIZER_FK_CACHE_EXPIRE = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__SERIALIZER_FK_CACHE_EXPIRE', 300))
"""Config variable to set cache associated with foreign key data fetch."""


AUTHORIZATION_CACHE_EXPIRE = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__AUTHORIZATION_CACHE_EXPIRE', 60))
"""Config variable to set cache associated with authorization and row
   permission cache."""

MICROSERVICE_URL = os.getenv('MICROSERVICE_URL')
