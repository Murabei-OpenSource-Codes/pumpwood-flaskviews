"""Fetch enviroment variables used on pumpwood flaskviews."""
import os

PUMPWOOD_FLASKVIEWS__INFO_CACHE_TIMEOUT = int(
    os.getenv('PUMPWOOD_FLASKVIEWS__INFO_CACHE_TIMEOUT', 600))
