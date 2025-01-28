"""Module for serialization of Json for complex fields."""
import numpy as np
import pandas as pd
import decimal
from datetime import datetime
from datetime import date
from datetime import time
from pandas import Timestamp
from shapely.geometry.base import BaseGeometry
from shapely.geometry import mapping
from sqlalchemy_utils.types.choice import Choice
from flask.json.provider import DefaultJSONProvider


class PumpWoodFlaskJSONProvider(DefaultJSONProvider):
    """PumpWood default serializer.

    Treat not simple python types to facilitate at serialization of
    pandas, numpy, data, datetime and other data types.
    """

    def default(self, obj):
        """Serialize complex objects."""
        # Return None if object is NaN
        try:
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, Timestamp):
                return obj.isoformat()
            if isinstance(obj, date):
                return obj.isoformat()
            if isinstance(obj, time):
                return obj.isoformat()
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, pd.DataFrame):
                return obj.to_dict('records')
            if isinstance(obj, pd.Series):
                return obj.tolist()
            if isinstance(obj, np.generic):
                return obj.item()
            if isinstance(obj, BaseGeometry):
                if obj.is_empty:
                    return None
                else:
                    return mapping(obj)
            if isinstance(obj, BaseGeometry):
                return mapping(obj)
            if isinstance(obj, Choice):
                return obj.code
            if isinstance(obj, set):
                return list(obj)
            if isinstance(obj, decimal.Decimal):
                return float(obj)
        except Exception as e:
            msg = (
                "Unserializable object {obj} of type {type}. Error msg: {msg}"
            ).format(obj=obj, type=type(obj), msg=str(e))
            raise TypeError(msg)

        msg = (
            "Unserializable object not mapped on PumpWoodFlaskJSONProvider "
            "{obj} of type {type}"
        ).format(obj=obj, type=type(obj))
        raise TypeError(msg)
