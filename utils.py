
import re
import datetime
from decimal import Decimal

class Utils:
    @staticmethod
    def normalize_string(value):
        if not value:
            return ''
        value = value.strip().lower()
        value = re.sub(r'[^a-z0-9\s]', '', value)
        value = re.sub(r'\s+', ' ', value)
        return value

    @staticmethod
    def normalize_date(d):
        if not d:
            return None
        if isinstance(d, datetime.date):
            return d.isoformat()
        elif isinstance(d, str):
            try:
                parsed = datetime.datetime.strptime(d, "%Y-%m-%d").date()
                return parsed.isoformat()
            except ValueError:
                return d
        return d

    @staticmethod
    def to_float_if_decimal(value):
        if isinstance(value, Decimal):
            return float(value)
        return value
