#!/usr/bin/python

"""
  Fixes invalid dates found in spl JSONs.
"""

import arrow

DATE_FORMAT = "YYYY-MM-DD"


# Restores the date string after parsing.
def fixDate(y, m, d):
    return y + "-" + m + "-" + d


# Accepts a date string in format YYYY-MM-DD and fixes invalid dates.
def validate_date(date):
    year, month, day = date.split("-", 2)
    year, month, day = year[:4].zfill(4), month[:2].zfill(2), day[:2].zfill(2)

    try:
        date_obj = arrow.get(fixDate(year, month, day), DATE_FORMAT)
    except Exception as e:
        if str(e) == "day is out of range for month":
            if int(day) < 1:
                day = "01"
            elif int(day) > 31:
                day = "31"
            elif int(day) > 28:
                day = str((int(day) - 1))
        elif str(e) == "month must be in 1..12":
            if int(month) < 1:
                month = "01"
            if int(month) > 12:
                month = "12"
        else:
            raise e
    try:
        date_obj = arrow.get(fixDate(year, month, day), DATE_FORMAT)
    except:
        date_obj = validate_date(fixDate(year, month, day))
    finally:
        return date_obj.format(DATE_FORMAT)
