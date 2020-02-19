#!/usr/bin/env python
"""Convert a local time to UT.
"""

import time
import datetime
import pytz


# The local timezone
tz = pytz.timezone('US/Eastern')


def convert2ut(ttp, dst_status=None):
    """Convert a formatted time from local time to UT.
    """
    tv = time.strptime(ttp, "%Y:%j:%H:%M")  # Input string -> struct_time
    dt = datetime.datetime(*(tv[:6]))  # struct_time -> datetime
    # This is_dst argument is ignored, unless the input time falls in the
    # ambiguous 2 hour interval of local time at the end of daylight saving.
    # However, leaving it unspecified in that case will cause an exception.
    local_dt = tz.localize(dt, is_dst=dst_status)  # make datetime aware
    # Convert to UTC and format
    utc_dt = local_dt.astimezone(pytz.utc)
    tgm = utc_dt.utctimetuple()
    return "{0:04d}:{1:03d}:{2:02d}:{3:02d}".format(tgm.tm_year, tgm.tm_yday,
                                                    tgm.tm_hour, tgm.tm_min)
