#!/usr/bin/env python
"""Serve the Chandra comm schedule.
"""

import sys
import os
import subprocess
import time
import datetime
import pytz


class CommPass(object):
    """Details of one comm pass.
    """

    def __init__(self, text):
        (self.label, self.botz, self.eotz, self.station, self.site, ttl,
         zone, self.dow, self.date, self.month, self.ybotz, dtbotz, self.yeotz,
         dteotz) = text.split()
        # Local time zone
        self.zone = zone[:3]
        # BOT and EOT in local time
        self.tbot, self.teot = ttl.split('-')
        # UT day of year for BOT
        self.doybotz = "{0:03d}".format(int(dtbotz.split('.')[0]))
        # UT day of year for EOT
        self.doyeotz = "{0:03d}".format(int(dteotz.split('.')[0]))
        # UT day of year and duration for whole pass
        self.doyz, dtz = self.label.split('/')
        # Start and end times of pass in UT
        self.tstartz, self.tendz = dtz.split('-')
        # Formatted track start and end in UT
        self.tbotz = "{0}:{1}:{2}:{3}:00.000".format(self.ybotz, self.doybotz,
                                                     self.botz[:2],
                                                     self.botz[2:])
        self.teotz = "{0}:{1}:{2}:{3}:00.000".format(self.yeotz, self.doyeotz,
                                                     self.eotz[:2],
                                                     self.eotz[2:])

    def during(self, tz):
        """Is tz is before, during or after this comm pass?
        """
        # tz must be formatted as YYYY:DOY:HH:MM:SS.SSS, so that
        # dictionary order can be used here
        if tz < self.tbotz:
            return -1
        if tz > self.teotz:
            return 1
        return 0


class DSNexception (Exception):
    pass


class DSNsched(object):
    """Serve the Chandra comm schedule for an application.
    """
    # Comm pass schedule in time order
    srcURL = "http://asc.harvard.edu/mta/DSN.txt"

    def parse(self):
        """Assemble comm pass details.
        """
        # Skip headings and blank lines, parsing the remaining lines for
        # comm pass details
        self.passes = [CommPass(l) for l in self.output.split("\n")[2:] if l]

    def __init__(self):
        # Fetch the current comm pass schedule
        cmd = "wget"
        cmdargs = [cmd, "-O-", "--no-check-certificate", DSNsched.srcURL]
        x = subprocess.Popen(cmdargs, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        # Wait for the command to complete and collect both stdout and stderr.
        self.output, self.stderr = x.communicate()
        if x.returncode:
            raise DSNexception(self.stderr)
        self.parse()


if __name__ == "__main__":
    s = DSNsched()

    for t in s.passes:
        print(t.tbotz, '-', t.teotz)

    if len(sys.argv) == 3:
        i = int(sys.argv[1])
        cp = s.passes[i]
        ztime = sys.argv[2]
        print("Comm pass time:", cp.tbotz, "-", cp.teotz)
        inpass = cp.during(ztime)
        if inpass < 0:
            print(ztime, "is before the pass")
        elif inpass == 0:
            print(ztime, "is during the pass")
        else:
            print(ztime, "is after the pass")
