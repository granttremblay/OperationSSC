#!/usr/bin/env python

import os
import time
from datetime import datetime
import subprocess as sp
import tempfile as tf
import re


# Maximum VCDU value + 1
vcduWrap = 1 << 24


def deltaVCDU(a, b):
    """Minimum difference between two vcdu values, allowing for wrap around.
    """
    d = b - a
    if d < 0:
        if abs(d) > abs(d + vcduWrap):
            d += vcduWrap
    elif d > abs(d - vcduWrap):
        d -= vcduWrap
    return d


class CurrentLoadException (Exception):
    pass


class CurrentLoad (object):
    """Monitor expected state in load review products.
    """
    # Class interface:
    # Constructor requires the path to the load review directory.
    # CheckForNewLoads () needs to be called periodically to ensure that new
    # loads are found.
    # Use getStateAtTime () or getStateAtVCDU () to obtain the expected
    # state vector.  These calls also update the loads when required.

    # VCDU rolls over every 49.76 days, making search by VCDU unambiguous,
    # provided that the duration of the loads is less than half of this.

    # Maximum look back time in days for the initial load set (allow for
    # two week loads)
    datespan = 21
    # Format of date-time tags
    gmtre = re.compile(r'(\d{4}:\d{3}:\d{2}:\d{2}:\d{2}\.\d{3})')
    # Load review directory name format.
    # NB: We do not countenance loads later than N (L is the record
    # and later letters are test loads).
    lrdre = re.compile(r'(.*/\d{4}:\d{3}:([A-N])-[A-Z]{3}\d{4}\2)')
    # Index of first new state vector entry in .svrdb
    # (index 40 is copied from the preceding .svrdb)
    startIndex = 40

    def getRecentLoads(self, refFile):
        """Identify load directories more recent than the reference file.
        """
        # All recently modified directories
        cmdargs = ["find", self.loadReviewDir, "-maxdepth", "1", "-newer",
                   refFile, "-a", "-type", "d"]
        x = sp.Popen(cmdargs, stdout=sp.PIPE, stderr=sp.PIPE)
        output, errout = x.communicate()
        if x.returncode:
            raise CurrentLoadException(errout)
        rawlist = output.split('\n')
        # Filter for the load review directory name format
        dirlist = []
        for t in rawlist:
            mo = CurrentLoad.lrdre.match(t)
            if mo:
                dirlist.append(mo.group(1))
        # Only keep the latest loads for each date
        pruned = dict()
        for t in dirlist:
            lrdate = t[-8:-1]
            if (not lrdate in pruned) or pruned[lrdate] < t:
                pruned[lrdate] = t
        dirlist = list(pruned.values())
        # Sort with most recent first
        dirlist.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        # NB: List of fully qualified path names
        return dirlist

    def startTime(self, loadDir):
        """Get time of first command for the specified loads.
        """
        hrcselPath = loadDir + '/' + loadDir[-8:] + ".combined.hrcsel"
        with open(hrcselPath) as hrcsel:
            for t in hrcsel:
                mo = CurrentLoad.gmtre.match(t)
                if mo:
                    return mo.group(1)
        return None

    def firstTimeVCDU(self, loadDir):
        """Get time and vcdu of the initial state vector for specified loads.
        """
        svrdbPath = loadDir + '/' + loadDir[-8:] + ".svrdb"
        with open(svrdbPath) as svrdb:
            for t in svrdb:
                mo = CurrentLoad.gmtre.match(t)
                if mo:
                    tt, vv = t.split()[:2]
                    return tt, int(vv)
        return None

    def slurpState(self, dir):
        """Read state vectors for specified loads.
        """
        svrdbPath = dir + '/' + dir[-8:] + '.svrdb'
        with open(svrdbPath) as svrdb:
            self.svrdb = [t.rstrip() for t in svrdb]
        self.currentStateIndex = CurrentLoad.startIndex
        self.currentLoads = dir
        self.nextLoads = None

    def checkForNewLoads(self):
        """Note the presence of newer loads.
        """
        # Needs to be run periodically - before the new loads commence
        if self.nextLoads:
            dirlist = self.getRecentLoads(self.nextLoads)
        else:
            dirlist = self.getRecentLoads(self.currentLoads)
        if len(dirlist):
            # Assume no more than one new set can be waiting
            nextLoads = dirlist[0]
            # Time of first command in the new loads
            nextStart = self.startTime(nextLoads)
            # Time and VCDU of first entry in the new .svrdb
            # (usually the last entry from the previous loads)
            firstTime, firstVCDU = self.firstTimeVCDU(nextLoads)
            # Discard failures
            if nextStart and firstVCDU:
                self.nextLoads = nextLoads
                self.nextStart = nextStart
                self.nextFirstTime = firstTime
                self.nextFirstVCDU = firstVCDU

    def __init__(self, loadReviewDir):
        self.loadReviewDir = loadReviewDir
        # Get recent load reviews
        earlier = time.time() - CurrentLoad.datespan * 3600 * 24
        tfhandle, tfname = tf.mkstemp(dir=loadReviewDir)
        os.utime(tfname, (earlier, earlier))
        dirlist = self.getRecentLoads(tfname)
        os.close(tfhandle)
        os.unlink(tfname)
        # Identify the most recent loads that start before the present
        current_time = datetime.utcnow().strftime("%Y:%j:%H:%M:%S.%f")[:-3]
        currentLoads = None
        for d in dirlist:
            start_time = self.startTime(d)
            if start_time and start_time <= current_time:
                currentLoads = d
                break
        if currentLoads is None:
            raise CurrentLoadException("Failed to locate active loads")
        self.slurpState(currentLoads)
        self.checkForNewLoads()

    def stateTime(self, k):
        """Time tag of state vector string at k.
        """
        return self.svrdb[k][:21]

    def stateVCDU(self, k):
        """VCDU value from state vector string at k.
        """
        return int(self.svrdb[k].split()[1])

    def checkAndChangeLoads(self, atTime, atVCDU):
        """When required, switch to a new set of loads.
        """
        # Allow atVCDU to be zero
        klast = len(self.svrdb) - 1
        if (not atVCDU is None and (atVCDU >= self.nextFirstVCDU
                                    or atVCDU >= self.stateVCDU(klast))
            or
            atTime and (atTime >= self.nextFirstTime
                        or atTime >= self.stateTime(klast))):
            # New loads should be ready
            if self.nextLoads:
                # Switch to new loads
                self.slurpState(self.nextLoads)
                self.checkForNewLoads()
            else:
                raise CurrentLoadException("No new loads found")

    def getStateAtTime(self, atTime):
        """Get the state vector at the specified time using linear search.
        """
        self.checkAndChangeLoads(atTime, None)
        if atTime >= self.stateTime(self.currentStateIndex):
            i = self.currentStateIndex + 1
            while i < len(self.svrdb):
                if self.stateTime(i) > atTime:
                    break
                i += 1
            i -= 1
        else:
            i = self.currentStateIndex - 1
            while i > CurrentLoad.startIndex:
                if self.stateTime(i) <= atTime:
                    break
                i -= 1
        self.currentStateIndex = i
        return self.svrdb[self.currentStateIndex]

    def getStateAtVCDU(self, atVCDU):
        """Get the state vector at the specified VCDU by linear search.
        """
        self.checkAndChangeLoads(None, atVCDU)
        vcdu = self.stateVCDU(self.currentStateIndex)
        dvcdu = deltaVCDU(vcdu, atVCDU)
        if dvcdu >= 0:
            i = self.currentStateIndex + 1
            while i < len(self.svrdb):
                vcdu = self.stateVCDU(i)
                dvcdu = deltaVCDU(vcdu, atVCDU)
                if dvcdu < 0:
                    break
                i += 1
            i -= 1
        else:
            i = self.currentStateIndex - 1
            while i > CurrentLoad.startIndex:
                vcdu = self.stateVCDU(i)
                dvcdu = deltaVCDU(vcdu, atVCDU)
                if dvcdu >= 0:
                    break
                i -= 1
        self.currentStateIndex = i
        return self.svrdb[self.currentStateIndex]

    def binaryGetStateAtTime(self, atTime):
        """Get the state vector at the specified time by binary search.
        """
        self.checkAndChangeLoads(atTime, None)

        klo = CurrentLoad.startIndex
        khi = len(self.svrdb)
        while khi > klo + 1:
            k = (klo + khi) / 2
            if atTime >= self.stateTime(k):
                klo = k
            else:
                khi = k
        self.currentStateIndex = klo
        return self.svrdb[self.currentStateIndex]

    def binaryGetStateAtVCDU(self, atVCDU):
        """Get the state vector at the specified VCDU.
        """
        self.checkAndChangeLoads(None, atVCDU)

        klo = CurrentLoad.startIndex
        khi = len(self.svrdb)
        while khi > klo + 1:
            k = (klo + khi) / 2
            vcdu = self.stateVCDU(k)
            if deltaVCDU(vcdu, atVCDU) >= 0:
                klo = k
            else:
                khi = k
        self.currentStateIndex = klo
        return self.svrdb[self.currentStateIndex]


############################################################
if __name__ == "__main__":
    import sys

    lrd = "/d0/hrc/occ/mp"
    loads = CurrentLoad(lrd)
    print("Found", loads.currentLoads)
    os.environ["TZ"] = "UTC"
    ttry = datetime.now().strftime("%Y:%j:%H:%M:%S.%f")[:-3]
    print(loads.getStateAtTime(ttry))
    kk = loads.currentStateIndex
    for i in range(kk - 1, kk + 2):
        print(loads.svrdb[i])
    loads.binaryGetStateAtTime(ttry)
    jj = loads.currentStateIndex
    print("Should be the same for the time:", kk, jj)

    if len(sys.argv) > 1:
        vcdu = int(sys.argv[1])
        print("Using vcdu", vcdu)
        print(loads.getStateAtVCDU(vcdu))
        kk = loads.currentStateIndex
        for i in range(kk - 1, kk + 2):
            print(loads.svrdb[i])
        loads.binaryGetStateAtVCDU(vcdu)
        jj = loads.currentStateIndex
        print("Should be the same for vcdu:", kk, jj)
