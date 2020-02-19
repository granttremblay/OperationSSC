#!/usr/bin/env python
""" Access data in the shared memory used by rtcads.
"""

import struct
import sysv_ipc
import re
import sys


# Keys for the two shared memory segments
# tm_shm_key = 100
dpp_shm_key = 101

# Access TM shared memory segment - may want this at some stage
# tm_shm = sysv_ipc.SharedMemory (tm_shm_key)

# Access DPP shared memory segment (telemetry data formatted by the
# data products processor)
dpp_shm = sysv_ipc.SharedMemory(dpp_shm_key)

# Values in DPP shared memory are stored thus:
# struct TMData {
#    float yll, yul, rll, rul, conval;
#    unsigned int dnval, flag, tag;
#    char strval [24], label [24], units [24], comment [24];
# };
TMData_size = 128
TMData_fmt = "5f3I24s24s24s24s"

# Nul terminated C string
cstre = re.compile('([^\x00]*)\x00')


def extract_string(index):
    """Get the label, value string and conval for one parameter (ie TMData) 
    from DPP shared memory.
    index = item ordinal
    """
    tmdata = dpp_shm.read(TMData_size, index * TMData_size)
    # Extract one TMData.  Most entries here are undefined
    (yll, yul, rll, rul, conval, dnval, flag, tag, cstrval, clabel,
     units, comment) = struct.unpack(TMData_fmt, tmdata)
    # Convert C strings
    cstrmo = cstre.match(cstrval)
    if not cstrmo:
        print('Formatted string does not match', cstrval)
        sys.exit(1)
    strval = cstrmo.group(1)
    label = clabel.rstrip('\x00')
    if not strval:
        # Convert items with no strval to a string
        strval = str(dnval)
    return (label, (strval, conval))


# GEN slots: MF, mf, fmt, Spare
GEN_base = 0
GEN_slots = 4
GEN_count = 3

# EVT slots: evt_MF, evt_mf, PSDspStatus, SSDspStatus, EEDspStatus, RD5DspStatus
EVT_base = GEN_base + GEN_slots
EVT_slots = 6
EVT_count = 6

# Secondary science data (SSD).
SSD_base = EVT_base + EVT_slots
SSD_slots = 128
SSD_count = 103

# Engineering data (EED)
EED_base = SSD_base + SSD_slots
EED_slots = 128
EED_count = 48

# These are followed by the image data.


def readSHMdata():
    """Extract strval and conval for all data items in DPP shared
    memory.
    """
    return dict([extract_string(i) for i in
                 list(range(GEN_base, GEN_base + GEN_count))
                 + list(range(EVT_base, EVT_base + EVT_count))
                 + list(range(SSD_base, SSD_base + SSD_count))
                 + list(range(EED_base, EED_base + EED_count))])


############################################################

if __name__ == "__main__":
    data_dict = readSHMdata()
    for x in data_dict:
        print(x, data_dict[x])
