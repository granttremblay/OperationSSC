# Get data from an alarm file

import re


class BadAlarmFile(Exception):
    pass


def readAlarmFile(path, fname):
    """Alarm limits are stored in a dictionary indexed by variable name.
    """
    alarmfile = path + '/' + fname
    with open(alarmfile) as afh:
        ald = dict()
        for line in afh:
            if not re.match(r'#', line):
                pcs = re.split('\t', line)
                if len(pcs) == 6:
                    ald[pcs[0]] = dict([('status', int(pcs[1])),
                                        ('rll', float(pcs[2])),
                                        ('yll', float(pcs[3])),
                                        ('yul', float(pcs[4])),
                                        ('rul', float(pcs[5]))])
                else:
                    raise BadAlarmFile(alarmfile)
        return ald
    raise BadAlarmFile(alarmfile)
