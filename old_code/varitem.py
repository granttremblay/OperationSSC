#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Jinja2 prefers the line above (strings default to unicode).
#
# Manage a single data item for display.

import sys
import re
import time
from access import readSHMdata
from alarms import readAlarmFile


class VarItem(object):
    """Represents one displayed item, including its label, value and
    formatting info.  
    """
    # The cycle count is used to prevent data items from being evaluated
    # more than once per shared memory read cycle.  This is essential for
    # some values, notably time and VCDU.

    # Values read from shared memory
    data_dict = dict()
    # Maps varname to VarItem
    allItems = dict()
    # Evaluation cycle number
    cycle = 0
    # Current alarm file
    alarmFileName = ""
    # Alarm settings
    alarmSet = None

    def __init__(self, varname, label, labclass, tooltip=None):
        # Invariants
        self.varname = varname
        self.label = label
        self.labclass = labclass
        self.tooltip = tooltip
        # Cycle when this item was last updated
        self.cycle = 0

    def _setTime(self):
        """Current time display item.
        """
        self.state = 'off'
        self.value = time.strftime("%Y%j.%H%M%S")

    def _setVCDU(self):
        """VCDU display item.
        """
        MF = int(VarItem.data_dict['MF'][0])
        mf = int(VarItem.data_dict['mf'][0])
        VCDU = 128 * MF + mf
        # Note that self.value is initially undefined
        if self.cycle > 0 and int(self.value) == VCDU:
            self.state = 'bad'
        else:
            self.value = '{}'.format(VCDU)
            fmt = int(VarItem.data_dict['fmt'][0])
            if fmt == 0:
                self.state = 'good'
            else:
                self.state = 'warn'

    def _fmtMFmf(self):
        """Format and frame numbers display item.
        """
        MF = int(VarItem.data_dict['MF'][0])
        mf = int(VarItem.data_dict['mf'][0])
        fmt = int(VarItem.data_dict['fmt'][0])
        self.state = 'off'
        self.value = '{}({}):{}:{}'.format(fmt + 1, fmt, MF, mf)

    def _setAlarms(self):
        """Select and load an alarm file and provide alarm file display item.
        """
        # Guess which alarm file to use.
        if VarItem.data_dict['S_DetSelPreampA'][1] == 0:
            if VarItem.data_dict['EdgeBlkValidityEnable'][1]:
                alarmFileName = 'HRC-I-TRIM.alarm'
            else:
                alarmFileName = 'HRC-I.alarm'
        elif VarItem.data_dict['S_InstSelect'][1] == 1:
            alarmFileName = 'HRC-S.alarm'
        else:
            alarmFileName = 'HRC-S-IMG.alarm'
        if alarmFileName != VarItem.alarmFileName:
            # Read the alarm file only if it has changed
            alarmFilePath = '/d0/hrc/src/rtcads/ALARMS'
            VarItem.alarmSet = readAlarmFile(alarmFilePath, alarmFileName)
            VarItem.alarmFileName = alarmFileName
        self.state = 'off'
        self.value = alarmFileName

    def _altSetter(self, basename):
        """Copy values time, VCDU, fmt:FM:fm and alarmfile to the
        alternative VarItems for these.
        """
        x = VarItem.allItems[basename]
        # Paranoia - test that the value has already been updated
        if x.cycle != VarItem.cycle:
            print('VarItem._altsetter: primary unset:', basename)
            sys.exit(1)
        self.state = x.state
        self.value = x.value

    def _altTime(self):
        self._altSetter('YDHHMMSS')

    def _altVCDU(self):
        self._altSetter('VCDU')

    def _altfmtMFmf(self):
        self._altSetter('format')

    def _altAlarms(self):
        self._altSetter('alarmfile')

    def _setSelMotor(self):
        """Selected motor display item - depends on several values from
        the telemetry stream.  The logic here matches that in RTCADS,
        although it looks inefficient.
        """
        self.state = 'off'
        self.value = 'NA'
        testset = [('M_CalSrcSel', 'CalSrc', 'warn'),
                   ('M_DoorSel', 'Door', 'warn'),
                   ('M_MYShutterSel', '-Y', 'warn'),
                   ('M_PYShutterSel', '+Y', 'warn'),
                   ('M_AllMotorsUnSel', 'None', 'good')]
        alarmactive = VarItem.alarmSet['SelectedMotor']['status']
        for (var, res, s) in testset:
            if 'Sel' == VarItem.data_dict[var][0]:
                self.value = res
                if alarmactive:
                    self.state = s

    def _alarmState(self, conval):
        """Determine alarm state - general case.
        """
        self.state = 'off'
        alarms = VarItem.alarmSet[self.varname]
        if alarms['status']:
            if conval <= alarms['rll'] or conval >= alarms['rul']:
                self.state = 'bad'
            elif conval <= alarms['yll'] or conval >= alarms['yul']:
                self.state = 'warn'
            else:
                self.state = 'good'

    # Special case display items
    varSource = {'YDHHMMSS': _setTime,
                 'YDHHMMSSalt': _altTime,
                 'VCDU': _setVCDU,
                 'VCDUalt': _altVCDU,
                 'format': _fmtMFmf,
                 'formatalt': _altfmtMFmf,
                 'alarmfile': _setAlarms,
                 'alarmfilealt': _altAlarms,
                 'SelectedMotor': _setSelMotor}

    # Alarm state for variables that have no alarm file entry
    alarmException = {'M_OverCurEnable': 'good',
                      'M_OverTempEnable': 'good',
                      'M_Direction': 'good',
                      'M_LastStepCtr': 'off',
                      'M_OverCurFlag': 'off',
                      'M_OverTempFlag': 'off',
                      'M_SensorPwrEnable': 'off',
                      'M_StopFlagEnable': 'off',
                      'M_OpenPriLSEnable': 'off',
                      'M_ClosPriLSEnable': 'off',
                      'M_OpenSecLSEnable': 'off',
                      'M_ClosSecLSEnable': 'off',
                      'M_OpenSecLSDetected': 'off',
                      'M_ClosSecLSDetected': 'off',
                      'M_StatCtrlReset': 'off',
                      'M_StatMvLSB': 'off',
                      'M_StatMvLSA': 'off',
                      'M_StatMvPosReg': 'off',
                      'M_StatMEnable': 'off',
                      'M_CmdCtrlReset': 'off',
                      'M_CmdMvLSB': 'off',
                      'M_CmdMvLSA': 'off',
                      'M_CmdMvPosReg': 'off',
                      'M_CmdMEnable': 'off',
                      'DetEvtRateA': 'off',
                      'DetEvtRateB': 'off',
                      'ShldEvtRateA': 'off',
                      'ShldEvtRateB': 'off'}

    def setVal(self):
        """Set value and format of this variable.
        """
        # Ensure value is set only once per cycle
        if self.cycle != VarItem.cycle:
            if not self.varname in VarItem.data_dict:
                # Special case updates
                VarItem.varSource[self.varname](self)
            else:
                # Variables defined in shared memory
                self.value, conval = VarItem.data_dict[self.varname]
                if self.varname in VarItem.alarmSet:
                    # Intercept T_CEA[01] for special handling
                    fmt = int(VarItem.data_dict['fmt'][0])
                    if fmt > 0 and re.match(r'T_CEA[01]', self.varname):
                        self.state = 'off'
                    else:
                        self._alarmState(conval)
                else:
                    # RTCADS sets these once and leaves them alone
                    self.state = VarItem.alarmException[self.varname]
            self.cycle = VarItem.cycle


def factory(varname, label, labclass, tooltip=None):
    """Construct VarItems using this function in order to make
    VarItems with the same varname identical.
    """
    if varname in VarItem.allItems:
        x = VarItem.allItems[varname]
        # Only used during initialization
        if x.label != label or x.labclass != labclass or x.tooltip != tooltip:
            print("factory: collision:", varname, label, labclass, tooltip, \
                x.label, x.labclass, x.tooltip)
            sys.exit(1)
    else:
        x = VarItem(varname, label, labclass, tooltip)
        VarItem.allItems[varname] = x
    return x


def startUpdate():
    """Commence an update cycle.
    """
    VarItem.data_dict = readSHMdata()
    VarItem.cycle += 1


def cycle():
    """Solely for printing.
    """
    return VarItem.cycle
