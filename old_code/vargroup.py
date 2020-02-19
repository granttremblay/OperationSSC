# -*- coding: utf-8 -*-
# The content and structure of the display tables is defined completely
# here.


from varitem import factory, startUpdate


class VarGroup(object):
    """One titled subtable within a display table.
    title = heading for the collection
    entries = VarItem list for display
    """

    def __init__(self, title, entries):
        self.title = title
        self.entries = entries

    def update(self):
        """Update all VarItems in this table.
        """
        for v in self.entries:
            v.setVal()


def mkVIlist(itemPars, labclass='labdef'):
    """Make a list of VarItems from a list of (name, label, tooltip) tuples,
    for building VarGroups.
    """
    return [factory(t[0], t[1], labclass) if len(t) == 2 else
            factory(t[0], t[1], labclass, t[2]) for t in itemPars]


class RTCADSTable(object):
    """"One complete table of the RTCADS display.
    caption = table title
    rtc_table = list of lists of VarGroups
    """

    def __init__(self, caption, rtc_table):
        self.caption = caption
        self.rtc_table = rtc_table

    def update(self):
        """Update all VarItems for one RTCADSTable.
        """
        # Update each VarGroup in the list of lists
        for col in self.rtc_table:
            for vg in col:
                vg.update()


def updateAll(tabList):
    """Updates all display items.
    tabList = listof RTTCADSTables for the display
    """
    startUpdate()
    # Each entry is an RTCADSTables
    for table in tabList:
        table.update()


############################################################
# Define every VarGroup in the display

vgIOStat = VarGroup('I/O Status',
                    mkVIlist([('YDHHMMSS', 'YDHHMMSS'),
                              ('VCDU', 'VCDU'),
                              ('format', 'fmt:Mfn:mfn'),
                              ('alarmfile', 'Alarm_File')]))

vgSSDrates = VarGroup('SSD Rates Status',
                      mkVIlist([('R_tevt1', 'TE Rate 1 (c/s)', '2TLEV1RT'),
                                ('R_tevt2', 'TE Rate 2 (c/s)', '2TLEV2RT'),
                                ('R_vevt1', 'VE Rate 1 (c/s)', '2VLEV1RT'),
                                ('R_vevt2', 'VE Rate 2 (c/s)', '2VLEV2RT'),
                                ('R_shield1', 'SH Rate 1 (c/s)', '2SHEV1RT'),
                                ('R_shield2', 'SH Rate 2 (c/s)', '2SHEV2RT')
                                ]))

vgEEDmech = \
    VarGroup('EED Mechanisms Status',
             mkVIlist([('S_PriDoorLSOpen', 'Door LS Open ([un]act)',
                        '2DRLSOP'),
                       ('S_PriDoorLSClosed', 'Door LS Closed ([un]act)',
                        '2DRLSCL'),
                       ('S_PYShutterLSHome', '+Y Shut LS Home ([un]act)',
                        '2PYLSHM'),
                       ('S_PYShutterLSMax', '+Y Shut LS Max ([un]act)',
                        '2PYLSMX'),
                       ('S_MYShutterLSHome', '-Y Shut LS Home ([un]act)',
                        '2MYLSHM'),
                       ('S_MYShutterLSMax', '-Y Shut LS Max ([un]act)',
                        '2MYLSMX'),
                       ('S_CalSrcPriLSHome', 'Cal Src LS Home ([un]act)',
                        '2CSLSHM'),
                       ('S_CalSrcPriLSMax', 'Cal Src LS Max ([un]act)',
                        '2CSLSMX')]))

vgEEDbus_main = \
    VarGroup('EED Bus Select',
             mkVIlist([('S_BusSelCeaALvps', 'CEA-A LVPS Bus (A|B)',
                        '2C1LVBS'),
                       ('S_BusSelCeaBLvps', 'CEA-B LVPS Bus (A|B)',
                        '2C2LVBS'),
                       ('S_BusSelImgHvps',  'IM HVPS Bus (A|B)', '2IMHVBS'),
                       ('S_BusSelSpecHvps', 'SP HVPS Bus (A|B)', '2SPHVBS'),
                       ('S_BusSelShldAHvps', 'Shld 1 HVPS Bus (A|B)',
                        '2S1HVBS'),
                       ('S_BusSelShldBHvps', 'Shld 2 HVPS Bus (A|B)',
                        '2S2HVBS'),
                       ('S_ShldPmtSelPreampA', 'Shld Pmt Preamp A (1|2)',
                        '2ELEASS'),
                       ('S_ShldPmtSelPreampB', 'Shld Pmt Preamp B (1|2)',
                        '2ELEBSS'),
                       ('S_DetSelPreampA', 'Det Preamps A (I|S)', '2PREADS'),
                       ('S_DetSelPreampB', 'Det Preamps B (I|S)', '2PREBDS')
                       ]))

vgEEDfs_main = \
    VarGroup('EED Fail Safes',
             mkVIlist([('S_FailSafeMasterRelay', 'FS Master Relay (ena|dis)',
                        '2FSMRST'),
                       ('S_FailSafeCalSrcRelay', 'FS CalSrc Relay (ena|dis)',
                        '2FSCAST'),
                       ('S_FailSafePYShutter', 'FS +Y Shutter (ena|dis)',
                        '2FSPYST'),
                       ('S_FailSafeMYShutter', 'FS -Y Shutter (ena|dis)',
                        '2FSNYST')]))

vgSSDmech_main = \
    VarGroup('SSD Mechanisms Status',
             mkVIlist([('SelectedMotor', 'Selected Motor'),
                       ('M_StatMvStepsA', 'Mv Twd Close (move|stop)',
                        '2MSNAAMD'),
                       ('M_StatMvStepsB', 'Mv Twd Open (move|stop)',
                        '2MSNBAMD'),
                       ('M_ClosPriLSActive', 'Pri LS Close ([un]act)',
                        '2CPLSAAC'),
                       ('M_ClosSecLSActive', 'Sec LS Close ([un]act)',
                        '2CSLSAAC'),
                       ('M_OpenPriLSActive', 'Pri LS Open ([un]sct)',
                        '2OPLSAAC'),
                       ('M_OpenSecLSActive', 'Sec LS Open ([un]act)',
                        '2OSLSAAC')]))

vgEEDlvps_main = \
    VarGroup('EED LVPS Status',
             mkVIlist([('S_P5LvpsA', '+5V LVPS A (on|off)', '2PS5AON'),
                       ('SecP5BusVA', 'Sec +5V Bus A (V)', '2P05VAVL'),
                       ('SecP15BusVA', 'Sec +15V Bus A (V)', '2P15VAVL'),
                       ('SecM15BusVA', 'Sec -15V Bus A (V)', '2N15VAVL'),
                       ('SecP24BusVA', 'Sec +24V Bus A (V)', '2P24VAVL'),
                       ('S_P5LvpsB', '+5V LVPS B (V)', '2PS5BON'),
                       ('SecP5BusVB', 'Sec +5V Bus B (V)', '2P05VBVL'),
                       ('SecP15BusVB', 'Sec +15V Bus B (V)', '2P15VBVL'),
                       ('SecM15BusVB', 'Sec -15V Bus B (V)', '2N15VBVL'),
                       ('SecP24BusVB', 'Sec +24V Bus B (V)', '2P24VBVL')]))

vgSSDlvps = \
    VarGroup('SSD LVPS Status',
             mkVIlist([('S_LvpsP24', '+24V LVPS (on|off)', '224PCAST'),
                       ('S_LvpsP15', '+15V LVPS (on|off)', '215PCAST'),
                       ('S_LvpsM15', '-15V LVPS (on|off)', '215NCAST'),
                       ('P5BusMon', '+5V Bus Voltage (V)', '2C05PALV'),
                       ('P15BusMon', '+15V Bus Voltage (V)', '2C15PALV'),
                       ('M15BusMon', '-15V Bus Voltage (V)', '2C15NALV'),
                       ('P24BusMon', '+24V Bus Voltage (V)', '2C24PALV'),
                       ('PrimBusI', 'Primary Bus Cur (dn)', '2PRBSCR'),
                       ('PrimBusV', 'Primary Bus Voltage (V)', '2PRBSVL')]))

vgSSDdoor = VarGroup('SSD Door Mv Status',
                     mkVIlist([('M_CmdMvStepsA', 'Mov Nsteps to A (closed)',
                                '2MCNAAMD'),
                               ('M_CmdMvStepsB', 'Mov Nsteps to B (open)',
                                '2MCNBAMD')]))

vgSSDimagerhvps = \
    VarGroup('SSD Imager HVPS Status',
             mkVIlist([('S_ImHvps', 'IM HVPS (on|off)', '2IMONST'),
                       ('S_ImHvpsCurLim', 'IM HVPS I Lim (ena|dis)',
                        '2IMCLST'),
                       ('V_ImTopHV', 'IM Top HV Mon (dn)', '2IMHBLV'),
                       ('V_ImBotHV', 'IM Bot HV Mon (dn)', '2IMHVLV'),
                       ('V_ImTopHVStep', 'IM Top HV Step (step)', '2IMTPAST'),
                       ('V_ImBotHVStep', 'IM Bot HV Step (step)', '2IMBPAST')]))

vgSSDspechvps = \
    VarGroup('SSD Spect HVPS Status',
             mkVIlist([('S_SpHvps', 'SP HVPS (on|off)', '2SPONST'),
                       ('S_SpHvpsCurLim', 'SP HVPS I Lim (ena|dis)',
                        '2SPCLST'),
                       ('V_SpTopHV', 'SP Top HV Mon (dn)', '2SPHBLV'),
                       ('V_SpBotHV', 'SP Bot HV Mon (dn)', '2SPHVLV'),
                       ('V_SpTopHVStep', 'SP Top HV Step (step)', '2SPTPAST'),
                       ('V_SpBotHVStep', 'SP Bot HV Step (step)', '2SPBPAST')
                       ]))

vgSSDshieldhvps = \
    VarGroup('SSD Shield HVPS Status',
             mkVIlist([('S_ShldA', 'Shld 1 (on|off)', '2S1ONST'),
                       ('V_ShldAHV', 'Shld 1 HV Mon (dn)', '2S1HVLV'),
                       ('V_ShldAHVStep', 'Shld 1 HV Step (step)', '2S1HVST'),
                       ('S_ShldB', 'Shld 2 (on|off)', '2S2ONST'),
                       ('V_ShldBHV', 'Shld 2 HV Mon (dn)', '2S2HVLV'),
                       ('V_ShldBHVStep', 'Shld 2 HV Step (step)', '2S2HVST')
                       ]))

vgIOaltstat = VarGroup('I/O Status',
                       mkVIlist([('YDHHMMSSalt', 'YDHHMMSS'),
                                 ('VCDUalt', 'VCDU'),
                                 ('formatalt', 'fmt:Mfn:mfn'),
                                 ('alarmfilealt', 'Alarm_File')],
                                'alter'))

vgSSDmech_ssd = \
    VarGroup('SSD Mechanisms Status',
             mkVIlist([('SelectedMotor', 'Selected Motor'),
                       ('M_StatMvStepsA', 'Mv Twd Close (move|stop)',
                        '2MSNAAMD'),
                       ('M_StatMvStepsB', 'Mv Twd Open (move|stop)',
                        '2MSNBAMD'),
                       ('M_ClosPriLSActive', 'Pri LS Close ([un]act)',
                        '2CPLSAAC'),
                       ('M_ClosSecLSActive', 'Sec LS Close ([un]act)',
                        '2CSLSAAC'),
                       ('M_OpenPriLSActive', 'Pri LS Open ([un]sct)',
                        '2OPLSAAC'),
                       ('M_OpenSecLSActive', 'Sec LS Open ([un]act)',
                        '2OSLSAAC'),
                       ('M_OverCurEnable', 'Over Cur Enable', '2DROIAST'),
                       ('M_OverTempEnable', 'Over Temp Enable', '2DROTAST'),
                       ('M_Direction', 'Motor Direction (dn)', '2MDIRAST')]))

vgSSDmotor = \
    VarGroup('SSD Motor Status',
             mkVIlist([('M_LastStepCtr', 'Last Step Ctr (steps)', '2SCTHAST'),
                       ('M_OverCurFlag', 'Over Cur Flag', '2SMOIAST'),
                       ('M_OverTempFlag', 'Over Temp Flag', '2SMOTAST'),
                       ('M_SensorPwrEnable', 'Pwr Sensor Enable'),
                       ('M_StopFlagEnable', 'Stop Enable Flag', '2SFLGAST'),
                       ('M_OpenPriLSEnable', 'Pri LS Open Enable',
                        '2OPLSAST'),
                       ('M_ClosPriLSEnable', 'Pri LS Close Enable',
                        '2CPLSAST'),
                       ('M_OpenSecLSEnable', 'Sec LS Open Enable',
                        '2OSLSAST'),
                       ('M_ClosSecLSEnable', 'Sec LS Close Enable',
                        '2CSLSAST'),
                       ('M_OpenSecLSDetected', 'Sec LS Open Det',
                        '2OSLSADT'),
                       ('M_ClosSecLSDetected', 'Sec LS Close Det ([un]act)',
                        '2CSLSADT'),
                       ('M_StatCtrlReset', 'StatCtrlReset', '2MSMDARS'),
                       ('M_StatMvLSB', 'M_StatMvLSB', '2MSLBAMD'),
                       ('M_StatMvLSA', 'M_StatMvLSA', '2MSLAAMD'),
                       ('M_StatMvPosReg', 'StatMvPosReg', '2MSPRAMD'),
                       ('M_StatMEnable', 'StatMEnable', '2MSDRAMD'),
                       ('M_CmdCtrlReset', 'CmdCtrlReset', '2MCMDARS'),
                       ('M_CmdMvStepsB', 'Mov Nsteps to B (open)',
                        '2MCNBAMD'),
                       ('M_CmdMvStepsA', 'Mov Nsteps to A (closed)',
                        '2MCNAAMD'),
                       ('M_CmdMvLSB', 'CmdMvLSB', '2MCLBAMD'),
                       ('M_CmdMvLSA', 'CmdMvLSA', '2MCLAAMD'),
                       ('M_CmdMvPosReg', 'CmdMvPosReg', '2MCPRAMD'),
                       ('M_CmdMEnable', 'CmdMEnable', '2MDRVAST')]))

vgSSDtemp = VarGroup('SSD Temperatures',
                     mkVIlist([('T_FePreAmp', 'FEA PreAmp (C)', '2FEPRATM'),
                               ('T_Spec', 'SP Det (C)', '2SPINATM'),
                               ('T_Img', 'IM Det (C)', '2IMINATM'),
                               ('T_Lvps', 'LVPS (C)', '2LVPLATM'),
                               ('T_SpHvps', 'SP HVPS (C)', '2SPHVATM'),
                               ('T_ImHvps', 'IM HVPS (C)', '2IMHVATM'),
                               ('T_Motor', 'Selected Motor (C)', '2SMTRATM'),
                               ('T_FEA', 'FEA (C)', '2FE00ATM'),
                               ('T_CEA0', 'CEA0 (C)', '2CE00ATM'),
                               ('T_CEA1', 'CEA1 (C)', '2CE01ATM')]))

vgevtproc = \
    VarGroup('Event Processing State',
             mkVIlist([('ForcedCpU', 'Forced CP-U (tap)', '2FCPUAST'),
                       ('ForcedCpV', 'Forced CP-V (tap)', '2FCPVAST'),
                       ('CenterBlankHiCpU', 'Center Blank Hi CP-U (tap)',
                        '2CBHUAST'),
                       ('CenterBlankLoCpU', 'Center Blank Lo CP-U (tap)',
                        '2CBLUAST'),
                       ('CenterBlankHiCpV', 'Center Blank Hi CP-V (tap)',
                        '2CBHVAST'),
                       ('CenterBlankLoCpV', 'Center Blank Lo CP-V (tap)',
                        '2CBLVAST'),
                       ('WidthThreshold', 'Width Threshold (tap)',
                        '2WDTHAST'),
                       ('CalModeEnable', 'Cal Mode (ena|dis)', '2CLMDAST'),
                       ('S_InstSelect', 'Spec Det Mode (spec|img)',
                        '2SPMDASL'),
                       ('S_ObsMode', 'Observing Mode (obs|nil)', '2OBNLASL'),
                       ('DataFifoEnable', 'Data FIFO (ena|dis)', '2FIFOAVR'),
                       ('EdgeBlkValidityEnable',
                        'Edge Blk Validity (ena|dis)', '2EBLKAVR'),
                       ('CtrBlkValidityEnable', 'Ctr Blk Validity (ena|dis)',
                        '2CBLKAVR'),
                       ('ULDValidityEnable', 'ULD Validity (ena|dis)',
                        '2ULDIAVR'),
                       ('WidValidityEnable', 'Width Validity (ena|dis)',
                        '2WDTHAVR'),
                       ('ShldValidityEnable', 'Shld Validity (ena|dis)',
                        '2SHLDAVR'),
                       ('ULD', 'UL Disc (dn)', '2ULDIALV'),
                       ('LLD', 'LL Disc (dn)', '2LLDIALV'),
                       ('CalPulserSetting', 'Cal Pulser Setting (dn)',
                        '2CALPALV'),
                       ('GridBias', 'Grid Bias (dn)', '2GRDVALV'),
                       ('RangeSwSetting',  'Range Sw Setting (dn)',
                        '2RSRFALV')]))

vgEEDratehob = \
    VarGroup('EED HOB Rates (AA)',
             mkVIlist([('DetEvtRateA', 'Det Evt Rate A HOB (c/s)', '2DETART'),
                       ('DetEvtRateB', 'Det Evt Rate B HOB (c/s)', '2DETBRT'),
                       ('ShldEvtRateA', 'Shld Evt Rate A HOB (c/s)',
                        '2SHLDART'),
                       ('ShldEvtRateB', 'Shld Evt Rate B HOB (c/s)',
                        '2SHLDBRT')]))

vgEEDfs_eed = \
    VarGroup('EED Fail Safes (BL)',
             mkVIlist([('S_FailSafeMasterRelay', 'FS Master Relay (ena|dis)',
                        '2FSMRST'),
                       ('S_FailSafeCalSrcRelay', 'FS CalSrc Relay (ena|dis)',
                        '2FSCAST'),
                       ('S_FailSafePYShutter', 'FS +Y Shutter (ena|dis)',
                        '2FSPYST'),
                       ('S_FailSafeMYShutter', 'FS -Y Shutter (ena|dis)',
                        '2FSNYST')]))

# RTCADS displays these with alarm settings off in the EED window, as the
# variables are omitted from @ED_List.  However, their alarms status is
# shown in the main window, so there is no reason to omit them here.
vgEEDls = \
    VarGroup('EED LS Status (BL)',
             mkVIlist([('S_PriDoorLSOpen', 'Door LS Open ([un]act)',
                        '2DRLSOP'),
                       ('S_PriDoorLSClosed', 'Door LS Closed ([un]act)',
                        '2DRLSCL'),
                       ('S_PYShutterLSHome', '+Y Shut LS Home ([un]act)',
                        '2PYLSHM'),
                       ('S_PYShutterLSMax', '+Y Shut LS Max ([un]act)',
                        '2PYLSMX'),
                       ('S_MYShutterLSHome', '-Y Shut LS Home ([un]act)',
                        '2MYLSHM'),
                       ('S_MYShutterLSMax', '-Y Shut LS Max ([un]act)',
                        '2MYLSMX'),
                       ('S_CalSrcPriLSHome', 'Cal Src LS Home ([un]act)',
                        '2CSLSHM'),
                       ('S_CalSrcPriLSMax', 'Cal Src LS Max ([un]act)',
                        '2CSLSMX')]))

vgEEDbus_eed = \
    VarGroup('EED Bus Select (BL)',
             mkVIlist([('S_BusSelCeaALvps', 'CEA-A LVPS Bus (A|B)',
                        '2C1LVBS'),
                       ('S_BusSelCeaBLvps', 'CEA-B LVPS Bus (A|B)',
                        '2C2LVBS'),
                       ('S_BusSelImgHvps', 'IM HVPS Bus (A|B)', '2IMHVBS'),
                       ('S_BusSelSpecHvps', 'SP HVPS Bus (A|B)', '2SPHVBS'),
                       ('S_BusSelShldAHvps', 'Shld 1 HVPS Bus (A|B)',
                        '2S1HVBS'),
                       ('S_BusSelShldBHvps', 'Shld 2 HVPS Bus (A|B)',
                        '2S2HVBS'),
                       ('S_ShldPmtSelPreampA', 'Shld Pmt Preamp A (1|2)',
                        '2ELEASS'),
                       ('S_ShldPmtSelPreampB', 'Shld Pmt Preamp B (1|2)',
                        '2ELEBSS'),
                       ('S_DetSelPreampA', 'Det Preamps A (I|S)', '2PREADS'),
                       ('S_DetSelPreampB', 'Det Preamps B (I|S)', '2PREBDS')
                       ]))

vgEEDlvps_eed = \
    VarGroup('EED LVPS (AA)',
             mkVIlist([('SecP5BusVA', 'Sec +5V Bus A (V)', '2P05VAVL'),
                       ('SecP15BusVA', 'Sec +15V Bus A (V)', '2P15VAVL'),
                       ('SecM15BusVA', 'Sec -15V Bus A (V)', '2N15VAVL'),
                       ('SecP24BusVA', 'Sec +24V Bus A (V)', '2P24VAVL'),
                       ('SecP5BusVB', 'Sec +5V Bus B (V)', '2P05VBVL'),
                       ('SecP15BusVB', 'Sec +15V Bus B (V)', '2P15VBVL'),
                       ('SecM15BusVB', 'Sec -15V Bus B (V)', '2N15VBVL'),
                       ('SecP24BusVB', 'Sec +24V Bus B (V)', '2P24VBVL')]))

vgEEDtemps = \
    VarGroup('EED Temperatures (PS)',
             mkVIlist([('T_Pmt1', 'Pmt1 (C)', '2PMT1T'),
                       ('T_Pmt2', 'Pmt2 (C)', '2PMT2T'),
                       ('T_OutDet1', 'OutDet1 (C)', '2DTSTATT'),
                       ('T_OutDet2', 'OutDet2 (C)', '2DCENTRT'),
                       ('T_FEABox', 'FEABox (C)', '2FHTRMZT'),
                       ('T_CEABox', 'CEABox (C)', '2CHTRPZT'),
                       ('T_PY', '+Y (C)', '2FRADPYT'),
                       ('T_MY', '-Y (C)', '2CEAHVPT'),
                       ('T_Conduit', 'Conduit (C)', '2CONDMXT'),
                       ('T_Snout', 'Snout (C)', '2UVLSPXT')]))


############################################################
# Define each of the three primary display tables.  One table
# is represented as a list of columns, each of which is a
# list of VarGroups.

# Main display
main_data = [[vgIOStat, vgSSDrates, vgEEDmech],
             [vgEEDbus_main, vgEEDfs_main, vgSSDmech_main],
             [vgEEDlvps_main, vgSSDlvps, vgSSDdoor],
             [vgSSDimagerhvps, vgSSDspechvps, vgSSDshieldhvps]]

# Secondary Science display
ssd_data = [[vgIOaltstat, vgSSDrates, vgSSDmech_ssd],
            [vgSSDmotor],
            main_data[3],
            [vgSSDlvps, vgSSDtemp],
            [vgevtproc]]

# Engineering data
eed_data = [[vgIOStat, vgEEDratehob, vgEEDfs_eed],
            [vgEEDls, vgEEDbus_eed],
            [vgEEDlvps_eed, vgEEDtemps]]


############################################################
# Finally, define the list of primary tables
rtctabList = [RTCADSTable('Main Display', main_data),
              RTCADSTable('Secondary Science Data', ssd_data),
              RTCADSTable('Engineering Data', eed_data)]
