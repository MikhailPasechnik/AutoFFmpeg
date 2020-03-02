# :author: Mikhail Pasechnik, email: michail.goodchild@gmail.com

import re
import os
import glob
import subprocess

try:
    from Deadline.Events import DeadlineEventListener
    from Deadline.Scripting import ClientUtils, RepositoryUtils
except ImportError:
    DeadlineEventListener = object

OPS_MAP = {
    'extension': lambda s: os.path.splitext(os.path.basename(s))[1],
    'basename': lambda s: os.path.splitext(os.path.basename(s))[0].rstrip('#').rstrip('.'),
}
SOURCE_MAP = {
    'info': lambda job, attr: job.GetJobInfoKeyValue(attr),
    'plugin': lambda job, attr: job.GetJobPluginInfoKeyValue(attr),
}


def GetDeadlineEventListener():
    return AutoFFmpeg()


def CleanupDeadlineEventListener(eventListener):
    eventListener.Cleanup()


class AutoFFmpeg(DeadlineEventListener):
    def __init__(self):
        self.OnJobFinishedCallback += self.OnJobFinished

    def Cleanup(self):
        del self.OnJobFinishedCallback

    def OnJobFinished(self, job):
        # Skip job if filtered or no filter
        jobNameFilter = self.GetConfigEntryWithDefault('JobNameFilter', '')
        if not jobNameFilter or not re.match(jobNameFilter, job.JobName):
            return

        pluginNameFilter = self.GetConfigEntryWithDefault('PluginNameFilter', '')
        if not pluginNameFilter or not re.match(pluginNameFilter, job.JobPlugin):
            return

        inputFileName = self.GetConfigEntry('InputFile')
        outputFileName = self.GetConfigEntry('OutputFile')

        # Format tokens
        delimiter = self.GetConfigEntryWithDefault('Delimiter', '').stript().replace(' ', '')
        if len(delimiter) in [1, 2]:
            inputFileName = formatToken(job, getTokens(inputFileName, delimiter), inputFileName)
            outputFileName = formatToken(job, getTokens(outputFileName, delimiter), outputFileName)
        else:
            self.LogWarning('Token Delimiter "%s" should be one or to char long' % delimiter)
            return
            
        # Path mapping
        inputFileName = RepositoryUtils.CheckPathMapping(inputFileName, True)
        outputFileName = RepositoryUtils.CheckPathMapping(outputFileName, True)

        if not os.path.isdir(os.path.dirname(inputFileName)):
            self.LogWarning('No such directory %s' % os.path.dirname(inputFileName))
            return

        if not glob.glob(sequenceToWildcard(inputFileName)):
            self.LogWarning('No file/sequence %s' % inputFileName)
            return
        createFFmpegJob(
            job,
            inputFileName=inputFileName,
            outputFileName=outputFileName,
            outputArgs=self.GetConfigEntryWithDefault('OutputArgs', ''),
            inputArgs=self.GetConfigEntryWithDefault('InputArgs', ''),
            priority=self.GetConfigEntryWithDefault('Priority', '50')
        )
        self.LogInfo('Submitted ffmpeg job with output: {}'.format(outputFileName))

    def GetConfigEntry(self, key, type_=str):
        return self._parseConfig(super(AutoFFmpeg, self).GetConfigEntry(key), type_)

    def GetConfigEntryWithDefault(self, key, default, type_=str):
        return self._parseConfig(super(AutoFFmpeg, self).GetConfigEntryWithDefault(
            key, str(default)), type_
        )

    @staticmethod
    def _parseConfig(value, type_=str):
        if type_ == bool and value in ('true', 'True', '1'):
            return True
        elif type_ == bool and value in ('false', 'False', '0'):
            return False
        return type_(value)


def commandLineSubmit(executable, plugin, info, aux=None):
    """Command line submit

    :param plugin: plugin info file
    :param info: job info file
    :param aux: auxiliary files witch will be transfered to the deadline repository
    :return: job id
    """
    if aux is None:
        aux = []
    cmd = [executable, info, plugin]
    cmd += aux
    process = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()

    if process.returncode != 0:
        # on windows with russian locale set, this works
        out = out.decode('cp1251').replace('\n', '\n\t\t')
        err = err.decode('cp1251').replace('\n', '\n\t\t')
        raise Exception(u'Failed to submit:\n\tCommand:\n\t\t{}\n\tOutput:\n\t\t{}\n\t'
                        u'Errors:\n\t\t{}'.format(cmd, out, err))
    else:
        jobId = re.findall(r'\nJobID=(.+)\n', out)[0].rstrip('\r')

    return jobId


def createFFmpegJob(job, inputFileName, outputFileName, outputArgs='', inputArgs='', **kwargs):
    pattern = r"(?P<head>.+?)(?P<padding>#+)(?P<tail>\.\w+$)"
    padding = re.search(pattern, inputFileName)

    # Convert ## padding to a ffmpeg padding
    if padding and padding.group('padding'):
        inputFileName = re.sub(
            pattern,
            r"\g<head>{}\g<tail>".format(
                '%0{}d'.format(len(padding.group('padding')))
            ),
            inputFileName
        )

    if isSequence(inputFileName):
        # Input is a sequence add start_number to inputArgs
        inputArgs = inputArgs + ' -start_number {}'.format(job.JobFramesList[0])

    jobInfo = {
        'Frames': 0,
        'Name': job.JobName + '_FFmpeg',
        'Plugin': 'FFmpeg',
        'OutputDirectory0': os.path.dirname(outputFileName).replace('\\', '/'),
        'OutputFilename0': os.path.basename(outputFileName),
        'OnJobComplete': 'delete',
        'Priority': kwargs.get('priority', 50),
    }

    # Inherit some slaves info from job
    for k in ['Pool', 'SecondaryPool', 'Whitelist', 'Blacklist', ]:
        v = job.GetJobInfoKeyValue(k)
        if v:
            jobInfo[k] = v

    pluginInfo = {
        'InputFile0': inputFileName.replace('\\', '/'),
        'InputArgs0': inputArgs,
        'ReplacePadding0': False,
        'OutputFile': outputFileName.replace('\\', '/'),
        'OutputArgs': outputArgs,
    }

    jobInfoFile = os.path.join(
        ClientUtils.GetDeadlineTempPath(), "ffmpeg_event_{0}.job".format(job.JobName)
    )
    pluginInfoFile = os.path.join(
        ClientUtils.GetDeadlineTempPath(), "ffmpeg_event_plugin_{0}.job".format(job.JobName)
    )

    # Write info files
    for p, i in ((jobInfoFile, jobInfo), (pluginInfoFile, pluginInfo)):
        with open(p, 'w') as f:
            for k, v in i.items():
                f.write('{}={}\n'.format(k, v))

    deadlineBin = ClientUtils.GetBinDirectory()
    if os.name == 'nt':
        deadlineCommand = os.path.join(deadlineBin, "deadlinecommand.exe")
    else:
        deadlineCommand = os.path.join(deadlineBin, "deadlinecommand")

    jobId = commandLineSubmit(deadlineCommand, pluginInfoFile, jobInfoFile)
    os.remove(jobInfoFile)
    os.remove(pluginInfoFile)
    return jobId


class JobMock:
    def __init__(self):
        pass

    @staticmethod
    def GetJobInfoKeyValue(key):
        return key + '.IV'

    @staticmethod
    def GetJobPluginInfoKeyValue(key):
        return key + '.PV'


def formatToken(job, token, string):
    """
    >>> s = '<info.key1>_<Plugin.key2>/<plugin.key2>%04d.exr'
    >>> formatToken(JobMock, getTokens(s), s)
    'key1.IV_key2.PV/key2.PV%04d.exr'
    >>> s = '<Info.key1.basename>.%04d<info.key1.extension>'
    >>> formatToken(JobMock, getTokens(s), s)
    'key1.%04d.IV'

    """
    if isinstance(token, list):
        for t in token:
            string = formatToken(job, t, string)
        return string
    split = token[1].split('.')

    # Unpack token
    if len(split) == 2:
        source, attributeName = split
        source = source.lower()
        op = None
    elif len(split) == 3:
        source, attributeName, op = split
        source = source.lower()
        op = op.lower()
    else:
        raise Exception('Invalid token %s' % token)

    # Check values
    assert source in SOURCE_MAP, \
        'Invalid source "%s" in token, should be one of %s' % (source, SOURCE_MAP.keys())
    assert op is None or op in OPS_MAP, \
        'Invalid operator "%s" in token, should be one of %s' % (op, OPS_MAP.keys())

    value = SOURCE_MAP[source](job, attributeName)
    if not value:
        raise Exception('Token returned empty value %s' % token)

    # Apply operation
    if op:
        value = OPS_MAP[op](value)

    return string.replace('%s%s%s' % token, value)


def isSequence(sequence):
    """
    >>> isSequence('/sequence.####.jpg')
    True
    >>> isSequence('/sequence.0001.jpg')
    False
    >>> isSequence('/dir.0001.sub/move.01.mov')
    False
    >>> isSequence('/sequence.%04d.jpg')
    True
    >>> isSequence('/sequence.%d.jpg')
    True
    """
    for pattern in (
            r"(?P<head>.+?)(?P<padding>#+)(?P<tail>\.\w+$)",
            r"(?P<head>.+)(?P<padding>%0?\d?d)(?P<tail>\.\w+$)"):

        search = re.search(pattern, sequence)

        if search:
            return True
    return False


def sequenceToWildcard(sequence):
    """
    >>> sequenceToWildcard('/sequence.####.jpg')
    '/sequence.*.jpg'
    >>> sequenceToWildcard('/sequence.0001.jpg')
    '/sequence.0001.jpg'
    >>> sequenceToWildcard('/sequence.%04d.jpg')
    '/sequence.*.jpg'
    >>> sequenceToWildcard('/sequence.%d.jpg')
    '/sequence.*.jpg'
    """
    for pattern in (
            r"(?P<head>.+?)(?P<padding>#+)(?P<tail>\.\w+$)",
            r"(?P<head>.+)(?P<padding>%0?\d?d)(?P<tail>\.\w+$)"):

        search = re.search(pattern, sequence)

        if search:
            return re.sub(pattern, r"\g<head>*\g<tail>", sequence)
    return sequence


def getTokens(string, delimiter=('<', '>')):
    assert len(delimiter) in (1, 2)
    delimiter = delimiter if len(delimiter) == 2 else delimiter * 2
    tokens = set(re.findall(r'{}(.+?){}'.format(*map(re.escape, delimiter)), string))
    return [(delimiter[0], t, delimiter[1]) for t in tokens]


if __name__ == "__main__":
    import doctest
    doctest.testmod()
