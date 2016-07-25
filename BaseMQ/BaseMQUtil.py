#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import logging, os, urllib2, json, datetime
from logging.handlers import RotatingFileHandler
from BaseMQConfig import *
#
# Request EStock API
#
def estockReq(url, data=None, headers=None):
    body = None

    if not headers:
        req = urllib2.Request(url, data)
    else:
        req = urllib2.Request(url, data, headers)
    res = urllib2.urlopen(req)
    body = res.read()
    resultJson = json.loads(body)
    if resultJson.get('retcode') == 0:
        return resultJson.get('result') 
    else:
        raise Exception('Access Service Error: retcode:%d' % resultJson.get('retcode'))

class CJsonEncoder(json.JSONEncoder):  
    def default(self, obj):  
        if isinstance(obj, datetime.datetime):  
            return obj.strftime('%Y-%m-%d %H:%M:%S')  
        elif isinstance(obj, datetime.date):  
            return obj.strftime("%Y-%m-%d")  
        else:  
            return json.JSONEncoder.default(self, obj)  

def toJsonStr(job):
    return json.dumps(job, cls=CJsonEncoder, ensure_ascii=False)

LOG_FILE = 'BaseMQ'
LOG_FOLDER = 'log/'
LOG_MAX_SIZE = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

def getLogger(name, logFolder=LOG_FOLDER, logFile=LOG_FILE, logMaxSize=LOG_MAX_SIZE, logBackupCount=LOG_BACKUP_COUNT):
    #return logger(name, logFolder, logFile, logMaxSize, logBackupCount)
    if not logFolder.endswith('/'):
        logFolder += '/'
    if not os.path.exists(logFolder):
        os.makedirs(logFolder)

    logger = logging.getLogger(name)

    if logger.getEffectiveLevel() != logging.DEBUG:
    
        errorRotateHandler = RotatingFileHandler(filename = logFolder + logFile+'.ERROR',
                                                    maxBytes = logMaxSize,
                                                    backupCount = logBackupCount,
                                                    encoding = 'UTF-8')
        errorRotateHandler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(module)s %(process)d %(threadName)s %(levelname)s %(message)s'))
        errorRotateHandler.setLevel(logging.ERROR)
        logger.addHandler(errorRotateHandler)

        debugRotateHandler = RotatingFileHandler(filename = logFolder + logFile+'.DEBUG',
                                                    maxBytes = logMaxSize,
                                                    backupCount = logBackupCount,
                                                    encoding = 'UTF-8')
        debugRotateHandler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(module)s %(process)d %(threadName)s %(levelname)s %(message)s'))
        debugRotateHandler.setLevel(logging.DEBUG)
        logger.addHandler(debugRotateHandler)

        infoRotateHandler = RotatingFileHandler(filename = logFolder + logFile+'.INFO',
                                                    maxBytes = logMaxSize,
                                                    backupCount = logBackupCount,
                                                    encoding = 'UTF-8')
        infoRotateHandler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(module)s %(process)d %(threadName)s %(levelname)s %(message)s'))
        infoRotateHandler.setLevel(logging.INFO)
        logger.addHandler(infoRotateHandler)

        logger.setLevel(logging.DEBUG)

        logger.debug('created logger %s' % name)

    return logger


