#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import ConfigParser,os,json,sys
sys.path.append("./BaseMQ/")
import BaseMQUtil

def getLogger(name, logFolder, logFile):
	return BaseMQUtil.getLogger(name, logFolder=logFolder, logFile=logFile)

def validate(SCHEDULE_ID_VALIDATE, taskName, config):
	scheduleId = int(config.get('schedule_id'))

	if SCHEDULE_ID_VALIDATE.has_key(scheduleId):
		raise Exception('duplicate scheduleId with config [%s] and [%s]' % (taskName, SCHEDULE_ID_VALIDATE.get(scheduleId)))
	
	if not config.has_key("queue") or len(config.get('queue')) == 0:
		config['queue'] = 'schedule_%d_%s' % (scheduleId, taskName)

	SCHEDULE_ID_VALIDATE[scheduleId] = taskName

	executables = config.keys()

	for executable in executables:
		eachConfig = config.get(executable)

		if not isinstance(eachConfig, dict):
			continue

		mode = eachConfig.get('process_or_thread')
		cmd = eachConfig.get('cmd')

		if mode == 'thread':
			if not executable.endswith('.py'):
				raise Exception('thread mode can only apply to python script config [%s] executable [%s]' % (taskName, executable))
		elif mode == 'process':
			pass
		else:
			raise Exception('process_or_thread invalid config [%s] executable [%s]' % (taskName, executable))

		executablePath = ''
		if eachConfig.has_key('path'):
			path = eachConfig.get('path')
			if len(path) != 0:
				if path.endswith('/'):
					executablePath += path
				else:
					executablePath += path + '/'
		executablePath += executable

		if not os.path.exists(executablePath):
			raise Exception('executable not found. path [%s] config [%s]' % (executablePath, taskName))

def validateTaskConfig(taskConfigs):
	taskValidateMap = {}
	for taskName, taskConfig in taskConfigs.iteritems():
		validate(taskValidateMap, taskName, taskConfig)

def loadConfigFile(configFilePath):
	if not os.path.exists(configFilePath):
		raise Exception('%s not found' % configFilePath)
		
	config = ConfigParser.ConfigParser()
	config.read(configFilePath)

	result = dict(config.defaults())
	for section in config.sections():
		result[section] = dict(config.items(section))

	return result


class CJsonEncoder(json.JSONEncoder):  
    def default(self, obj):  
        if isinstance(obj, datetime.datetime):  
            return obj.strftime('%Y-%m-%d %H:%M:%S')  
        elif isinstance(obj, date):  
            return obj.strftime("%Y-%m-%d")  
        else:  
            return json.JSONEncoder.default(self, obj)  

def toJsonStr(job):
    return json.dumps(job, cls=CJsonEncoder, ensure_ascii=False)

ENV_CONFIG_NAME = 'schedule_task_config'


# EXPORT env method can only be called from main thread
def exportEnvConfig(config):
	os.environ[ENV_CONFIG_NAME] = toJsonStr(config)

def exportTaskConfig(taskConfig):
	config = importEnvConfig()
	config['TaskConfig'] = taskConfig
	exportEnvConfig(config)

# thread need to be respawned if env need to be reloaded
def importEnvConfig():
	return json.loads(os.getenv(ENV_CONFIG_NAME))

def getConfig(configName):
	return importEnvConfig().get(configName)
	
def getLogFolder():
	return getConfig('ScheduleMain').get('log_folder')

def getConfFolder():
	return getConfig('ScheduleMain').get('conf_folder')

def getRabbitMQConfig():
	return getConfig('RabbitMQ')

def getAllTaskConfig():
	return getConfig('TaskConfig')

def getTaskConfig(taskName):
	return getAllTaskConfig().get(taskName)



