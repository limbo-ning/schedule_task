#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import threading,ctypes
import os,time, sys
import ScheduleListener, ScheduleUtil, ScheduleController

CONFIG_SUFFIX = '.conf'
MAIN_CONFIG = 'main.conf'

class listenerThread(threading.Thread):
	def __init__(self, configName, lock):
		super(listenerThread, self).__init__()
		self.listener = ScheduleListener.ScheduleListener(configName, lock)

	def terminate(self):
		# self.listener.close()
		res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self.ident),ctypes.py_object(SystemExit))

		if res == 0:
			raise ValueError('listenerThread %s not exists' % self.name)
		elif res > 1:
			raise SystemError('listenerThread thread %s failed' % self.name)

	def run(self):
		self.listener.start()

def spawnListenerThread(configName, lock):
	thread = listenerThread(configName, lock)
	thread.setName('ListenerThread-%s' % configName)
	thread.setDaemon(True)
	thread.start()
	return thread

class controllerThread(threading.Thread):
	def __init__(self, mainInstance):
		super(controllerThread, self).__init__()
		self.controller = ScheduleController.ScheduleController(mainInstance)

	def run(self):
		self.controller.start()

def spawnControllerThread(mainInstance):
	thread = controllerThread(mainInstance)
	thread.setName('ControllerThread')
	thread.setDaemon(True)
	thread.start()
	return thread


class ScheduleMain():
	def __init__(self, config):
		mainConfig = config.get('ScheduleMain')
		logFolder = mainConfig.get('log_folder')
		confFolder = mainConfig.get('conf_folder')

		self.__maxConcurrentExecutor = int(mainConfig.get('max_concurrent_executor'))

		self.__LISTENER_THREAD = []

		ScheduleUtil.exportEnvConfig(config)
		self.__loadTaskConfigFromFile()
		self.__CONTROLLER_THREAD = self.spawnControllerThread()


	def __loadTaskConfigFromFile(self):
		confFolder = ScheduleUtil.getConfFolder()

		taskConfig = {}

		configFiles = os.listdir(confFolder)
		for configFile in configFiles:
			if not configFile.endswith(CONFIG_SUFFIX):
				continue

			config = ScheduleUtil.loadConfigFile(confFolder + '/' +configFile)
			taskConfig[configFile] = config

		ScheduleUtil.validateTaskConfig(taskConfig)
		ScheduleUtil.exportTaskConfig(taskConfig)

		self.restartListeners()

	def spawnControllerThread(self):
		self.__CONTROLLER_THREAD = spawnControllerThread(self)


	def setTaskConfig(self, taskConfig):
		ScheduleUtil.validateTaskConfig(taskConfig)
		ScheduleUtil.exportTaskConfig(taskConfig)

	def restartListeners(self):
		while len(self.__LISTENER_THREAD) != 0:
			thread = self.__LISTENER_THREAD.pop()
			thread.terminate()
			thread.join(5)

		self.__LISTENER_THREAD = []

		LOCK = threading.BoundedSemaphore(value=self.__maxConcurrentExecutor)

		taskConfigs = ScheduleUtil.getAllTaskConfig()

		for taskName in taskConfigs.keys():
			self.__LISTENER_THREAD.append(spawnListenerThread(taskName, LOCK))


	def run(self):
		while True:
			for thread in self.__LISTENER_THREAD:
				if not thread.is_alive():
					print 'ERROR: listener stopped. exit all'
					sys.exit(1)
			time.sleep(1)
		


if __name__ == '__main__':

	main = ScheduleMain(ScheduleUtil.loadConfigFile(MAIN_CONFIG))
	main.run()

	


