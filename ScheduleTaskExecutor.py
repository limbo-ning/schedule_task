#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import json,sys,traceback,threading,os,subprocess,ctypes,signal,time,Queue,gc
import ScheduleUtil

PWD = sys.path[0]
if os.path.isfile(PWD):
	PWD = os.path.dirname(PWD)

class ScheduleTaskExecutor():
	def __init__(self, taskName):
		executorConfig = ScheduleUtil.getConfig('ScheduleTaskExecutor')
		taskConfig = ScheduleUtil.getTaskConfig(taskName)

		self._maxConcurrent = int(executorConfig.get('max_concurrent_task'))
		if taskConfig.has_key('max_concurrent_task'):
			taskMaxConcurrent = int(taskConfig.get('max_concurrent_task'))
			if taskMaxConcurrent < self._maxConcurrent:
				self._maxConcurrent = taskMaxConcurrent

		self._autoAck = int(taskConfig.get('auto_ack', 1))

		self._exeDict = {}
		self._exeNameQueue = Queue.Queue(maxsize=0)

		for exeName,exeConfig in taskConfig.iteritems():
			if isinstance(exeConfig, dict):
				self._exeDict[exeName] = exeConfig
				self._exeNameQueue.put(exeName)

		self._threadList = []
		self._error = threading.Event()
		self._error.clear()

		self.LOG = ScheduleUtil.getLogger(taskName, logFolder=ScheduleUtil.getLogFolder()+'/'+taskName, logFile='executor')
	
	def start(self):
		try:
			for i in range(0, min(self._exeNameQueue.qsize(), self._maxConcurrent)):
				thread = workerThread(self._exeNameQueue, self._exeDict, self.LOG, self._error)
				thread.setDaemon(True)
				thread.start()
				self._threadList.append(thread)

			while len(self._threadList) > 0:
				self.checkRet()
				time.sleep(1)

			self.LOG.debug('all task execution done')

			sys.exit(0)

		except Exception as e:
			self.LOG.error(traceback.format_exc())
			self.terminate()

	def checkRet(self):
		if self._error.isSet():
			self.LOG.error('error. auto_ack:%d' % self._autoAck)
			self._error.clear()
			if not self._autoAck:
				self.terminate()

		snapShotList = list(self._threadList)

		for thread in snapShotList:
			if not thread.is_alive():
				self.LOG.debug('thread %s return' % thread.name)
				self._threadList.remove(thread)
				

	def terminate(self):
		for thread in self._threadList:
			if thread.isAlive():
				try:
					thread.terminate()
				except Exception:
					pass

		sys.exit(1)


class workerThread(threading.Thread):
	def __init__(self, exeNameQueue, exeDict, LOG, error):
		super(workerThread, self).__init__()
		
		self._exeNameQueue= exeNameQueue
		self._exeDict = exeDict

		self.LOG = LOG
		self._process = None

		self._error = error

	def run(self):
		while not self._exeNameQueue.empty():
			exeName = self._exeNameQueue.get()
			exeConfig = self._exeDict.get(exeName)
			self.execute(exeName, exeConfig)
			gc.collect()

	def terminate(self):
		if self._process is not None:
			os.kill(self._process.pid, signal.SIGKILL)
		res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self.ident),ctypes.py_object(SystemExit))

	def execute(self, exeName, exeConfig):
		process_or_thread = exeConfig.get('process_or_thread')
		path = exeConfig.get('path', '')
		cmd = exeConfig.get('cmd')

		self.LOG.debug('start executing %s' % exeName)

		try:
			if len(path) > 0:
				os.chdir(path)

			if process_or_thread == 'process':
				self._process = subprocess.Popen(cmd.split(' '))
				self._process.wait()
				if self._process.returncode != 0 :
					self.LOG('process %d spawned for %s returned error' % (self._process.pid, self._exeName))
					self._error.set()

			elif process_or_thread == 'thread':
				sys.path.append(os.getcwd())
				moduleName = exeName[:len(exeName)-3]

				exec('import %s' % moduleName)
				exec('%s.%s' % (moduleName, cmd))

			os.chdir(PWD)

		except Exception as e:
			self.LOG.error(traceback.format_exc())
			self._error.set()
			os.chdir(PWD)

def execute(taskName):
	executor = ScheduleTaskExecutor(taskName)
	executor.start()

if __name__ == "__main__":
	if len(sys.argv) < 2:
		raise Exception('Wrong input. arg: taskName')

	if len(sys.argv) == 3 and '--debug' in sys.argv:
		sys.argv.remove('--debug')
		config = ScheduleUtil.loadConfigFile('main.conf')
		ScheduleUtil.exportEnvConfig(config)
		taskConfig = {
			sys.argv[1] : ScheduleUtil.loadConfigFile(ScheduleUtil.getConfFolder()+'/'+sys.argv[1])
		}
		ScheduleUtil.exportTaskConfig(taskConfig)

	execute(sys.argv[1])


