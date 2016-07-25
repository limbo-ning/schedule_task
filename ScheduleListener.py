#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import json,sys,traceback,os,subprocess
import ScheduleMQ, ScheduleUtil

class ScheduleListener():
	def __init__(self, taskName, lock):
		self._taskName = taskName
		self._lock = lock

		config = ScheduleUtil.getTaskConfig(taskName)
		rabbitMQConfig = ScheduleUtil.getRabbitMQConfig()
		logFolder = ScheduleUtil.getLogFolder()

		self._queue = config.get('queue')

		self._scheduleType = int(config.get('schedule_type'))
		self._scheduleId = int(config.get('schedule_id'))

		self._listenerConfig = ScheduleUtil.getConfig('ScheduleListener')

		self.LOG = ScheduleUtil.getLogger(self._taskName, logFolder=logFolder+'/'+self._taskName, logFile='listener')

		self.mq = ScheduleMQ.ScheduleMQHandler(
				logger=self.LOG, 
				host=rabbitMQConfig.get('host'), 
				port=int(rabbitMQConfig.get('port')),
				user=rabbitMQConfig.get('user'),
				password=rabbitMQConfig.get('password'),
				vhost=rabbitMQConfig.get('vhost'),
				exchange=rabbitMQConfig.get('exchange')
			)

	def start(self):
		self.mq.listen_schedule_mq(self.buildProcessFunction(), queue = self._queue)


	def buildProcessFunction(self):
		def processFunction(msg, properties):
			self.LOG.debug('received msg:%s properties:%s', ScheduleUtil.toJsonStr(msg), ScheduleUtil.toJsonStr(properties))
			if properties.get('type') != self._scheduleType:
				self.LOG.debug('scheduleType not match. skip')
				return
			if properties.get('subType') != self._scheduleId and properties.get('subType') != ScheduleMQ.PROPERTIES_SUB_TYPE_SCHEDULE_ALL:
				self.LOG.debug('scheduleId not match. skip')
				return

			if self._lock.acquire():
				self.LOG.debug('lock acquired. start executor')

				success = True
				try:
					logFolder = ScheduleUtil.getLogFolder()

					f = open(logFolder+'/'+self._taskName+'/out.log', 'a+')

					exeProcess = subprocess.Popen(['python', self._listenerConfig.get('target'), self._taskName], stdout=f.fileno(), stderr=f.fileno())
					exeProcess.wait()

					self.LOG.debug('executer finished. returncode %d' % exeProcess.returncode)

					if exeProcess.returncode != 0:
						success = False

				finally:
					self._lock.release()

				if not success:
					raise Exception('task not success.')

		return processFunction




