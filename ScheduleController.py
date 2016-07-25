#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import ScheduleMQ, ScheduleUtil

class ScheduleController():
	def __init__(self, mainInstance):
		rabbitMQConfig = ScheduleUtil.getRabbitMQConfig()
		logFolder = ScheduleUtil.getLogFolder()
		controllerConfig = ScheduleUtil.getConfig('ScheduleController')

		self.MAIN_INSTANCE = mainInstance
		self.QUEUE = controllerConfig.get('queue')
		self.LOG = ScheduleUtil.getLogger(__name__, logFolder, 'controller')
		self.MQ = ScheduleMQ.ScheduleMQHandler(
				logger=self.LOG, 
				host=rabbitMQConfig.get('host'), 
				port=int(rabbitMQConfig.get('port')),
				user=rabbitMQConfig.get('user'),
				password=rabbitMQConfig.get('password'),
				vhost=rabbitMQConfig.get('vhost'),
				exchange=rabbitMQConfig.get('exchange')
			)


	def buildProcessFunction(self):
		def processFunction(msg, properties):
			self.LOG.debug('received msg:%s properties:%s', ScheduleUtil.toJsonStr(msg), ScheduleUtil.toJsonStr(properties))
			if properties.get('type') != ScheduleMQ.PROPERTIES_TYPE_SCHEDULE_CONTROL:
				self.LOG.debug('not control msg. skip')
				return

			if msg.has_key("taskConfig"):
				self.MAIN_INSTANCE.setTaskConfig(msg.get("taskConfig"))

			self.MAIN_INSTANCE.restartListeners()

			self.LOG.debug('restarted listeners')

		return processFunction

	def start(self):
		self.MQ.listen_schedule_mq(self.buildProcessFunction(), queue=self.QUEUE)
