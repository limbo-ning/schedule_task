#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import pika
import json,sys
from BaseMQConfig import *
from BaseMQUtil import *
import traceback


def action(self, channel, method_frame, header_frame, body):
	raise NotImplementedError

class BaseMQHandler:
	def __init__(
					self, 
					host, 
					port, 
					vhost, 
					user, 
					password,
					logger=None
				):
		self._closing = False
		self._connection = None
		self._consume_channel = None
		self._publish_channel = None
		self._host = host
		self._port = port
		self._vhost = vhost
		self._user = user
		self._password = password


		self._auto_ack = False
		self._action = None
		self._auto_delete = False
		self._exchange = None

		if logger != None:
			self.LOG = logger
		else:
			self.LOG = getLogger(__name__)

	'''
	Connect
	'''
	# Connect to EStock Rabbit MQ, get connection
	def connect(self, exchange):
		self._exchange = exchange

		credentials = pika.PlainCredentials(self._user, self._password)
		param = pika.ConnectionParameters(host=self._host, port=self._port, virtual_host=self._vhost, credentials=credentials)
		self._connection = pika.BlockingConnection(parameters=param)
		self.LOG.debug('Connected to MQ')
	''' 
	Consumer Module
	action(self, channel, method_frame, header_frame, body)
	'''
	# Loop to start consuming message
	def start_consuming(self):
		self.LOG.info('Start Consuming')
		# self._consume_channel.basic_consume(self.on_message, queue=self._queue)
		# self._consume_channel.start_consuming()
		while not self._closing:
			try:
				self._consume_channel.basic_consume(self.on_message, queue=self._queue)
				self._consume_channel.start_consuming()
			except SystemExit:
				self.close()
			except Exception as e:
				self.LOG.error(traceback.format_exc())
				if not self._closing:
					self.reconnect_consumer()
		
	def reconnect_consumer(self):
		raise NotImplementedError

	# Message handler when consuming message
	def on_message(self, channel, method_frame, header_frame, body):
		try:
			self.LOG.debug('%s %s %s' % (method_frame, header_frame, body))
			self._action(channel, method_frame, header_frame, body)
			self._consume_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
		except Exception as e:
			self.LOG.error(traceback.format_exc())
			if self._auto_ack:
				self._consume_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
			else:
				self._consume_channel.basic_reject(delivery_tag=method_frame.delivery_tag)


	'''
	Publisher Module
	'''
	# Set up publisher channel
	def set_publisher(self):
		self._publish_channel = self._connection.channel()
		self._publish_channel.add_on_return_callback(self.on_publish_fail)
		self.LOG.debug('Publisher Channel Set')

	# self.LOG down failure on rejected message
	def on_publish_fail(self, method, properties, body):
		self.LOG.error('Failed to publish MQ message. method:%s properties:%s body:%s' % (str(method), str(properties), str(body)))

	'''
	Graceful shut down
	'''
	def close(self):
		self.LOG.debug('Closing MQ')
		self._closing = True
		self._connection.close()
		self.LOG.debug('Closing MQ DONE')

class TopicMQHandler(BaseMQHandler):
	# Set up consumer
	def set_consumer(self, queue, routingKeys, action=action, autoDelete=False, autoAck=False):
		self._auto_ack = autoAck
		self._queue = queue
		self._routing_keys = routingKeys
		self._action = action

		self._consume_channel = self._connection.channel()
		#declare exchange
		self._consume_channel.exchange_declare(exchange=self._exchange, exchange_type=EXCHANGE_TYPE_TOPIC)
		#delcare queue
		self._consume_channel.queue_declare(queue=self._queue, auto_delete=autoDelete)
		#bind queue和routingKeys
		for routing_key in self._routing_keys:
			self._consume_channel.queue_bind(queue=self._queue, exchange=self._exchange, routing_key=routing_key)

		self.LOG.debug('Consume Channel Set. queue=%s routing_key=%s' % (queue, routingKeys))

	# Publish Message to MQ, raise exception
	def publish_mq(self, routingKeys, msg, properties):
		if not self._publish_channel:
			self.set_publisher()
		if self._closing:
			self.LOG.error('MQ closing. unable to send MQ')
			raise Exception('MQ closing')

		self.LOG.debug('Sending MQ routingKeys=%s msg=%s properties=%s' % (routingKeys, msg, properties))
		body = {
			'msg' : msg,
			'properties' : properties
		}
		for routingKey in routingKeys:
			try:
				self._publish_channel.basic_publish(self._exchange, routingKey, toJsonStr(body))
			except Exception, e:
				self.LOG.error(traceback.format_exc())
				self.connect(self._exchange)
				self.set_publisher()
				self._publish_channel.basic_publish(self._exchange, routingKey, toJsonStr(body))

	def reconnect_consumer(self):
		self.LOG.error('Restarting connection and topic_consume_channel')
		self._consume_channel = None
		self.connect(self._exchange)
		self.set_consumer(self._queue, self._routing_keys, self._action, self._auto_delete, self._auto_ack)
		self.LOG.debug('Restarted connection and topic_consume_channel')




class DirectMQHandler(BaseMQHandler):
	# Set up direct queue consumeer
	def set_direct_consumer(self, queue, action=action, autoDelete=False,autoAck=False):
		self._auto_ack = autoAck
		self._queue = queue
		self._routing_keys = [queue]
		self._action = action

		self._consume_channel = self._connection.channel()
		self._consume_channel.queue_declare(queue=self._queue, auto_delete=autoDelete)

		self.LOG.debug('Consume Direct Channel Set. queue=%s' % queue)

	# Pushish Direct message to queue
	def publish_direct_mq(self, queue, msg, properties, exchange=''):
		if not self._publish_channel:
			self.set_publisher()
		if self._closing:
			self.LOG.error('MQ closing. unable to send MQ')
			raise Exception('MQ closing')
		body = {
			'msg' : msg,
			'properties' : properties
		}
		self.LOG.debug('Sending direct MQ exchange=%s queue=%s msg=%s properties=%s' % (exchange, queue, msg, properties))

		try:
			self._publish_channel.basic_publish(exchange=exchange, routing_key=queue, body=toJsonStr(body))
		except Exception, e:
			self.LOG.error(traceback.format_exc())
			self.connect(exchange)
			self.set_publisher()
			self._publish_channel.basic_publish(exchange=exchange, routing_key=queue, body=toJsonStr(body))

	def reconnect_consumer(self):
		self.LOG.error('Restarting connection and direct_consume_channel')
		self._consume_channel = None
		self.connect(self._exchange)
		self.set_direct_consumer(self._queue, action=self._action, autoDelete=self._auto_delete, autoAck=self._auto_ack)
		self.LOG.debug('Restarted connection and direct_consume_channel')


class BroadcastMQHandler(BaseMQHandler):
	# Set up fanout broadcast consumer
	def set_broadcast_consumer(self, queue='', action=action,autoAck=False, autoDelete=True):
		self._auto_ack = autoAck
		self._action = action
		self._auto_delete = autoDelete

		self._consume_channel = self._connection.channel()
		#declare exchange
		self._consume_channel.exchange_declare(exchange=self._exchange, exchange_type=EXCHANGE_TYPE_FANOUT)
		#declare queue
		if queue and len(queue) > 0:
			self._queue = queue
			self._consume_channel.queue_declare(queue=self._queue, auto_delete=autoDelete)
		else:
			self._queue = self._consume_channel.queue_declare(auto_delete=autoDelete).method.queue
		#bind queue
		self._consume_channel.queue_bind(queue=self._queue, exchange=self._exchange)

		self.LOG.debug('Consume broadcast Channel Set. queue=%s' % self._queue)

	def publish_broadcast_mq(self, properties, msg='{}'):
		if not self._publish_channel:
			self.set_publisher()
		if self._closing:
			self.LOG.error('MQ closing. unable to send MQ')
			raise Exception('MQ closing')
		body = {
			'msg' : msg,
			'properties' : properties
		}
		self.LOG.debug('Sending broadcast MQ exchange=%s msg=%s properties=%s' % (self._exchange, msg, properties))

		try:
			self._publish_channel.basic_publish(exchange=self._exchange, body=toJsonStr(body), routing_key='')
		except Exception, e:
			self.LOG.error(traceback.format_exc())
			self.connect(self._exchange)
			self.set_publisher()
			self._publish_channel.basic_publish(exchange=self._exchange, body=toJsonStr(body), routing_key='')

	def reconnect_consumer(self):
		self.LOG.error('Restarting connection and broadcast_consume_channel')
		self._consume_channel = None
		self.connect(self._exchange)
		self.set_broadcast_consumer(queue=self._queue, action=self._action, autoAck=self._auto_ack, autoDelete=self._auto_delete)
		self.LOG.debug('Restarted connection and broadcast_consume_channel')


