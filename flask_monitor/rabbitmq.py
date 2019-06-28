# -*- coding: utf-8 -*-

from flask_monitor import ObserverMetrics
import logging

import pika


class ObserverRabbit(ObserverMetrics):

    args_mq = ['host','port','virtual_host','credentials','channel_max','frame_max',
                'heartbeat', 'ssl', 'ssl_options', 'connection_attempts', 'retry_delay',
                'socket_timeout', 'locale', 'backpressure_detection', 'blocked_connection_timeout',
                'client_properties']
    
    def __init__(self, 
                       exchange='flask',
                       routing_key='',
                       *args,
                       **kw):
        # 从 kw 获取 RabbitMQ 的参数
        kw_mq = { key : kw[key] for key in kw if key in self.args_mq}
        # 从 kw 获取非 RabbitMQ 的参数
        kw = { key : kw[key] for key in kw if key not in self.args_mq}
        # 非RabbitMQ参数传给 ObserverMetrics
        ObserverMetrics.__init__(self, *args, **kw)
        try:
            # RabbitMQ参数传给pika
            connection = pika.BlockingConnection(pika.ConnectionParameters(**kw_mq))
            self.channel = connection.channel()
            self.exchange = exchange  # default is 'flask'
            self.routing_key = routing_key
            try:
                self.channel.exchange_declare(exchange=exchange,
                                               type='fanout')
                logging.getLogger(self._logger).debug("Create channel RabbitMq '%s'" % exchange)
            except:
                logging.getLogger(self._logger).debug("Not create channel RabbitMq '%s'" % exchange)
        except Exception as e:
            logging.getLogger(self._logger).critical("Cannot connect to RabbitMq '%s'" % str(e))
        

    def action(self, event):
        """发布消息"""
        try:
            self.channel.basic_publish(exchange=self.exchange,
                                  routing_key=self.routing_key,
                                  body=event.json)
        except Exception as e:
            logging.getLogger(self._logger).critical("Error Unknow on RabbitMq '%s'" % str(e))
            

