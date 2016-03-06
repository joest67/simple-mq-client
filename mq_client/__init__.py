# -*- coding: utf-8 -*-
from kombu import Connection, Queue
from kombu.exceptions import MessageStateError
from kombu.mixins import ConsumerMixin


class BaseConsumer(ConsumerMixin):

    def __init__(self, broker_url, queue_name, prefetch_count=0,
                 auto_ack=False, logger=None):
        self.connection = Connection(broker_url)
        self.task_queue = Queue(queue_name)
        self.prefetch_count = prefetch_count
        self.auto_ack = auto_ack

        if not logger:
            import logging
            logger = logging.getLogger(__name__)
        self.logger = logger

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(queues=[self.task_queue], callbacks=[self.on_task])
        if self.prefetch_count > 0:
            consumer.qos(prefetch_count=self.prefetch_count)
        return [consumer]

    def on_task(self, body, message):
        try:
            self.handle_task(body, message)
        except Exception as e:
            self.handle_error(body, message, e)
        else:
            if self.auto_ack:
                self.try_ack(body, message)

    def handle_error(self, body, message, exc):
        self.logger.exception('default error handler: %s', exc.message)

    def handle_task(self, body, message):
        raise NotImplementedError

    def try_ack(self, body, message):
        try:
            message.ack()
        except MessageStateError:
            self.logger.error('message %s is already ack.', message)
            return False
        else:
            return True
