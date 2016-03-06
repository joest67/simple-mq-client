# -*- coding: utf-8 -*-
import argparse
from multiprocessing import Process

from mq_client import BaseConsumer


class DemoConsumer(BaseConsumer):

    def handle_task(self, body, message):
        print 'get message, %s, %s' % (body, message)


def main(args):
    processes = []
    kwargs = {
        'broker_url': args.broker_url,
        'queue_name': args.queue_name,
    }
    for i in xrange(args.process_num):
        new_process = Process(target=run_consumer,
                              kwargs=kwargs)
        processes.append(new_process)
        new_process.start()

    for process in processes:
        process.join()


def run_consumer(broker_url, queue_name):
    consumer = DemoConsumer(broker_url, queue_name, auto_ack=True)
    consumer.run()


if __name__ == '__main__':
    from examples import consts
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker_url', default=consts.broker_url)
    parser.add_argument('--queue_name', default=consts.queue_name)
    parser.add_argument('--process_num', type=int, default=1)
    args = parser.parse_args()
    main(args)
