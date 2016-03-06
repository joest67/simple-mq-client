# -*- coding: utf-8 -*-
import argparse
import time

from kombu import Connection


def main(args):
    payload = {'hello': 'world', 'now': int(time.time())}

    with Connection(args.broker_url) as conn:
        simple_queue = conn.SimpleQueue(args.queue_name)
        print('Sent: %s' % payload)

        simple_queue.put(payload)
        simple_queue.close()


if __name__ == '__main__':
    from examples import consts
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker_url', default=consts.broker_url)
    parser.add_argument('--queue_name', default=consts.queue_name)
    args = parser.parse_args()
    main(args)
