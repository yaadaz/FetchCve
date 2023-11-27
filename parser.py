"""
parser.py
~~~~~~~~~

Exports a generic abstract parser for parsing a queue of entries
received from a REST API server.
"""
import queue
import threading


class Parser(threading.Thread):
    """ Abstract parser for parsing entries from a REST API server.

        This parser should be used in its own thread and in concurrent
        with a fetcher object.

        Arguments:
            input_queue: The entries in this queue are used as input
            for the parser.
    """

    def __init__(self, input_queue: queue.Queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_queue = input_queue

    def run(self):
        raise NotImplementedError("Parser is an abstract class")
