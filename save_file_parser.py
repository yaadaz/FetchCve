"""
save_file_parser.py
~~~~~~~~~~~~~~~~~~~

Exports a parser that simply saves the entries into files.
"""
import json
import logging
import os
import parser
import queue
from datetime import datetime

FILENAME_FORMAT = '%Y-%m-%dT%H-%M-%S-%f.txt'

logger = logging.getLogger(__name__)


class SaveFileParser(parser.Parser):
    """Save entries into the disk.

        Arguments:
            output_directory: The directory to store the output file in.
            max_entries: Max entries to put in a single file.
            input_queue: The entries in this queue are used as input
            for the parser.
    """
    def __init__(self, output_directory: str, max_entries: int,
                 input_queue: queue.Queue, *args, **kwargs):
        self.output_directory = output_directory
        self.max_entries = max_entries
        super().__init__(input_queue, *args, **kwargs)

    def run(self):
        """Save the entries into files."""
        logger.debug("Starting parser thread")

        # Create output folder if not exist
        if not os.path.isdir(self.output_directory):
            logger.info(f"Creating folder: {self.output_directory}")
            os.makedirs(self.output_directory)

        # Save up to max_entries entries into a file
        total_entries = 0
        entry_count = 0
        entry = self.input_queue.get()
        output = None  # added this line so that pycharm will know this variable
        while True:
            if 0 == entry_count:
                # Create a new file
                filename = datetime.now().strftime(FILENAME_FORMAT)
                file_path = os.path.join(self.output_directory, filename)
                logger.info(f"Creating new output file {filename}")
                output = open(file_path, 'w')
            output.write(json.dumps(entry))
            output.write('\n')
            total_entries += 1
            entry_count += 1
            # logger.debug(f"Wrote entry into file. "
            #             f"Current entry count for this file: {entry_count}")
            if entry_count == self.max_entries:
                # Close file and restart count
                logger.debug(f"Max entries reached for file")
                output.close()
                entry_count = 0
            entry = self.input_queue.get()
            if entry is None:  # end of queue reached
                logger.info("Parser finished parsing all entries")
                logger.info(f"Total entries parsed: {total_entries}")
                output.close()
                return
