#!/usr/bin/python3
"""
Fetch CVE data from NVD (https://nvd.nist.gov/) and save it to the disk.
The output is saved in JSON format, several entries in a file.

Example usage:
    python fetch_cves.py --days-back 180 --output-directory ./output

Configuration can be changed in the config file.
"""
import argparse
import logging
import queue

import config
import cve_fetcher
import log_config
import save_file_parser

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Fetches CVE records from NVD and saves it to disk.")
    parser.add_argument('--days-back', '-d',
                        type=int,
                        required=True,
                        metavar='DAYS',
                        help="How many days back to fetch CVEs")
    parser.add_argument('--output-directory', '-o',
                        required=True,
                        metavar='OUTPUT',
                        help="Path of the output directory")
    args = parser.parse_args()

    log_config.config_root_logger(config.LOG_FILE_DIRECTORY)

    # Start parser thread
    output_queue = queue.Queue()
    logger.debug(f"Creating SaveFileParser with parameters: "
                 f"{args.output_directory}, {config.MAX_RECORDS_PER_FILE}")
    parser = save_file_parser.SaveFileParser(args.output_directory,
                                             config.MAX_RECORDS_PER_FILE,
                                             output_queue,
                                             name="ParserThread")
    logger.info(f"Starting parser")
    parser.daemon = True
    parser.start()

    # Start fetcher
    logger.debug(f"Starting CveFetcher with the last {args.days_back} days")
    fetcher = cve_fetcher.CveFetcher(output_queue)
    fetcher.fetch_days_back(args.days_back)
    logger.debug(f"Finished fetching, waiting for the parser to finish")
    parser.join()


if __name__ == '__main__':
    main()
