"""
cve_fetcher.py
~~~~~~~~~~~~~~

Exports CVE fetchers for fetching CVEs using REST API.
"""
import math
import logging
import requests
from datetime import datetime, timedelta
from queue import Queue

import fetcher

logger = logging.getLogger(__name__)


class CveFetcher(fetcher.Fetcher):
    """A CVE fetcher to fetch CVE entries using REST API."""

    # Overriding base class variables
    BASE_URL = 'https://services.nvd.nist.gov/rest/json/cves/2.0'
    MAX_WORKERS = 5
    COOLDOWN_TIME = 30

    # Keys used in the response json
    CVE_KEY = 'vulnerabilities'
    RESULTS_PER_PAGE_KEY = 'resultsPerPage'
    START_INDEX_KEY = 'startIndex'
    TOTAL_RESULTS_KEY = 'totalResults'

    # Name of parameters used for GET operations
    START_INDEX_PARAM = 'startIndex'
    PUBLISH_DATE_START_PARAM = 'pubStartDate'
    PUBLISH_DATE_END_PARAM = 'pubEndDate'

    # Server constants
    MAX_CONSECUTIVE_DAYS = 120

    # @typing_extensions.override   # pycharm doesn't support this yet
    def _parse_response(self, response: requests.Response) -> None:
        """See base class."""
        cves = response.json()[self.CVE_KEY]
        logging.debug(f"Found {len(cves)} CVEs in the response object")
        for cve in cves:
            self.output_queue.put(cve)

    # @typing_extensions.override   # pycharm doesn't support this yet
    def _create_page_params_queue(
            self, response: requests.Response
    ) -> Queue[fetcher.Params]:
        """See base class."""
        response_data = response.json()
        results_per_page = response_data[self.RESULTS_PER_PAGE_KEY]
        logger.info(f"resultsPerPage: {results_per_page}")
        start_index = response_data[self.START_INDEX_KEY]
        logger.info(f"startIndex: {start_index}")
        total_results = response_data[self.TOTAL_RESULTS_KEY]
        logger.info(f"totalResults: {total_results}")
        total_pages = math.ceil((total_results-start_index) / results_per_page)
        logger.info(f"Total pages: {total_pages}")

        # Put page offsets into the queue. Current page is ignored
        # because it was already fetched.
        params_queue = Queue()
        for page_index in range(1, total_pages):
            offset = start_index + (results_per_page * page_index)
            params = {self.START_INDEX_PARAM: offset}
            params_queue.put(params)

        return params_queue

    def fetch_days_back(self, days_back: int) -> None:
        """ Fetch CVEs from the last X days.

        The days are calculated backwards from the exact moment of the
        time this method was called (and not from the start of the day).

        Args:
            days_back: The amount of days to fetch.
        """
        # There is a maximum allowable range, so we make consecutive
        # fetch requests as needed.
        logger.info(f"Got request to fetch {days_back} days")
        days_left = days_back
        end_date = datetime.now().astimezone()
        while days_left:
            days_to_fetch = min(days_left, self.MAX_CONSECUTIVE_DAYS)
            logger.info(f"Fetching {days_to_fetch} days")
            delta = timedelta(days=days_to_fetch)
            start_date = end_date - delta
            start_str = start_date.isoformat(timespec='milliseconds')
            end_str = end_date.isoformat(timespec='milliseconds')
            logger.info(f"Start date: {start_str} | End date: {end_str}")
            request_params = {self.PUBLISH_DATE_START_PARAM: start_str,
                              self.PUBLISH_DATE_END_PARAM: end_str}
            end_date = start_date
            days_left -= days_to_fetch
            last_fetch = False if days_left > 0 else True
            self.fetch(query_params=request_params, last_fetch=last_fetch)
