"""
fetcher.py
~~~~~~~~~~

Exports a generic abstract fetcher for fetching objects using REST API.
"""
import logging
import queue
import requests
import threading
import time

from abc import ABC, abstractmethod
from queue import Queue
from typing import Dict

# HTTP status codes
STATUS_CODE_FORBIDDEN = 403
STATUS_CODE_SERVICE_UNAVAILABLE = 503
STATUS_CODE_SUCCESS = 200

# Shorter type hint for params
Params = Dict[str, str | int]

logger = logging.getLogger(__name__)


class QueryFailed(Exception):
    """Failed to query the API Server."""

    def __init__(self, response: requests.Response, message="Request failed"):
        super().__init__(message)
        self.response = response


class UnsupportedStatusCode(QueryFailed):
    """Received response with unsupported status code."""
    pass


class UnavailableService(QueryFailed):
    """Received Service Unavailable status code for the request."""
    pass


class Fetcher(ABC):
    """A generic abstract fetcher to fetch entries by using REST API.

    The fetcher sends requests to the API server and extracts the
    results from the response objects into the output_queue. The
    fetcher takes into consideration the rate limit and pagination of
    the API server.

    Arguments:
        output_queue: Stores the results of the requests. This queue
            can be used by a parser thread to process the received
            data. A None entry is used to indicate the end of the queue.

    Usage:
        Concrete classes must initialize class variables. For a rate
        limit of X requests in Y seconds it is advised to set
        COOLDOWN_TIME to Y and MAX_WORKERS to a value divisible by X.

        Concrete classes must implement the methods '_parse_response'
        and 'create_page_params_queue'.

    Assumptions:
        - The server sends the pagination info in the response of the
          first request.
        - The cooldown timer of the server is a rolling timer (it
          doesn't restart after each request, but it checks how many
          requests it got in the last X seconds).
    """

    BASE_URL: str = None  # Base URL to use for the API requests
    MAX_WORKERS: int = None  # Max thread workers to use concurrently
    COOLDOWN_TIME: int = None  # Cooldown time between consecutive requests

    RETRY_TIMER = 15  # Retry sleep time after failing a request
    MAX_RETRIES = 10  # Max retries per request

    def __init__(self, output_queue: Queue):
        self.output_queue = output_queue
        if not all([self.BASE_URL, self.COOLDOWN_TIME, self.MAX_WORKERS]):
            logger.critical("Fetcher class variables are not initialized")
            raise NotImplementedError(f"Class variables must be initialized")
        logger.debug(f"BASE_URL: {self.BASE_URL}")
        logger.debug(f"MAX_WORKERS: {self.MAX_WORKERS}")
        logger.debug(f"COOLDOWN_TIME: {self.COOLDOWN_TIME}")

    @abstractmethod
    def _parse_response(self, response: requests.Response) -> None:
        """Parse a response object.

        Extract the requested entries from a response object and add
        them to output_queue.

        Args:
            response: A response object containing the requested
                entries from the server.
        """
        pass

    @abstractmethod
    def _create_page_params_queue(
            self, response: requests.Response
    ) -> Queue[Params]:
        """Create page parameters for the next requests.

        A response object from the server is assumed to include the
        info needed for pagination. This info is extracted here and
        used to create the parameters for the following page requests.
        Each set of parameters is pushed into a queue to be consumed
        later by worker threads.

        Args:
            response: A response object to use to extract the
            info needed to create the page parameters.

        Returns:
            A queue of dicts, each dict contains the parameters for a
            single page request. The first pages up until this response
            are not included. Return an empty queue when no further
            pages are needed.
        """
        pass

    def make_request(self, params: Params) -> requests.Response:
        """Make a single request to the server and return the response.

        Raises:
            UnsupportedStatusCode: When the response has unsupported
                status code.
            UnavailableService: When several attempts to make the request
                resulted in Service Unavailable status code.
        """
        full_url = requests.Request('GET',
                                    self.BASE_URL,
                                    params=params).prepare().url
        logger.debug(f"Requesting URL: {full_url}")
        attempts = 1
        while True:
            response = requests.get(full_url)
            logger.debug(f"Response status code: {response.status_code}")
            if response.status_code == STATUS_CODE_FORBIDDEN:
                # Reached rate limit. Need to cooldown.
                logger.debug(f"Waiting {self.COOLDOWN_TIME} seconds")
                time.sleep(self.COOLDOWN_TIME)
                continue
            if response.status_code == STATUS_CODE_SERVICE_UNAVAILABLE:
                # Server is temporarily unavailable
                logger.debug(f"Server is temporarily unavailable")
                if attempts == self.MAX_RETRIES:
                    logger.error("Max retries reached for request")
                    raise UnavailableService(response, f"Service unavailable")
                attempts += 1
                # TODO it's better to wait increasingly amount of time every iteration
                time.sleep(self.RETRY_TIMER)
                logger.debug(f"Retrying request. Attempt no. {attempts}")
                continue
            if response.status_code != STATUS_CODE_SUCCESS:
                # The results are unreliable, so it's better to skip
                # this request with a log message than to try and fix
                # it here
                logger.error(f"Unsupported status code: {response.status_code}")
                raise UnsupportedStatusCode(
                    response,
                    f"Unsupported status code: {response.status_code}"
                )
            return response

    def fetch(self, query_params: Params, last_fetch: bool = True) -> None:
        """Fetch the requested entries from the server.

        Fetch the requested entries from the server based on the given
        parameters. The fetch process can be finalized afterward so no
        consecutive fetches can be made.

        Args:
            query_params: Parameters to use for the GET method.
            last_fetch: Whether to signal the end of the fetch process
                to consumers of the output queue.
        """
        self._fetch(query_params)
        if last_fetch:
            self._finalize_fetch()

    def _finalize_fetch(self) -> None:
        """Signal the end of the fetching process."""
        # Put None at the end of the queue to signal the consumer that
        # there are no further entries
        logger.info("No more entries to fetch")
        logger.debug("Putting None at the end of output queue")
        self.output_queue.put(None)

    def _fetch(self, query_params: Params) -> None:
        """Start the actual fetching.

        Use consecutive GET methods to get all the requested entries
        from the server, and put the results in the output_queue.
        This method creates workers threads to help fetching the data
        faster.
        """
        # Start the first request and get the pagination info
        # along with the requested data
        logger.info(f"Starting fetcher with parameters: {query_params}")
        logger.info("Getting first page from the server")
        try:
            response = self.make_request(query_params)
        except QueryFailed as e:
            logger.error(f"First request failed. Parameters: {query_params}")
            logger.debug(f"Response: {e.response.text}")
            return
        self._parse_response(response)

        # Run worker threads to help fetching the rest of the data
        logger.debug("Getting queue of page parameters")
        params_queue = self._create_page_params_queue(response)
        logger.info(f"Need additional {params_queue.qsize()} pages")
        if not params_queue.empty():
            thread_count = min(params_queue.qsize(), self.MAX_WORKERS)
            logger.info(f"Creating {thread_count} worker threads")
            for i in range(thread_count):
                thread = threading.Thread(target=self._fetch_worker,
                                          name=f"FetchWorker_{i}",
                                          args=(query_params, params_queue))
                thread.daemon = True
                thread.start()
                logger.info(f"Thread {thread.name} started")
            params_queue.join()
            logger.info("No more pages to fetch")

    def _fetch_worker(self, base_params: Params,
                      page_params_queue: Queue[Params]) -> None:
        """Worker fetch method to be used in a thread.

        Make consecutive requests to the server and put results in the
        output_queue.

        Args:
            base_params: Base parameters to use for the request. These
                parameters are the same for every request.
            page_params_queue: Additional parameters that differ
                between page requests.
        """
        # Get the first page parameters
        try:
            page_params = page_params_queue.get(block=False)
        except queue.Empty:
            logger.info(f"No more pages in queue")
            return  # no more requests to make

        while True:
            # Handle request
            full_params = base_params | page_params
            try:
                response = self.make_request(full_params)
            except QueryFailed as e:
                # Log error and move on to the next page
                logger.error(f"Request failed. Parameters: {full_params}")
                logger.debug(f"Response: {e.response.text}")
            else:
                self._parse_response(response)

            # Mark request as done and get the next one
            page_params_queue.task_done()
            try:
                page_params = page_params_queue.get(block=False)
            except queue.Empty:
                logger.info(f"No more pages in queue")
                return  # no more requests to make
            time.sleep(self.COOLDOWN_TIME)
