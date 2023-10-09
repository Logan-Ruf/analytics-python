import logging
from threading import Thread
import monotonic
import backoff
import json

from segment.analytics.request import post, APIError, DatetimeSerializer, start_object_session, post_object

from queue import Empty

MAX_MSG_SIZE = 32 << 10

# Our servers only accept batches less than 500KB. Here limit is set slightly
# lower to leave space for extra data that will be added later, eg. "sentAt".
BATCH_SIZE_LIMIT = 475000


class Consumer(Thread):
    """Consumes the messages from the client's queue."""
    log = logging.getLogger('segment')

    def __init__(self, queue, write_key, upload_size=100, host=None, endpoint=None,
                 on_error=None, upload_interval=0.5, gzip=False, retries=10,
                 timeout=15, proxies=None):
        """Create a consumer thread."""
        Thread.__init__(self)
        # Make consumer a daemon thread so that it doesn't block program exit
        self.daemon = True
        self.upload_size = upload_size
        self.upload_interval = upload_interval
        self.write_key = write_key
        self.host = host
        self.endpoint = endpoint
        self.on_error = on_error
        self.queue = queue
        self.gzip = gzip
        # It's important to set running in the constructor: if we are asked to
        # pause immediately after construction, we might set running to True in
        # run() *after* we set it to False in pause... and keep running
        # forever.
        self.running = True
        self.retries = retries
        self.timeout = timeout
        self.proxies = proxies
        # Object API
        self.sessionId = None

    def run(self):
        """Runs the consumer."""
        self.log.debug('consumer is running...')
        while self.running:
            self.upload()

        self.log.debug('consumer exited.')

    def pause(self):
        """Pause the consumer."""
        self.running = False

    def upload(self):
        """Upload the next batch of items, return whether successful."""
        success = False
        batch = self.next()
        if len(batch) == 0:
            return False

        try:
            self.request(batch)
            success = True
        except Exception as e:
            self.log.error('error uploading: %s', e)
            success = False
            if self.on_error:
                self.on_error(e, batch)
        finally:
            # mark items as acknowledged from queue
            for _ in batch:
                self.queue.task_done()
            return success

    def next(self):
        """Return the next batch of items to upload."""
        queue = self.queue
        items = []
        objects = []

        start_time = monotonic.monotonic()
        total_size = 0

        while len(items) < self.upload_size:
            elapsed = monotonic.monotonic() - start_time
            if elapsed >= self.upload_interval:
                break
            try:
                item = queue.get(
                    block=True, timeout=self.upload_interval - elapsed)
                item_size = len(json.dumps(
                    item, cls=DatetimeSerializer).encode())
                if item_size > MAX_MSG_SIZE:
                    self.log.error(
                        'Item exceeds 32kb limit, dropping. (%s)', str(item))
                    continue
                if getattr(item, 'type', None) == 'object':
                    objects.append(item)
                else:
                    items.append(item)
                # TODO: total_size needs to account for both APIs
                total_size += item_size
                if total_size >= BATCH_SIZE_LIMIT:
                    self.log.debug(
                        'hit batch size limit (size: %d)', total_size)
                    break
            except Empty:
                break
            except Exception as e:
                self.log.exception('Exception: %s', e)

        return items

    def request_object(self, batch):
        """Upload object data to the object API """

        # TODO add session keep alive logic
        if self.sessionId is None:
            self.sessionId = self._request(start_object_session, self.write_key, timeout=self.timeout,
                                           proxies=self.proxies)

        url = f'https://objects-bulk-api.segmentapis.com/v0/upload/{self.sessionId}'

        self._request(post_object, url, batch, write_key=self.write_key, timeout=self.timeout, proxies=self.proxies)

    def request(self, batch):
        """Attempt to upload the batch and retry before raising an error """

        self._request(post, self.write_key, self.host, endpoint=self.endpoint, gzip=self.gzip,
                      timeout=self.timeout, batch=batch, proxies=self.proxies)

    def _request(self, request_func, *args, **kwargs):
        """Request wrapper that will retry before raising an error """

        def fatal_exception(exc):
            if isinstance(exc, APIError):
                # retry on server errors and client errors
                # with 429 status code (rate limited),
                # don't retry on other client errors
                return (400 <= exc.status < 500) and exc.status != 429
            else:
                # retry on all other errors (eg. network)
                return False

        @backoff.on_exception(
            backoff.expo,
            Exception,
            max_tries=self.retries + 1,
            giveup=fatal_exception)
        def send_request():
            return request_func(*args, **kwargs)

        return send_request()
