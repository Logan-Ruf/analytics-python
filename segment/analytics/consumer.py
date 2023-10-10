import logging
from threading import Thread
import monotonic
import backoff
import json

from segment.analytics.request import post, APIError, DatetimeSerializer, start_object_session, post_object, \
    keep_alive_object_session

from queue import Empty

from segment.analytics.utils import remove_trailing_slash

MAX_MSG_SIZE = 32 << 10

# Our servers only accept batches less than 500KB. Here limit is set slightly
# lower to leave space for extra data that will be added later, eg. "sentAt".
BATCH_SIZE_LIMIT = 475000


class Consumer(Thread):
    """Consumes the messages from the client's queue."""
    log = logging.getLogger('segment')

    def __init__(self, queue, write_key, upload_size=100, host=None, endpoint=None,
                 on_error=None, upload_interval=0.5, gzip=False, retries=10,
                 timeout=15, proxies=None, keep_alive=True):
        """Create a consumer thread."""
        Thread.__init__(self)
        # Make consumer a daemon thread so that it doesn't block program exit
        self.daemon = True
        self.upload_size = upload_size
        self.upload_interval = upload_interval
        self.write_key = write_key
        self.host = host or 'https://api.segment.io'
        self.endpoint = endpoint or '/v1/batch'
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
        self.object_host = 'https://objects-bulk-api.segmentapis.com'
        self.object_start_endpoint = '/v0/start'
        self.object_upload_endpoint = '/v0/upload/{sync_id}'
        self.object_keep_alive_endpoint = '/v0/keep-alive/{sync_id}'
        self.session_id = None
        self.keep_alive = keep_alive
        self.keep_alive_time = None
        self.keep_alive_interval = 9.5 * 60  # 9.5 minutes in seconds

    def run(self):
        """Runs the consumer."""
        self.log.debug('consumer is running...')
        while self.running:
            self.upload()
            self.check_keep_alive()

        self.log.debug('consumer exited.')

    def pause(self):
        """Pause the consumer."""
        self.running = False

    def upload(self):
        """Upload the next batch of items, return whether successful."""
        success = False
        batch, objects = self.next()
        if len(batch) == 0 and len(objects) == 0:
            return False

        try:
            self.request(batch)
            self.request_object(objects)
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
                if item['type'] == 'object':
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

        return items, objects

    def check_keep_alive(self):
        """Check if the object session needs to be kept alive """
        if not self.keep_alive or self.keep_alive_time is None or self.session_id is None:
            return
        now = monotonic.monotonic()
        elapsed = now - self.keep_alive_time
        if elapsed >= self.keep_alive_interval:
            self.log.debug('keep alive interval passed')
            self.request_keep_alive()

    def request_keep_alive(self):
        """Keep alive the object session """
        if self.session_id is None:
            self.log.error('No object session id found, cannot keep alive session')
            return
        url = remove_trailing_slash(self.object_host) + self.object_keep_alive_endpoint.format(sync_id=self.session_id)
        self._request(keep_alive_object_session, url, self.write_key, self.session_id,
                      timeout=self.timeout, proxies=self.proxies)
        self.keep_alive_time = monotonic.monotonic()

    def request_object(self, batch):
        """Upload object data to the object API """
        if len(batch) == 0:
            return

        # TODO add session keep alive logic
        if self.session_id is None:
            url = remove_trailing_slash(self.object_host) + self.object_start_endpoint
            self.session_id = self._request(start_object_session, url, self.write_key,
                                            timeout=self.timeout, proxies=self.proxies)

        url = remove_trailing_slash(self.object_host) + self.object_upload_endpoint.format(sync_id=self.session_id)
        self._request(post_object, url, batch, write_key=self.write_key, timeout=self.timeout, proxies=self.proxies)
        self.keep_alive_time = monotonic.monotonic()

    def request(self, batch):
        """Attempt to upload the batch and retry before raising an error """
        if len(batch) == 0:
            return

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
