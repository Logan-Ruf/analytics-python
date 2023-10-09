from datetime import date, datetime
from io import BytesIO
from gzip import GzipFile
import logging
import json
from dateutil.tz import tzutc
from requests.auth import HTTPBasicAuth
from requests import sessions

from segment.analytics.version import VERSION
from segment.analytics.utils import remove_trailing_slash

_session = sessions.Session()


def send_post(*args, **kwargs):
    log = logging.getLogger('segment')
    res = _session.post(*args, **kwargs)

    if res.status_code == 200:
        log.debug('data uploaded successfully')
        return res

    try:
        payload = res.json()
        log.debug('received response: %s', payload)
        raise APIError(res.status_code, payload['code'], payload['message'])
    except ValueError:
        raise APIError(res.status_code, 'unknown', res.text)


def post(write_key, host=None, endpoint=None, gzip=False, timeout=15, proxies=None, **kwargs):
    """Post the `kwargs` to the API"""
    log = logging.getLogger('segment')
    body = kwargs
    body["sentAt"] = datetime.utcnow().replace(tzinfo=tzutc()).isoformat()
    url = remove_trailing_slash(host or 'https://api.segment.io') + (endpoint or '/v1/batch')
    auth = HTTPBasicAuth(write_key, '')
    data = json.dumps(body, cls=DatetimeSerializer)
    log.debug('making request: %s', data)
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'analytics-python/' + VERSION
    }
    if gzip:
        headers['Content-Encoding'] = 'gzip'
        buf = BytesIO()
        with GzipFile(fileobj=buf, mode='w') as gz:
            # 'data' was produced by json.dumps(),
            # whose default encoding is utf-8.
            gz.write(data.encode('utf-8'))
        data = buf.getvalue()

    kwargs = {
        "data": data,
        "auth": auth,
        "headers": headers,
        "timeout": timeout,
    }

    if proxies:
        kwargs['proxies'] = proxies

    return send_post(url, **kwargs)


def start_object_session(write_key, timeout=15, proxies=None):
    """Start a new session with Segment's Object API."""
    log = logging.getLogger('segment')
    auth = HTTPBasicAuth(write_key, '')
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'analytics-python/' + VERSION
    }

    kwargs = {
        "auth": auth,
        "headers": headers,
        "timeout": timeout,
    }

    if proxies:
        kwargs['proxies'] = proxies

    res = send_post('https://objects-bulk-api.segmentapis.com/v0/start', auth=auth,
                    headers=headers, timeout=timeout)
    data = res.json()
    sync_id = data['sync_id']
    log.debug(f'object session start: {sync_id = }')
    return data['sync_id']


def post_object(url, batch, write_key=None, timeout=15, proxies=None):
    log = logging.getLogger('segment')
    auth = HTTPBasicAuth(write_key, '')

    data_str = "\n".join(json.dumps(obj) for obj in batch)
    buf = BytesIO()
    with GzipFile(fileobj=buf, mode='w') as gz:
        # 'data' was produced by json.dumps(),
        # whose default encoding is utf-8.
        gz.write(data_str.encode('utf-8'))
    data = buf.getvalue()

    headers = {
        'Content-Encoding': 'gzip',
        'Content-Type': 'application/json',
        'User-Agent': 'analytics-python/' + VERSION
    }

    kwargs = {
        "auth": auth,
        "headers": headers,
        "timeout": timeout,
        "data": data,
    }

    if proxies:
        kwargs['proxies'] = proxies

    log.debug(f'Making request to Segment Object API:\n {data_str}')

    return send_post(url, **kwargs)


class APIError(Exception):

    def __init__(self, status, code, message):
        self.message = message
        self.status = status
        self.code = code

    def __str__(self):
        msg = "[Segment] {0}: {1} ({2})"
        return msg.format(self.code, self.message, self.status)


class DatetimeSerializer(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)
