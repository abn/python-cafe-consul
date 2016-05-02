from os import getenv
from twisted.internet import defer, reactor, task

from cafe.logging import LoggedObject
from cafe.twisted import async_sleep
from consul import Consul as ConsulSynchronous
from consul.base import ConsulException
from consul.twisted import Consul

CONSUL_HOST = getenv('CONSUL_HOST', '127.0.0.1')
CONSUL_PORT = int(getenv('CONSUL_PORT', '8500'))
CONSUL_TOKEN = getenv('CONSUL_TOKEN', None)
CONSUL_SCHEME = getenv('CONSUL_SCHEME', 'http')
CONSUL_DC = getenv('CONSUL_DC', None)
CONSUL_VERIFY = bool(getenv('CONSUL_VERIFY', 'True'))


class SessionedConsulAgent(LoggedObject, object):
    SESSION_NAME = getenv('CONSUL_SESSION_NAME', None)
    SESSION_RENEWAL_SECONDS = int(getenv(
        'CONSUL_SESSION_RENEWAL_SECONDS', '75'))

    def __init__(self, name=None, renewal_interval=None,
                 host=CONSUL_HOST, port=CONSUL_PORT, token=CONSUL_TOKEN,
                 scheme=CONSUL_SCHEME, dc=CONSUL_DC, verify=CONSUL_VERIFY,
                 **kwargs):
        """
        :param name: session name to use
        :type name: str
        :param renewal_interval: interval (in seconds) in which a session
            should be renewed, this value is also used as the session ttl.
        :type renewal_interval: str
        """
        self.name = name if name is not None else self.SESSION_NAME
        self.ttl = renewal_interval \
            if renewal_interval is not None else self.SESSION_RENEWAL_SECONDS
        self.consul = Consul(
            host=host, port=port, token=token, scheme=scheme, dc=dc,
            verify=verify, **kwargs)
        self.consul_sync = ConsulSynchronous(
            host=host, port=port, token=token, scheme=scheme, dc=dc,
            verify=verify, **kwargs)
        self.session_id = None
        self.heartbeat = task.LoopingCall(self.session_renew)
        self.start()
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)

    def start(self):
        """
        Start this instance by creating a new session and starting heartbeat
        renewals.
        """
        self.logger.debug('starting consul agent')
        reactor.callLater(0, self.session_create)
        reactor.callLater(self.ttl, self.heartbeat.start, interval=self.ttl)

    def stop(self):
        """
        Execute clean-up tasks, including stopping heartbeat (renewal) and
        session destruction.
        """
        self.logger.debug('stopping consul agent')

        if self.heartbeat.running:
            self.logger.trace('stopping heartbeat')
            self.heartbeat.stop()

        if self.session_id is not None:
            self.logger.trace('session=%s destroying', self.session_id)
            self.consul_sync.session.destroy(session_id=self.session_id)

    @property
    def ready(self):
        """Check if a session has been established with consul."""
        return self.session_id is not None

    @defer.inlineCallbacks
    def wait_for_ready(self, attempts=None, interval=3):
        """
        :param attempts: number of attempts before giving up, if None there is
            no giving up.
        :type attempts: int or None
        :param interval: interval (in seconds) between each check, if None,
            session ttl / 4 is used. (default=3)
        :type interval: int or None
        """
        interval = interval if interval is not None else self.ttl / 4
        attempt = 0
        while not self.ready and (attempts is None or attempt <= attempts):
            attempt += 1
            self.logger.debug(
                'attempt=%s interval=%ss waiting for session to established',
                attempt, interval
            )
            yield async_sleep(interval)

    @defer.inlineCallbacks
    def session_create(self, retry=True):
        """
        Create a session, and set the internal `session_id` property. If an
        exception is encountered during creation, the operation will be
        reattempted again at half the ttl of the session itself if `retry` is
        `True`.

        :param retry: retry later if creation fails
        :type retry: bool
        """
        try:
            self.logger.trace('attempting to create a new session')
            self.session_id = yield self.consul.session.create(self.name)
            self.logger.info('session=%s created', self.session_id)
        except ConsulException as e:
            self.logger.warning(
                'session=%s creation failed, retrying reason=%s',
                self.session_id, e.message)
            if retry:
                reactor.callLater(self.ttl / 2, self.session_create)

    @defer.inlineCallbacks
    def session_renew(self):
        """Renew session if one is active, else do nothing."""
        try:
            if self.session_id is not None:
                self.logger.trace('session=%s renewing', self.session_id)
                yield self.consul.session.renew(self.session_id)
        except ConsulException as e:
            self.logger.warning(
                'session=%s renewal attempt failed reason=%s',
                self.session_id, e.message
            )

    @classmethod
    def create_lock_key(cls, *args):
        """Helper method to create a valid key provider components as args"""
        return '/'.join(args)

    @defer.inlineCallbacks
    def _lock(self, action, key, value=None):
        """
        Internal method to acquire/release a lock

        :type key: str
        :type value: str
        """
        assert action in ('acquire', 'release')
        self.logger.debug(
            'lock=%s action=%s session=%s value=%s',
            key, action, self.session_id, value
        )
        if not self.ready:
            self.logger.trace(
                'lock=%s action=%s failed as consul agent is not ready',
                key, action
            )
            result = False
        else:
            result = yield self.consul.kv.put(
                key=key, value=value, **{action: self.session_id})
        self.logger.info('lock=%s action=%s result=%s', key, action, result)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def acquire_lock(self, key, value=None):
        """
        Acquire a lock with a provided value.

        :type key: str
        :type value: str
        """
        result = yield self._lock(action='acquire', key=key, value=value)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def release_lock(self, key, value=None):
        """
        Release a lock with a provided value.

        :type key: str
        :type value: str
        """
        result = yield self._lock(action='release', key=key, value=value)
        if result:
            try:
                self.logger.trace('key=%s deleting as lock is released', key)
                yield self.consul.kv.delete(key=key)
            except ConsulException as e:
                self.logger.warning(
                    'key=%s failed to delete reason=%s', key, e.message)
        defer.returnValue(result)
