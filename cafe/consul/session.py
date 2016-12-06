from json import dumps
from os import getenv
from time import time
from twisted.internet import task, reactor, defer

from cafe.logging import LoggedObject
from cafe.twisted import async_sleep
from consul import ConsulException
from consul.base import Consul as ConsulBase, NotFound


# noinspection PyMethodOverriding
class ConsulSessionWrapper(LoggedObject, ConsulBase.Session):
    SESSION_TTL_SECONDS = int(getenv('CONSUL_SESSION_TTL_SECONDS', '75'))
    SESSION_HEARTBEAT_SECONDS = int(getenv('CONSUL_SESSION_HEARTBEAT_SECONDS', '75'))
    SESSION_LOCK_DELAY_SECONDS = int(getenv('CONSUL_SESSION_LOCK_DELAY_SECONDS', '15'))
    SESSION_CREATE_RETRY_DELAY_SECONDS = int(getenv('CONSUL_GLOBAL_RETRY_DELAY_SECONDS', 10))

    # noinspection PyMissingConstructor
    def __init__(self, agent, ttl=None, lock_delay=None, heartbeat_interval=None):
        self._agent = agent
        self._uuid = None
        self.ttl = ttl or self.SESSION_TTL_SECONDS
        self.heartbeat_interval = heartbeat_interval or self.SESSION_HEARTBEAT_SECONDS
        self.lock_delay = lock_delay or self.SESSION_LOCK_DELAY_SECONDS
        if self.lock_delay is not None and 0 < self.lock_delay < 60:
            self.logger.debug('invalid lock-delay=%s specified, using defaults', self.lock_delay)
            self.lock_delay = 15
        self._renewed_at = None
        self.heartbeat = task.LoopingCall(self.renew)
        reactor.callLater(0, self.create)
        reactor.addSystemEventTrigger('before', 'shutdown', self.destroy)

    @property
    def agent(self):
        return self._agent

    @property
    def base(self):
        return self.agent.agent.session

    @property
    def name(self):
        return self.agent.name

    @property
    def uuid(self):
        return self._uuid

    @defer.inlineCallbacks
    def create(self, retry=True):
        """
        Create a session, and set the internal `id` property. If an
        exception is encountered during creation, the operation will be
        reattempted again at half the ttl of the session itself if `retry` is
        `True`.

        :param retry: retry later if creation fails
        :type retry: bool
        :rtype: bool
        """
        session_created = False
        try:
            self.logger.trace('attempting to create a new session')
            self._uuid = yield self.base.create(
                self.name, behavior='delete', ttl=self.ttl, lock_delay=self.lock_delay)
            self._renewed_at = time()
            self.logger.info('name=%s session=%s created', self.name, self.uuid)
            reactor.callLater(0, self.watch_for_session_change)

            if not self.heartbeat.running:
                reactor.callLater(0, self.heartbeat.start, interval=self.heartbeat_interval)
        except ConsulException as e:
            self.logger.warning(
                'name=%s, session=%s creation failed, retrying reason=%s', self.name, self.uuid, e.message)
            if retry:
                # try again in SESSION_CREATE_RETRY_DELAY_SECONDS
                reactor.callLater(self.SESSION_CREATE_RETRY_DELAY_SECONDS, self.create)
        else:
            session_created = True
        defer.returnValue(session_created)

    @defer.inlineCallbacks
    def recreate(self, retry=True, force=False):
        """
        Recreate a session. If instance hsa a uuid and has remaining life, this is skipped unless forced.

        :param retry: retry later if creation fails
        :type retry: bool
        :param force: force session recreation
        :type force: bool
        :rtype: bool
        """
        self.logger.trace('name=%s session=%s session recreate requested', self.name, self.uuid)
        if self.uuid is not None and not force and self.life > 0:
            self.logger.debug(
                'name=%s session=%s session recreate requested but not forced, skipping as life is not 0',
                self.name, self.uuid
            )
            defer.returnValue(False)
        yield self.destroy()
        created = yield self.create(retry=retry)
        defer.returnValue(created)

    @defer.inlineCallbacks
    def watch_for_session_change(self, index=None):
        """If the session is changed or removed on the Consul server/cluster,
        we need to know about it. Here we call the /v1/session/info endpoint
        to get the session details.

        When `index` is provided, the call to `info` will block, until either
        the wait time is reached, or something happens to the session that
        makes the GET invalid. If the wait time is reached, we simply
        reschedule `watch_for_session_change`.  If the session is no longer
        valid, we schedule a new session creation.

        :param index: The current Consul index. If provided, the `info` get
                      will block.
        :type index: int
        :rtype: None
        """
        if self.uuid is None:
            self.logger.debug('name=%s session not set, discontinuing watch on session')
            defer.returnValue(None)

        try:
            self.logger.trace('name=%s session=%s fetch info', self.name, self.uuid)
            index, session = yield self.base.info(self.uuid, index=index, consistency='consistent')
        except ConsulException as e:
            self.logger.debug(
                'name=%s session=%s fetch info failed, retrying later reason=%s', self.name, self.uuid, e.message)
            # since something went wrong with the query to consul, wait for a bit before retrying
            reactor.callLater(self.SESSION_CREATE_RETRY_DELAY_SECONDS, self.watch_for_session_change, index=index)
        else:
            if self.uuid is not None and session is None:
                self.logger.warning(
                    'The Consul session is missing. This should almost never happen, and if it occurs frequently then '
                    'it indicates that the Consul server cluster is unhealthy. This client will attempt to create a '
                    'new session.'
                )
                reactor.callLater(0, self.recreate, force=True)
                defer.returnValue(None)
            self.logger.trace('name=%s session=%s info=%s', self.name, self.uuid, dumps(session, indent=2))
            # Since the session is valid, just go back to watching immediately
            reactor.callLater(0, self.watch_for_session_change, index=index)

    @defer.inlineCallbacks
    def renew(self):
        """Renew session if one is active, else do nothing."""
        try:
            if self.uuid is not None:
                self.logger.trace('name=%s session=%s renewing session', self.name, self.uuid)
                yield self.base.renew(self.uuid)
                self._renewed_at = time()
        except NotFound:
            self.logger.warning('name=%s session=%s renewal failed reason=session not found', self.name, self.uuid)
            yield self.recreate(force=True)
        except ConsulException as e:
            self.logger.warning(
                'session=%s renewal attempt failed reason=%s',
                self.uuid, e.message
            )

    @property
    def life(self):
        """
        Life remaining for this session in seconds. Life is calculated as Session TTL - time since last renewal.

        :rtype: float
        """
        if self._renewed_at is not None:
            return max(self.ttl - (time() - self._renewed_at), 0.0)
        return 0.0

    @defer.inlineCallbacks
    def destroy(self, dc=None):
        """Destroy a session if one is active, else do nothing."""
        if self.uuid is None:
            defer.returnValue(None)

        if self.heartbeat.running:
            self.logger.trace('name=%s session=%s stopping heartbeat', self.name, self.uuid)
            self.heartbeat.stop()

        try:
            self.logger.trace('name=%s session=%s destroying session', self.name, self.uuid)
            yield self.base.destroy(self.uuid, dc=dc)
            self.logger.info('name=%s session=%s destroyed session', self.name, self.uuid)
        except ConsulException as e:
            self.logger.warning(
                'name=%s session=%s destruction attempt failed but session will be invalidated reason=%s',
                self.name, self.uuid, e.message
            )
        finally:
            self._uuid = None
            self._renewed_at = None

    @property
    def ready(self):
        """Check if a session has been established with consul."""
        return self.uuid is not None

    @defer.inlineCallbacks
    def wait(self, attempts=None, interval=None):
        """
        :param attempts: number of attempts before giving up, if None there is
            no giving up.
        :type attempts: int or None
        :param interval: interval (in seconds), by default the create retry interval is used
        :type interval: int or None
        """
        interval = interval if interval is not None else self.SESSION_CREATE_RETRY_DELAY_SECONDS
        attempt = 0
        while not self.ready and (attempts is None or attempt <= attempts):
            attempt += 1
            self.logger.debug('attempt=%s interval=%ss waiting for session to established', attempt, interval)
            yield async_sleep(interval)
