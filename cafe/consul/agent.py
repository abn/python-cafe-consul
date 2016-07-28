from os import getenv
from twisted.internet import defer, reactor
from twisted.internet import task

from cafe.consul.lock import ConsulLock
from cafe.consul.semaphore import ConsulSemaphore
from cafe.consul.session import ConsulSessionWrapper
from cafe.logging import LoggedObject
from cafe.twisted import async_sleep
from consul.base import Consul as ConsulBase, ConsulException
from consul.std import Consul as ConsulStandardAgent
from consul.twisted import Consul

CONSUL_HOST = getenv('CONSUL_HOST', '127.0.0.1')
CONSUL_PORT = int(getenv('CONSUL_PORT', '8500'))
CONSUL_TOKEN = getenv('CONSUL_TOKEN', None)
CONSUL_SCHEME = getenv('CONSUL_SCHEME', 'http')
CONSUL_DC = getenv('CONSUL_DC', None)
CONSUL_VERIFY = bool(getenv('CONSUL_VERIFY', 'True'))


class SimpleConsulClient(ConsulStandardAgent, object):
    def __init__(self, host=CONSUL_HOST, port=CONSUL_PORT, token=CONSUL_TOKEN, scheme=CONSUL_SCHEME, dc=CONSUL_DC,
                 verify=CONSUL_VERIFY, **kwargs):
        super(SimpleConsulClient, self).__init__(
            host=host, port=port, token=token, scheme=scheme, dc=dc, verify=verify, **kwargs)


class SessionedConsulAgent(LoggedObject, ConsulBase):
    GLOBAL_RETRY_DELAY_SECONDS = int(getenv('CONSUL_GLOBAL_RETRY_DELAY_SECONDS', 10))

    # noinspection PyMissingConstructor
    def __init__(self, name, behavior='release', ttl=None, heartbeat_interval=None, lock_delay=None, host=CONSUL_HOST,
                 port=CONSUL_PORT, token=CONSUL_TOKEN, scheme=CONSUL_SCHEME, dc=CONSUL_DC, verify=CONSUL_VERIFY,
                 **kwargs):
        """
        :type behavior: str
        :param behavior: consul session behavior (release, delete)
        :type ttl: int
        :param ttl: time to live for the session before it is invalidated
        :param name: session name to use
        :type name: str
        :param heartbeat_interval: interval (in seconds) in which a session
            should be renewed, this value is also used as the session ttl.
        :type heartbeat_interval: str
        :type lock_delay: int
        :param lock_delay: consul lock delay to use for sessions
        """
        assert behavior in ('release', 'delete')
        self.name = name
        self._agent = Consul(host=host, port=port, token=token, scheme=scheme, dc=dc, verify=verify, **kwargs)
        self.session = ConsulSessionWrapper(self, ttl, lock_delay, heartbeat_interval)

    @property
    def agent(self):
        return self._agent

    def __getattr__(self, item):
        """
        :type item: str
        :rtype: consul.base.Consul
        """
        return getattr(self.agent, item)

    @defer.inlineCallbacks
    def wait_for_ready(self, attempts=None, interval=None):
        yield self.session.wait(attempts, interval)

    def lock(self, key, value='', delete=False, **kwargs):
        return ConsulLock(self, key=key, value=value, delete=delete, **kwargs)

    def semaphore(self, key, value='', delete=False, limit=3, cardinality=None, **kwargs):
        return ConsulSemaphore(
            self, key=key, value=value, delete=delete, limit=limit, cardinality=cardinality, **kwargs)


class DistributedConsulAgent(SessionedConsulAgent):
    ELECTION_EXPIRY = int(getenv('CONSUL_ELECTION_EXPIRY', ConsulSessionWrapper.SESSION_HEARTBEAT_SECONDS))
    ELECTION_RETRY = int(getenv('CONSUL_ELECTION_RETRY', ConsulSessionWrapper.SESSION_CREATE_RETRY_DELAY_SECONDS))

    def __init__(self, name, behavior='delete', ttl=None, heartbeat_interval=None, lock_delay=None, host=CONSUL_HOST,
                 port=CONSUL_PORT, token=CONSUL_TOKEN, scheme=CONSUL_SCHEME, dc=CONSUL_DC, verify=CONSUL_VERIFY,
                 **kwargs):
        super(DistributedConsulAgent, self).__init__(
            name, behavior=behavior, ttl=ttl, heartbeat_interval=heartbeat_interval, lock_delay=lock_delay,
            host=host, port=port, token=token, scheme=scheme, dc=dc, verify=verify, **kwargs
        )
        self._leader = None
        self.is_leader = False
        self._abstain = False
        self.leader_key = 'service/{}/leader'.format(name)
        self._refresh_index = None
        self.refresh_leader_task = task.LoopingCall(self.refresh_leader)
        self.refresh_leader_task.start(interval=0, now=True)
        reactor.addSystemEventTrigger(
            'before', 'shutdown',
            lambda: self.refresh_leader_task.stop if self.refresh_leader_task.running else None
        )

    @property
    def leader(self):
        """Current leader data"""
        return self._leader

    @leader.setter
    def leader(self, value):
        self._leader = value
        if value is None:
            # immediate retry if we are the leader
            reactor.callLater(0, self.acquire_leadership)

    @defer.inlineCallbacks
    def refresh_leader(self):
        self._refresh_index = yield self.update_leader(
            index=self._refresh_index, sleep_on_error=True)

    @defer.inlineCallbacks
    def update_leader(self, index=None, sleep_on_error=True):
        try:
            index, data = yield self.agent.kv.get(key=self.leader_key, index=index)
            if data is not None and hasattr(data, 'get'):
                self.leader = data.get('Value', None)
            else:
                # the key does not exist, we are using 'delete' behaviour
                self.leader = None
            self.logger.trace('name=%s session=%s leader=%s', self.name, self.session.uuid, self.leader)
            defer.returnValue(index)
        except ConsulException as e:
            self.logger.error(
                'leader update failed, retrying later exception=%s message=%s', e.__class__.__name__, e.message)
            if sleep_on_error:
                yield async_sleep(self.session.SESSION_CREATE_RETRY_DELAY_SECONDS)

    @property
    def candidate_data(self):
        """
        Data to use when applying for leadership.

        :rtype: str
        """
        return self.session.uuid

    @defer.inlineCallbacks
    def acquire_leadership(self):
        """
        Try to acquire leadership.

        :rtype: bool
        """
        if self.session.uuid is None:
            self.logger.trace('name=%s session not ready, retrying later', self.name)
            reactor.callLater(self.ELECTION_RETRY, self.acquire_leadership)
        elif self._abstain:
            self.logger.trace('name=%s session=%s currently abstaining from elections, skipping', self.name,
                              self.session.uuid)
        elif self.leader is not None:
            self.logger.trace('name=%s leader exists, skipping', self.name)
        else:
            value = self.candidate_data
            self.logger.trace('name=%s session=%s can i haz leadership', self.name, self.session.uuid)
            try:
                self.is_leader = yield self.lock(key=self.leader_key, value=value).acquire()
                if self.is_leader:
                    self.logger.info('name=%s session=%s acquired leadership', self.name, self.session.uuid)
                else:
                    # handle consul lock-delay safe guard, retry a bit later
                    reactor.callLater(self.ELECTION_RETRY, self.acquire_leadership)
                self.logger.trace('name=%s session=%s acquired_leadership=%s', self.name, self.session.uuid,
                                  self.is_leader)
            except ConsulException as e:
                self.logger.trace('name=%s session=%s acquiring leadership attempt failed reason=%s', self.name,
                                  self.session.uuid, e.message)
        defer.returnValue(self.is_leader)

    @defer.inlineCallbacks
    def relinquish_leadership(self, abstain=False):
        """
        :param abstain: abstain from next election till a new leader is elected,
            WARNING: be sure you know what you are doing, this can lead to potential deadlocks.
        :type abstain: bool
        """
        try:
            self.logger.info('name=%s session=%s relinquishing leadership', self.name, self.session.uuid)
            self._abstain = abstain
            yield self.lock(key=self.leader_key).release()
            if abstain:
                self.logger.debug('name=%s session=%s waiting for next leader', self.name, self.session.uuid)
                yield self.wait_for_leader()
        finally:
            self._abstain = False

    @defer.inlineCallbacks
    def wait_for_leader(self, attempts=None, interval=None):
        """
        :param attempts: number of attempts before giving up, if None there is
            no giving up.
        :type attempts: int or None
        :param interval: interval (in seconds), by default the election retry interval is used
        :type interval: int or None
        """
        yield self.wait_for_ready()
        interval = interval if interval is not None else self.ELECTION_RETRY
        attempt = 0
        while self.leader is None and (attempts is None or attempt <= attempts):
            attempt += 1
            self.logger.debug('attempt=%s interval=%ss waiting for leader to be elected', attempt, interval)
            yield async_sleep(interval)

    @defer.inlineCallbacks
    def wait_for_leadership(self, index=None):
        yield self.wait_for_leader()
        while not self.is_leader:
            index = yield self.update_leader(index=index, sleep_on_error=True)
