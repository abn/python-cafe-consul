from abc import abstractmethod
from twisted.internet import defer

from cafe.abc import AbstractClass
from cafe.logging import LoggedObject
from consul import ConsulException


class ConsulLockFailed(ConsulException):
    pass


class ConsulLockContext(LoggedObject, object):
    def __init__(self, lock, attempts=None, delete=None, **kwargs):
        self.lock = lock
        """:type: cafe.consul.lock.AbstractConsulLock"""
        self.attempts = attempts
        self.delete = delete
        self.kwargs = kwargs
        self.release_kwargs = {}
        self.locked = False

    @property
    def key(self):
        return self.lock.key

    def set_release_kwarg(self, **kwargs):
        self.release_kwargs.update(other=kwargs)

    @defer.inlineCallbacks
    def __enter__(self):
        super(ConsulLockContext, self).__enter__()
        self.logger.trace('lock=%s entering context', self.lock.key)
        self.locked = yield self.lock.wait(
            value=self.lock.value, attempts=self.attempts, **self.kwargs)
        if not self.locked:
            raise ConsulLockFailed(self.lock.key)
        defer.returnValue(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.locked:
            self.lock.release(delete=self.delete, **self.release_kwargs)
        super(ConsulLockContext, self).__exit__(exc_type, exc_val, exc_tb)


class ConsulMultiLockContext(LoggedObject, object):

    def __init__(self, *locks, **kwargs):
        self.locks = locks
        self.kwargs = kwargs
        self._context_stack = []

    @defer.inlineCallbacks
    def __enter__(self):
        for lock in self.locks:
            context = lock.context(**self.kwargs)
            yield context.__enter__()
            self._context_stack.append(context)
        defer.returnValue(super(ConsulMultiLockContext, self).__enter__())

    @defer.inlineCallbacks
    def _exit(self):
        while self._context_stack:
            context = self._context_stack.pop()
            yield context.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit()
        super(ConsulMultiLockContext, self).__exit__(exc_type, exc_val, exc_tb)


class AbstractConsulLock(LoggedObject, AbstractClass):
    def __init__(self, agent, key, value='', delete=False, **kwargs):
        self.agent = agent
        """:type: cafe.consul.agent.SessionedConsulAgent"""
        self.key = key if not isinstance(key, list) else self.make_key(*key)
        self.value = value
        self.delete = delete
        self._kwargs = kwargs

    def kwargs(self, **kwargs):
        new_kwargs = self._kwargs.copy()
        new_kwargs.update(kwargs)
        return new_kwargs

    @classmethod
    def make_key(cls, *args):
        """Helper method to create a valid key provider components as args"""
        return '/'.join(args)

    @abstractmethod
    def acquire(self, value=None, **kwargs):
        """
        Acquire a lock with a provided value.

        :type value: str
        """
        self.logger.warning('unimplemented method called')
        raise NotImplementedError

    @abstractmethod
    def release(self, value=None, delete=None, **kwargs):
        """
        Release a lock with a provided value.

        :type value: str
        :type delete: bool
        """
        self.logger.warning('unimplemented method called')
        raise NotImplementedError

    @defer.inlineCallbacks
    def wait(self, value=None, attempts=None, **kwargs):
        """
        Wait till a lock is acquired. If attempts is None, wait for ever.

        :type value: str
        :type attempts: None or int
        :rtype: bool
        """
        index = None
        result = False
        yield self.agent.wait_for_ready()
        while not result and (attempts is None or attempts >= 0):
            self.logger.debug(
                'lock=%s waiting for lock; %s attempts left',
                self.key, attempts if attempts is not None else 'infinite')
            result = yield self.acquire(value=value, **kwargs)
            if not result:
                # we use wait to deter any dead-locks that might happen due to lock-delays
                wait = '{}s'.format(self.agent.session.lock_delay)
                index, _ = yield self.agent.kv.get(key=self.key, index=index, wait=wait)
                if attempts is not None:
                    attempts -= 1
        defer.returnValue(result)

    @defer.inlineCallbacks
    def call(self, function, *args, **kwargs):
        yield self.wait()
        try:
            result = yield defer.maybeDeferred(function, *args, **kwargs)
            defer.returnValue(result)
        finally:
            self.release()

    def context(self, attempts=None, delete=None, **kwargs):
        """:rtype: cafe.consul.lock.ConsulLockContext"""
        return ConsulLockContext(self, attempts=attempts, delete=delete, **kwargs)

    def __enter__(self):
        # return a default context
        return self.context().__enter__()


class ConsulLock(AbstractConsulLock):
    @defer.inlineCallbacks
    def _lock(self, action, value=None, **kwargs):
        """
        Internal method to acquire/release a lock

        :type key: str
        :type value: str
        """
        assert action in ('acquire', 'release')
        value = value if value is not None else self.value
        self.logger.debug(
            'lock=%s action=%s session=%s value=%s',
            self.key, action, self.agent.session.uuid, value
        )
        if not self.agent.session.ready:
            self.logger.trace(
                'lock=%s action=%s failed as consul agent is not ready',
                self.key, action
            )
            result = False
        else:
            kwargs[action] = self.agent.session.uuid
            result = yield self.agent.kv.put(
                key=self.key, value=value, **self.kwargs(**kwargs))
        self.logger.info('lock=%s action=%s result=%s', self.key, action, result)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def acquire(self, value=None, **kwargs):
        result = yield self._lock(action='acquire', value=value, **kwargs)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def release(self, value=None, delete=None, **kwargs):
        delete = delete if delete is not None else self.delete
        result = yield self._lock(action='release', value=value, **kwargs)
        if result and delete:
            try:
                self.logger.trace('key=%s deleting as lock is released', self.key)
                yield self.agent.kv.delete(key=self.key)
            except ConsulException as e:
                self.logger.warning(
                    'key=%s failed to delete reason=%s', self.key, e.message)
        defer.returnValue(result)
