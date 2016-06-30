from twisted.internet import defer

from cafe.consul.kv import KVData
from cafe.consul.lock import AbstractConsulLock


class ConsulSemaphore(AbstractConsulLock):
    """
    A simplified semaphore implementation.
    """

    def __init__(self, agent, key=None, value='', delete=False, limit=3, cardinality=None, **kwargs):
        key = key \
            if key is not None \
            else 'service/{}/semaphore'.format(agent.name)
        super(ConsulSemaphore, self).__init__(agent, key=key, value=value, delete=delete, **kwargs)
        self.limit = limit
        self.cardinality = cardinality
        self.slots = set(['{}/{}'.format(self.key, i) for i in range(limit)])
        self.slots_held = set()

    @defer.inlineCallbacks
    def _acquire(self, data, value, **kwargs):
        self.logger.trace('prefix=%s data=%s', self.key, data)
        result = False
        available = set()

        if data is None and self.limit > 0:
            available = self.slots
        elif data is not None and isinstance(data, list):
            used_slots = [
                d.Key for d in [KVData(kv) for kv in data] if d.Session is not None
            ]
            self.logger.trace('prefix=%s used slots=%s', self.key, len(used_slots))
            if len(used_slots) < self.limit:
                available = set(self.slots) - set(used_slots)

        self.logger.debug('prefix=%s available slots=%s', self.key, len(available))
        for slot in available:
            # try to acquire lock on each
            self.logger.trace('prefix=%s trying slot=%s', self.key, slot)
            result = yield self.agent.lock(key=slot, value=value, **kwargs).acquire()
            if result:
                self.slots_held.add(slot)
                break

        defer.returnValue(result)

    @defer.inlineCallbacks
    def acquire(self, value='', block=False, retries=None, **kwargs):
        """
        Acquire a slot. If `block` is `True`, this call will listen for change in the prefix directory and retry if
        initial attempts fail. If `retries` is not None, this is the number retries attempted. Improper usage of a
        blocking could yield a dead lock.

        :type value: str
        :type block: bool
        :type retries: None or int
        :type kwargs: dict[str, object]
        :rtype: bool
        """
        if self.cardinality is not None and len(self.slots_held) >= self.cardinality:
            defer.returnValue(True)

        # wait for agent to be ready
        yield self.agent.wait_for_ready()
        index = None
        result = False

        while index is None or not result and block:
            if block and retries is not None:
                retries -= 1
                block = retries > 0
                self.logger.debug('prefix=%s %s retries left', self.key, retries)
            index, data = yield self.agent.agent.kv.get(self.key, index=index, recurse=True)
            result = yield self._acquire(data, value, **kwargs)

        defer.returnValue(result)

    @defer.inlineCallbacks
    def wait(self, value=None, attempts=None, **kwargs):
        result = yield self.acquire(value=value, block=True, retries=attempts, **kwargs)
        defer.returnValue(result)

    @property
    def has_slot(self):
        """Is a slot held?"""
        return len(self.slots_held) > 0

    @defer.inlineCallbacks
    def release(self, slots=None, value=None, delete=None, **kwargs):
        """
        Release one slot or specified slots. If `slots` is `None`, first slot is released.

        :type slots: None or list[str]
        :type value: str
        :type delete: bool
        :type kwargs: dic[str, object]
        :rtype: dict[str, bool]
        """
        if slots is None:
            slots = [next(iter(self.slots_held))]
        elif not isinstance(slots, list):
            slots = [slots]

        self.logger.debug('prefix=%s releasing %s slots', self.key, len(slots))

        value = value if value is not None else self.value
        delete = delete if delete is not None else delete

        results = {}
        # noinspection PyTypeChecker
        for slot in slots:
            if slot not in self.slots_held:
                self.logger.debug('prefix=%s skipping release of unheld slot=%s', self.key, slot)
                continue
            self.agent.lock(key=slot, value=value)
            results[slot] = yield self.agent.lock(key=slot, value=value, delete=delete, **kwargs).release()
            if results[slot]:
                self.slots_held.remove(slot)

        defer.returnValue(results)

    @defer.inlineCallbacks
    def release_all(self, value=None, delete=None, **kwargs):
        """
        Release all slots.

        :rtype: bool
        """
        result = yield self.release(slots=self.slots_held.copy(), value=value, delete=delete, **kwargs)
        defer.returnValue(result)
