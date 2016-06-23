from twisted.internet import defer

from cafe.consul.kv import KVData
from cafe.logging import LoggedObject


class Semaphore(LoggedObject):
    """
    A simplified semaphore implementation.
    """

    def __init__(self, agent, prefix=None, limit=3, cardinality=None):
        self.agent = agent
        """:type: cafe.consul.agent.SessionedConsulAgent"""
        self.prefix = prefix \
            if prefix is not None \
            else 'service/{}/semaphore'.format(agent.name)
        self.limit = limit
        self.cardinality = cardinality
        self.slots = set(['{}/{}'.format(self.prefix, i) for i in range(limit)])
        self.slots_held = set()

    @defer.inlineCallbacks
    def _acquire(self, data, value, **kwargs):
        self.logger.trace('prefix=%s data=%s', self.prefix, data)
        result = False
        available = set()

        if data is None and self.limit > 0:
            available = self.slots
        elif data is not None and isinstance(data, list):
            used_slots = [
                d.Key for d in [KVData(kv) for kv in data] if d.Session is not None
            ]
            self.logger.trace('prefix=%s used slots=%s', self.prefix, len(used_slots))
            if len(used_slots) < self.limit:
                available = set(self.slots) - set(used_slots)

        self.logger.debug('prefix=%s available slots=%s', self.prefix, len(available))
        for slot in available:
            # try to acquire lock on each
            self.logger.trace('prefix=%s trying slot=%s', self.prefix, slot)
            result = yield self.agent.acquire_lock(slot, value=value, **kwargs)
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
                self.logger.debug('prefix=%s %s retries left', self.prefix, retries)
            index, data = yield self.agent.agent.kv.get(self.prefix, index=index, recurse=True)
            result = yield self._acquire(data, value, **kwargs)

        defer.returnValue(result)

    @property
    def has_slot(self):
        """Is a slot held?"""
        return len(self.slots_held) > 0

    @defer.inlineCallbacks
    def release(self, slots=None, value='', delete=False, **kwargs):
        """
        Release all or specified slots. If `slots` is `None`, all slots are released.

        :type slots: None or list[str]
        :type value: str
        :type delete: bool
        :type kwargs: dic[str, object]
        :rtype: dict[str, bool]
        """
        if slots is None:
            slots = self.slots_held.copy()
        elif not isinstance(slots, list):
            slots = [slots]

        self.logger.debug('prefix=%s releasing %s slots', self.prefix, len(slots))

        results = {}
        for slot in slots:
            if slot not in self.slots_held:
                self.logger.debug('prefix=%s skipping release of unheld slot=%s', self.prefix, slot)
                continue
            results[slot] = yield self.agent.release_lock(slot, value=value, delete=delete, **kwargs)
            if results[slot]:
                self.slots_held.remove(slot)

        defer.returnValue(results)

    @defer.inlineCallbacks
    def release_one(self):
        """
        Release any one slot, if at least one slot is held. Returns `False` if no slots are held.

        :rtype: bool
        """
        result = False
        if len(self.slots_held) > 0:
            result = yield self.release(next(iter(self.slots_held)))
        defer.returnValue(result)
