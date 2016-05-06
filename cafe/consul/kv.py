class KVData(object):
    def __init__(self, data=None):
        if data is None:
            raise ValueError('data is expected to be a dictionary')
        self.LockIndex = data.get('LockIndex')
        self.ModifyIndex = data.get('ModifyIndex')
        self.CreateIndex = data.get('CreateIndex')
        self.Value = data.get('Value')
        self.Flags = data.get('Flags')
        self.Key = data.get('Key')
        self.Session = data.get('Session', None)

    @property
    def created(self):
        return self.ModifyIndex == self.CreateIndex

    @property
    def modified(self):
        return self.ModifyIndex > self.CreateIndex

    def _compare(self, other):
        if isinstance(other, int):
            return self.ModifyIndex - other
        return self.ModifyIndex - other.ModifyIndex

    def __gt__(self, other):
        return self._compare(other) < 0

    def __eq__(self, other):
        return self._compare(other) == 0

    def __lt__(self, other):
        return self._compare(other) > 0
