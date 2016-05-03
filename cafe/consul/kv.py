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

    @property
    def created(self):
        return self.ModifyIndex == self.CreateIndex

    @property
    def modified(self):
        return self.ModifyIndex > self.CreateIndex
