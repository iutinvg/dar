class BaseDriver(object):
    def __init__(self, name):
        self.name = name

    def post(self, value, uid=None):
        """
        Create a new document in a database.

        value -- value to store
        returns ``Document`` instance
        """
        raise NotImplementedError

    def put(self, uid, value):
        raise NotImplementedError

    def get(self, uid, rev=None):
        raise NotImplementedError
