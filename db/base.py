class BaseDb(object):
    def __init__(self, name):
        self.name = name

    def post(self, document):
        """
        Create a new document in a database.

        document -- a document to store
        returns ``Document`` instance
        """
        raise NotImplementedError
