class Frontend(object):
    def __init__(self, driver):
        """
        Fronend interface for the database.

        driver -- is instance of ``BaseDriver`` subclass
        """
        self.driver = driver
