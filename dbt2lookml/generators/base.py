from dbt2lookml.log import Logger

class BaseGenerator(Logger):

    def generate(self):
        raise NotImplementedError