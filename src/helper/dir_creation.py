""" Create required directories for the project. """

import os


class DirCreator:
    """ Create required directories for the project. """

    def __init__(self, config):
        for path in config.paths2create.__dict__:
            path = config.paths2create.__dict__[path]
            os.makedirs(path, exist_ok=True)