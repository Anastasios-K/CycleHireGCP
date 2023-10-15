""" Yaml reader."""
import yaml


class YamlReader:
    """ Yaml reader. """

    @staticmethod
    def read_yaml_file(path: str):
        """ Read yaml file. """
        with open(path) as file:
            content = yaml.load(file, Loader=yaml.FullLoader)
            file.close()
        return content