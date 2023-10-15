""" Load configuration from the config yaml file. """

from dataclasses import dataclass
from typing import Type
from ..helper.yaml_reading import YamlReader


@dataclass
class ShowOut:
    """ Read the show outcome configuration from the config yaml file. """
    show_outcome: bool

    @classmethod
    def read_config(cls: Type["ShowOut"], obj: dict):
        return cls(
            show_outcome=obj["show_outcome"]
        )


@dataclass
class ExistingPaths:
    """ Read path configuration from the config yaml file. """
    gcp_credential_dir: str
    gcp_credential_file: str
    london_geodata_dir: str
    london_geodata_file: str

    @classmethod
    def read_config(cls: Type["ExistingPaths"], obj: dict):
        return cls(
            gcp_credential_dir=obj["paths"]["existing_paths"]["gcp_credential_dir"],
            gcp_credential_file=obj["paths"]["existing_paths"]["gcp_credential_file"],
            london_geodata_dir=obj["paths"]["existing_paths"]["london_geodata_dir"],
            london_geodata_file=obj["paths"]["existing_paths"]["london_geodata_file"]
        )


@dataclass
class Paths2Create:
    """ Read paths to create configuration from the config yaml file. """
    plots_path: str
    eda_report: str
    model_results: str

    @classmethod
    def read_config(cls: Type["Paths2Create"], obj: dict):
        return cls(
            plots_path=obj["paths"]["paths2create"]["plots_path"],
            eda_report=obj["paths"]["paths2create"]["eda_report"],
            model_results=obj["paths"]["paths2create"]["model_results"]
        )


@dataclass
class DataBase:
    """ Read database tables configuration from the config yaml file. """
    hire_table: str
    station_table: str
    my_project: str
    my_dataset: str
    my_table: str

    @classmethod
    def read_config(cls: Type["DataBase"], obj: dict):
        return cls(
            hire_table=obj["database"]["tables"]["hire_table"],
            station_table=obj["database"]["tables"]["station_table"],
            my_project=obj["database"]["myproject"],
            my_dataset=obj["database"]["mydataset"],
            my_table=obj["database"]["mytable"]
        )
        

@dataclass
class PlotDefault:
    """ Read plotting configuration from the config yaml file. """
    title_color: str
    title_font_style: str
    title_font_size: int
    axes_line_width: int
    axes_line_color: str
    cmap: str
    colour1: str
    colour2: str
    colour3: str
    colour4: str

    @classmethod
    def read_config(cls: Type["PlotDefault"], obj: dict):
        return cls(
            title_color=obj["plotting_default"]["title_color"],
            title_font_style=obj["plotting_default"]["title_font_style"],
            title_font_size=obj["plotting_default"]["title_font_size"],
            axes_line_width=obj["plotting_default"]["axes_line_width"],
            axes_line_color=obj["plotting_default"]["axes_line_color"],
            cmap=obj["plotting_default"]["cmap"],
            colour1=obj["plotting_default"]["colour1"],
            colour2=obj["plotting_default"]["colour2"],
            colour3=obj["plotting_default"]["colour3"],
            colour4=obj["plotting_default"]["colour4"]
        )


@dataclass
class RandomState:
    """ Read random state configuration from the config yaml file. """
    seed: int

    @classmethod
    def read_config(cls: Type["RandomState"], obj: dict):
        return cls(
            seed=obj["random_state"]
        )


class Config:
    """ Load all configurations. """

    def __init__(self, config_path):
        config_file = YamlReader.read_yaml_file(path=config_path)

        self.show_outcome = ShowOut.read_config(obj=config_file)
        self.existing_paths = ExistingPaths.read_config(obj=config_file)
        self.paths2create = Paths2Create.read_config(obj=config_file)
        self.database = DataBase.read_config(obj=config_file)
        self.plotdefault = PlotDefault.read_config(obj=config_file)
        self.random_state = RandomState.read_config(obj=config_file)