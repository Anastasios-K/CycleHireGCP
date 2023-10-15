""" Run the London Cycle ML Pipeline. """

import os
from src.helper.dir_creation import DirCreator
from src.model_development.config_loading import Config
from src.model_development.data_1preview import DataPreviewer


class LondonCyclePipelineRunner:
    """ Run the London Cycle ML Pipeline. """

    def __init__(self, config_path):
        self.config = Config(config_path=config_path)
        DirCreator(config=self.config)
        self.run = DataPreviewer(config=self.config)\
            .preprocess_data()\
            .explore_data()\
            .prepare_data_for_modelling()\


if __name__ == "__main__":
   
    CONFIG_PATH = os.path.join("src", "config", "config.yaml")
    run = LondonCyclePipelineRunner(config_path=CONFIG_PATH)
    
    print(run.run.info_tracker.__dict__)
