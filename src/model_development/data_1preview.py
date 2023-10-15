""" Preview the London Cycle Hire data from GCP and save the results in the info_tracker object. """

import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from ..model_development.config_loading import Config
from ..helper.info_tracking import InfoTracker
from ..model_development.data_2preprocessing import DataPreprocessor


class DataPreviewer:
    """ 
    Access the London Bicycle Hires database in GCP and provide a data preview.
        1. Create a preview of the cycle_hire and the cycle_stations tables.
        2. Save the preview in the info_tracker object, in the format of pandas dataframe.
        
            - WARNING: OUTPUT IS SAVED IN THE INFO_TRACKER OBJECT TO HAVE A CLEAN PIPELINE, WITHOUT SHARING INFORMATION BETWEEN CLASSES THAT ARE NOT DIRECTLY USED.
            
        3. Count the null values of both tables and save them in the info tracker object as pandas dataframes.
        
    :param config: A configuration object that reads the pipeline configuration from a yaml file and load them.
    """

    def __init__(self, config: Config):
        self.config=config
        self.info_tracker = InfoTracker()
        self.__gcp_client = self.__init_bigquery_client()
        self.info_tracker.cycle_hire_preview = self.__create_cycle_hire_preview()
        self.info_tracker.cycle_stations_preview = self.__create_cycle_stations_preview()
        self.info_tracker.hires_null_values_count = self.__count_null_values_in_hires()
        self.info_tracker.stations_null_values_count = self.__count_null_values_in_stations()

    def __init_bigquery_client(self) -> bigquery.Client:
        """ Use my GCP credentials to initiate a bigquery client. """
        
        # Define GCP credentials path
        cred_path = os.path.join(
            self.config.existing_paths.gcp_credential_dir,
            self.config.existing_paths.gcp_credential_file
        )

        # Set up service account.
        credentials = service_account.Credentials.from_service_account_file(cred_path)

        # Init client.
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        return client        

    def __create_cycle_hire_preview(self) -> pd.DataFrame:
        """ Create a preview of the cycle_hire table. The data is limited to 100 rows to accelerate the process. """

        # Build query
        query_job = self.__gcp_client.query(
            f""" 
            SELECT *
            FROM bigquery-public-data.london_bicycles.{self.config.database.hire_table}
            LIMIT 100
            """
        )
        # Call query and extract data in pandas df
        df = query_job.result().to_dataframe()
        return df

    def __create_cycle_stations_preview(self) -> pd.DataFrame:
        """ Create a preview of the cycle_stations table. The data is limited to 100 rows to accelerate the process. """

        # Build query
        query_job = self.__gcp_client.query(
            f""" 
            SELECT *
            FROM bigquery-public-data.london_bicycles.{self.config.database.station_table}
            LIMIT 100
            """
        )
        # Call query and extract data in pandas df
        df = query_job.result().to_dataframe()
        return df

    def __count_null_values_in_hires(self) -> pd.DataFrame:
        """ Count null values for each column in the cycle_hire table. """

        # Build query
        query_job = self.__gcp_client.query(
            f"""
            SELECT
              COUNTIF(rental_id IS NULL) AS rental_id_null_count,
              COUNTIF(duration IS NULL) AS duration_null_count,
              COUNTIF(duration_ms IS NULL) AS duration_ms_null_count,
              COUNTIF(bike_id IS NULL) AS bike_id_null_count,
              COUNTIF(bike_model IS NULL) AS bike_model_null_count,
              COUNTIF(end_date IS NULL) AS end_date_null_count,
              COUNTIF(end_station_id IS NULL) AS end_station_id_null_count,
              COUNTIF(end_station_name IS NULL) AS end_station_name_null_count,
              COUNTIF(start_date IS NULL) AS start_date_null_count,
              COUNTIF(start_station_id IS NULL) AS start_station_id_null_count,
              COUNTIF(start_station_name IS NULL) AS start_station_name_null_count,
              COUNTIF(end_station_logical_terminal IS NULL) AS end_station_logical_terminal_null_count,
              COUNTIF(start_station_logical_terminal IS NULL) AS start_station_logical_terminal_null_count,
              COUNTIF(end_station_priority_id IS NULL) AS end_station_priority_id_null_count
            FROM bigquery-public-data.london_bicycles.{self.config.database.hire_table}
            """
        )
        # Call query and extract data in pandas df
        df = query_job.result().to_dataframe()
        return df

    def __count_null_values_in_stations(self) -> pd.DataFrame:
        """ Count null values for each column in the cycle_stations table. """

        # Build query
        query_job = self.__gcp_client.query(
            f"""
            SELECT
              COUNTIF(id IS NULL) AS station_id_null_count,
              COUNTIF(installed IS NULL) AS installed_null_count,
              COUNTIF(latitude IS NULL) AS latitude_null_count,
              COUNTIF(locked IS NULL) AS locked_null_count,
              COUNTIF(longitude IS NULL) AS longitude_null_count,
              COUNTIF(name IS NULL) AS name_null_count,
              COUNTIF(bikes_count IS NULL) AS bikes_count_null_count,
              COUNTIF(docks_count IS NULL) AS docks_count_null_count,
              COUNTIF(nbEmptyDocks IS NULL) AS nbEmptyDocks_null_count,
              COUNTIF(temporary IS NULL) AS temporary_null_count,
              COUNTIF(terminal_name IS NULL) AS terminal_name_null_count,
              COUNTIF(install_date IS NULL) AS install_date_null_count,
              COUNTIF(removal_date IS NULL) AS removal_date_null_count,
            FROM bigquery-public-data.london_bicycles.{self.config.database.station_table}
            """
        )
        # Call query and extract data in pandas df
        df = query_job.result().to_dataframe()
        return df

    def preprocess_data(self) -> DataPreprocessor:
        """ Call DataPreprocessor class to preprocess the data. """
        
        return DataPreprocessor(
            config=self.config,
            info_tracker=self.info_tracker,
            gcp_client = self.__gcp_client
        )